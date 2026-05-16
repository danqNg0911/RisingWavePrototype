import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import psycopg
from confluent_kafka import Consumer, KafkaException
from openiap import Client


TOPIC = "rpa_decisions"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


class RpaWorker:
    def __init__(self) -> None:
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
        self.dsn = os.getenv("RISINGWAVE_DSN", "postgresql://root@risingwave:4566/dev")
        self.rpa_mode = os.getenv("RPA_MODE", "openflow_queue").strip().lower() or "openflow_queue"
        self.openflow_url = self.normalize_openflow_url(os.getenv("OPENFLOW_URL", "").strip())
        self.openflow_username = os.getenv("OPENFLOW_USERNAME", "").strip()
        self.openflow_password = os.getenv("OPENFLOW_PASSWORD", "").strip()
        self.openflow_token = os.getenv("OPENFLOW_TOKEN", "").strip()
        self.queue_name = os.getenv("OPENRPA_QUEUE_NAME", "risingwave_rpa_decisions").strip() or "risingwave_rpa_decisions"
        self.entry_workflow = os.getenv("OPENRPA_ENTRY_WORKFLOW", "").strip()
        self.max_dispatch_retries = int(os.getenv("OPENFLOW_MAX_DISPATCH_RETRIES", "6"))
        self.backlog_batch_size = int(os.getenv("OPENFLOW_BACKLOG_BATCH_SIZE", "3"))
        self.openflow_loop: asyncio.AbstractEventLoop | None = None
        self.openflow_client: Client | None = None
        self.openflow_signed_in = False
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "risingwave-rpa-worker",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

    def normalize_openflow_url(self, raw_url: str) -> str:
        if not raw_url:
            return ""

        parsed = urlparse(raw_url)
        if parsed.scheme in {"grpc", "ws", "wss"}:
            return raw_url

        if parsed.scheme in {"http", "https"} and parsed.hostname:
            host = parsed.hostname
            grpc_host = host if host.startswith("grpc.") else f"grpc.{host}"
            port = parsed.port or 443
            normalized = f"grpc://{grpc_host}:{port}"
            logging.warning(
                "normalized OPENFLOW_URL from %s to %s; prefer using grpc:// or wss:// directly in .env",
                raw_url,
                normalized,
            )
            return normalized

        return raw_url

    def connect(self) -> psycopg.Connection:
        while True:
            try:
                conn = psycopg.connect(self.dsn, autocommit=False)
                logging.info("connected to RisingWave")
                return conn
            except psycopg.Error as exc:
                logging.warning("db connect retry err=%s", exc)
                time.sleep(3)

    def get_openflow_loop(self) -> asyncio.AbstractEventLoop:
        if self.openflow_loop is None:
            self.openflow_loop = asyncio.new_event_loop()
        return self.openflow_loop

    async def ensure_openflow_client(self) -> Client:
        if self.openflow_client and self.openflow_signed_in and getattr(self.openflow_client, "connected", False):
            return self.openflow_client

        if self.openflow_client:
            try:
                self.openflow_client.Close()
            except Exception:
                pass

        self.openflow_client = Client(self.openflow_url)
        self.openflow_signed_in = False

        if self.openflow_token:
            await self.openflow_client.Signin(self.openflow_token, ping=False)
        else:
            await self.openflow_client.Signin(self.openflow_username, self.openflow_password, ping=False)

        self.openflow_signed_in = True
        return self.openflow_client

    def reset_openflow_client(self) -> None:
        if self.openflow_client:
            try:
                self.openflow_client.Close()
            except Exception:
                pass
        self.openflow_client = None
        self.openflow_signed_in = False

    def close_openflow_resources(self) -> None:
        self.reset_openflow_client()
        if self.openflow_loop is not None:
            self.openflow_loop.close()
            self.openflow_loop = None

    def action_id_for(self, transaction_id: str, action: str) -> str:
        return hashlib.sha256(f"{transaction_id}:{action}".encode("utf-8")).hexdigest()

    def get_status(self, conn: psycopg.Connection, action_id: str) -> tuple[str, int] | None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, retry_count FROM workflow_dispatch_log WHERE action_id = %s",
                (action_id,),
            )
            row = cur.fetchone()
        return row

    def insert_if_missing(
        self,
        conn: psycopg.Connection,
        action_id: str,
        transaction_id: str,
        action: str,
        decision_time: datetime | None,
        payload_json: str,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO workflow_dispatch_log (
                    action_id,
                    transaction_id,
                    rpa_action,
                    status,
                    retry_count,
                    created_at,
                    dispatched_at,
                    decision_time,
                    processor,
                    error_message,
                    dispatch_mode,
                    queue_name,
                    entry_workflow,
                    external_workitem_id,
                    external_state,
                    payload_json
                ) VALUES (%s, %s, %s, 'QUEUED', 0, %s, NULL, %s, 'rpa_worker', NULL, %s, %s, %s, NULL, NULL, %s)
                """,
                (
                    action_id,
                    transaction_id,
                    action,
                    now_utc(),
                    decision_time,
                    self.rpa_mode,
                    self.queue_name,
                    self.entry_workflow or None,
                    payload_json,
                ),
            )

    def update_status(
        self,
        conn: psycopg.Connection,
        action_id: str,
        status: str,
        retry_count: int,
        error_message: str | None,
        external_workitem_id: str | None = None,
        external_state: str | None = None,
        dispatched_at: datetime | None = None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE workflow_dispatch_log
                SET status = %s,
                    retry_count = %s,
                    dispatched_at = COALESCE(%s, dispatched_at),
                    error_message = %s,
                    external_workitem_id = COALESCE(%s, external_workitem_id),
                    external_state = COALESCE(%s, external_state)
                WHERE action_id = %s
                """,
                (
                    status,
                    retry_count,
                    dispatched_at,
                    error_message,
                    external_workitem_id,
                    external_state,
                    action_id,
                ),
            )

    def build_workitem_payload(self, payload: dict) -> dict:
        return {
            "transaction_id": payload["transaction_id"],
            "user_id": payload["user_id"],
            "merchant_id": payload.get("merchant_id"),
            "merchant_name": payload.get("merchant_name"),
            "amount": payload.get("amount"),
            "channel": payload.get("channel"),
            "location": payload.get("location"),
            "event_time": payload.get("event_time"),
            "decision_time": payload.get("decision_time"),
            "rule_risk_score": payload.get("rule_risk_score"),
            "ai_score": payload.get("ai_score"),
            "confidence": payload.get("confidence"),
            "model_version": payload.get("model_version"),
            "final_risk_score": payload.get("final_risk_score"),
            "decision_reason": payload.get("decision_reason"),
            "rpa_action": payload["rpa_action"],
            "target_queue": self.queue_name,
            "entry_workflow": self.entry_workflow or None,
        }

    def workitem_priority_for(self, action: str) -> int:
        if action == "BLOCK_AND_CREATE_TICKET":
            return 1
        if action == "CREATE_TICKET_AND_NOTIFY":
            return 2
        return 3

    async def push_openflow_workitem(self, workitem_payload: dict, action: str):
        if not self.openflow_url:
            raise RuntimeError("OPENFLOW_URL is required for OpenFlow dispatch")
        if not self.openflow_token and not (self.openflow_username and self.openflow_password):
            raise RuntimeError("OPENFLOW_TOKEN or OPENFLOW_USERNAME/OPENFLOW_PASSWORD is required")

        client = await self.ensure_openflow_client()
        workitem = await client.PushWorkitem(
            wiq=self.queue_name,
            name=f"{action}:{workitem_payload['transaction_id']}",
            payload=workitem_payload,
            priority=self.workitem_priority_for(action),
        )
        return workitem

    def dispatch_external(self, workitem_payload: dict, action: str) -> tuple[str | None, str | None]:
        loop = self.get_openflow_loop()
        last_error: Exception | None = None

        for attempt in range(1, 3):
            try:
                asyncio.set_event_loop(loop)
                workitem = loop.run_until_complete(self.push_openflow_workitem(workitem_payload, action))
                return getattr(workitem, "_id", None), getattr(workitem, "state", None)
            except Exception as exc:
                last_error = exc
                logging.warning(
                    "openflow dispatch attempt=%s action=%s transaction_id=%s err=%s",
                    attempt,
                    action,
                    workitem_payload.get("transaction_id"),
                    exc,
                )
                self.reset_openflow_client()
                time.sleep(1)

        if last_error is None:
            raise RuntimeError("openflow dispatch failed without an error")
        raise last_error

    def dispatch_mock(self, payload: dict) -> tuple[str, str]:
        time.sleep(0.2)
        logging.info(
            "mock-dispatched action=%s transaction_id=%s final_risk_score=%.2f",
            payload["rpa_action"],
            payload["transaction_id"],
            float(payload["final_risk_score"]),
        )
        return self.action_id_for(payload["transaction_id"], payload["rpa_action"]), "mock_succeeded"

    def dispatch_decision(self, conn: psycopg.Connection, payload: dict) -> None:
        transaction_id = payload["transaction_id"]
        action = payload["rpa_action"]
        action_id = self.action_id_for(transaction_id, action)
        decision_time = parse_timestamp(payload.get("decision_time"))
        workitem_payload = self.build_workitem_payload(payload)
        payload_json = json.dumps(workitem_payload, separators=(",", ":"))
        existing = self.get_status(conn, action_id)

        if existing and existing[0] == "DISPATCHED":
            logging.info("skip already dispatched action_id=%s", action_id)
            return

        retry_count = existing[1] if existing else 0
        if existing is None:
            self.insert_if_missing(conn, action_id, transaction_id, action, decision_time, payload_json)

        self.update_status(conn, action_id, "DISPATCHING", retry_count=retry_count, error_message=None)
        conn.commit()

        try:
            if self.rpa_mode == "mock":
                external_workitem_id, external_state = self.dispatch_mock(payload)
            else:
                external_workitem_id, external_state = self.dispatch_external(workitem_payload, action)

            self.update_status(
                conn,
                action_id,
                "DISPATCHED",
                retry_count=retry_count,
                error_message=None,
                external_workitem_id=external_workitem_id,
                external_state=external_state or "new",
                dispatched_at=now_utc(),
            )
            conn.commit()
            logging.info(
                "dispatched mode=%s action=%s transaction_id=%s external_workitem_id=%s external_state=%s",
                self.rpa_mode,
                action,
                transaction_id,
                external_workitem_id,
                external_state,
            )
        except Exception as exc:
            retry_count += 1
            failure_status = "FAILED_CONFIG" if isinstance(exc, RuntimeError) else "FAILED_DISPATCH"
            self.update_status(
                conn,
                action_id,
                failure_status,
                retry_count=retry_count,
                error_message=str(exc),
                dispatched_at=now_utc(),
            )
            conn.commit()
            logging.exception("failed to dispatch workitem err=%s", exc)

    def fetch_backlog_payloads(self, conn: psycopg.Connection) -> list[dict]:
        limit_value = max(1, min(self.backlog_batch_size, 100))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT payload_json
                FROM workflow_dispatch_log
                WHERE dispatch_mode = %s
                  AND status IN ('QUEUED', 'FAILED_DISPATCH', 'FAILED_CONFIG')
                  AND retry_count < %s
                ORDER BY created_at
                LIMIT {limit_value}
                """,
                (self.rpa_mode, self.max_dispatch_retries),
            )
            rows = cur.fetchall()
        return [json.loads(row[0]) for row in rows]

    def process_backlog(self, conn: psycopg.Connection) -> None:
        if self.rpa_mode == "mock":
            return

        for payload in self.fetch_backlog_payloads(conn):
            try:
                self.dispatch_decision(conn, payload)
            except (psycopg.Error, KeyError, ValueError, TypeError) as exc:
                conn.rollback()
                logging.exception("failed to process backlog dispatch err=%s", exc)
                break

    def run(self) -> None:
        self.consumer.subscribe([TOPIC])
        conn = self.connect()

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    self.process_backlog(conn)
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                if msg.value() is None:
                    self.consumer.commit(message=msg)
                    continue

                payload = json.loads(msg.value().decode("utf-8"))
                try:
                    self.dispatch_decision(conn, payload)
                    self.consumer.commit(message=msg)
                except (psycopg.Error, KeyError, ValueError, TypeError) as exc:
                    conn.rollback()
                    logging.exception("failed to process RPA decision err=%s", exc)
                    time.sleep(2)
        finally:
            self.consumer.close()
            self.close_openflow_resources()
            conn.close()


if __name__ == "__main__":
    configure_logging()
    RpaWorker().run()
