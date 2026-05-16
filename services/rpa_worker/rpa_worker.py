import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, KafkaException


TOPIC = "rpa_decisions"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class RpaWorker:
    def __init__(self) -> None:
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
        self.dsn = os.getenv("RISINGWAVE_DSN", "postgresql://root@risingwave:4566/dev")
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "risingwave-rpa-worker",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

    def connect(self) -> psycopg.Connection:
        while True:
            try:
                conn = psycopg.connect(self.dsn, autocommit=False)
                logging.info("connected to RisingWave")
                return conn
            except psycopg.Error as exc:
                logging.warning("db connect retry err=%s", exc)
                time.sleep(3)

    def action_id_for(self, transaction_id: str, action: str) -> str:
        return hashlib.sha256(f"{transaction_id}:{action}".encode("utf-8")).hexdigest()

    def get_status(self, conn: psycopg.Connection, action_id: str) -> tuple[str, int] | None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, retry_count FROM workflow_audit_log WHERE action_id = %s",
                (action_id,),
            )
            row = cur.fetchone()
        return row

    def insert_if_missing(self, conn: psycopg.Connection, action_id: str, transaction_id: str, action: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO workflow_audit_log (
                    action_id,
                    transaction_id,
                    rpa_action,
                    status,
                    retry_count,
                    created_at,
                    processed_at,
                    processor,
                    error_message
                ) VALUES (%s, %s, %s, 'QUEUED', 0, %s, NULL, 'rpa_worker', NULL)
                """,
                (action_id, transaction_id, action, now_utc()),
            )

    def update_status(
        self,
        conn: psycopg.Connection,
        action_id: str,
        status: str,
        retry_count: int | None = None,
        error_message: str | None = None,
    ) -> None:
        if retry_count is None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE workflow_audit_log
                    SET status = %s,
                        processed_at = %s,
                        error_message = %s
                    WHERE action_id = %s
                    """,
                    (status, now_utc(), error_message, action_id),
                )
        else:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE workflow_audit_log
                    SET status = %s,
                        retry_count = %s,
                        processed_at = %s,
                        error_message = %s
                    WHERE action_id = %s
                    """,
                    (status, retry_count, now_utc(), error_message, action_id),
                )

    def simulate_action(self, payload: dict) -> None:
        action = payload["rpa_action"]
        time.sleep(0.2)
        logging.info(
            "executed action=%s transaction_id=%s final_risk_score=%.2f",
            action,
            payload["transaction_id"],
            float(payload["final_risk_score"]),
        )

    def process_message(self, conn: psycopg.Connection, payload: dict) -> None:
        transaction_id = payload["transaction_id"]
        action = payload["rpa_action"]
        action_id = self.action_id_for(transaction_id, action)
        existing = self.get_status(conn, action_id)

        if existing and existing[0] == "SUCCEEDED":
            logging.info("skip already succeeded action_id=%s", action_id)
            return

        retry_count = existing[1] if existing else 0
        self.insert_if_missing(conn, action_id, transaction_id, action)
        self.update_status(conn, action_id, "RUNNING", retry_count=retry_count)
        conn.commit()

        try:
            self.simulate_action(payload)
            self.update_status(conn, action_id, "SUCCEEDED", retry_count=retry_count, error_message=None)
            conn.commit()
        except Exception as exc:
            retry_count += 1
            self.update_status(conn, action_id, "FAILED", retry_count=retry_count, error_message=str(exc))
            conn.commit()
            raise

    def run(self) -> None:
        self.consumer.subscribe([TOPIC])
        conn = self.connect()

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                if msg.value() is None:
                    self.consumer.commit(message=msg)
                    continue

                payload = json.loads(msg.value().decode("utf-8"))
                try:
                    self.process_message(conn, payload)
                    self.consumer.commit(message=msg)
                except (psycopg.Error, KeyError, ValueError, TypeError) as exc:
                    conn.rollback()
                    logging.exception("failed to process RPA decision err=%s", exc)
                    time.sleep(2)
        finally:
            self.consumer.close()
            conn.close()


if __name__ == "__main__":
    configure_logging()
    RpaWorker().run()
