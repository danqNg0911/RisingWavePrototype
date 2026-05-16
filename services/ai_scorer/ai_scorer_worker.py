import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, KafkaException


TOPIC = "risk_candidates"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ScoreResult:
    ai_score: float
    confidence: float
    model_version: str
    decision_reason: str


class AiScorerWorker:
    def __init__(self) -> None:
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
        self.dsn = os.getenv("RISINGWAVE_DSN", "postgresql://root@risingwave:4566/dev")
        self.mode = os.getenv("AI_MODE", "mock")
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "risingwave-ai-scorer",
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

    def score_event(self, payload: dict) -> ScoreResult:
        score = float(payload.get("rule_risk_score", 0.2))
        reasons: list[str] = []
        amount = float(payload.get("amount", 0.0))
        txn_count = int(payload.get("txn_count", 0))
        message = str(payload.get("message", "")).lower()
        channel = str(payload.get("channel", "unknown")).lower()

        if amount >= 5000:
            score = max(score, 0.95)
            reasons.append("large_amount")
        if txn_count >= 8:
            score = max(score, 0.85)
            reasons.append("high_frequency_user")
        if "urgent" in message or "suspicious" in message:
            score = max(score, 0.80)
            reasons.append("suspicious_text")
        if channel == "unknown":
            score = max(score, 0.70)
            reasons.append("unknown_channel")
        if not reasons:
            reasons.append("rule_based_candidate")

        confidence = min(0.99, 0.60 + score * 0.35)
        return ScoreResult(
            ai_score=round(score, 4),
            confidence=round(confidence, 4),
            model_version=f"{self.mode}-risk-v1",
            decision_reason="_and_".join(reasons),
        )

    def upsert_score(self, conn: psycopg.Connection, payload: dict, result: ScoreResult) -> None:
        transaction_id = payload["transaction_id"]
        user_id = payload["user_id"]
        inference_time = now_utc()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ai_scored_events WHERE transaction_id = %s", (transaction_id,))
            cur.execute(
                """
                INSERT INTO ai_scored_events (
                    transaction_id,
                    user_id,
                    ai_score,
                    confidence,
                    model_version,
                    decision_reason,
                    inference_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    transaction_id,
                    user_id,
                    result.ai_score,
                    result.confidence,
                    result.model_version,
                    result.decision_reason,
                    inference_time,
                ),
            )
        conn.commit()

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
                    result = self.score_event(payload)
                    self.upsert_score(conn, payload, result)
                    self.consumer.commit(message=msg)
                    logging.info(
                        "scored transaction_id=%s ai_score=%.2f reason=%s",
                        payload["transaction_id"],
                        result.ai_score,
                        result.decision_reason,
                    )
                except (psycopg.Error, KeyError, ValueError, TypeError) as exc:
                    conn.rollback()
                    logging.exception("failed to score message err=%s", exc)
                    time.sleep(2)
        finally:
            self.consumer.close()
            conn.close()


if __name__ == "__main__":
    configure_logging()
    AiScorerWorker().run()

