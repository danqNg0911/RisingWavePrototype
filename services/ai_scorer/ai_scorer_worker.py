import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, KafkaException
from huggingface_hub import InferenceClient


TOPIC = "risk_candidates"
DEFAULT_HF_MODEL = "facebook/bart-large-mnli"
SUPPORTED_HF_PROVIDERS = {
    "auto",
    "black-forest-labs",
    "cerebras",
    "clarifai",
    "cohere",
    "deepinfra",
    "fal-ai",
    "featherless-ai",
    "fireworks-ai",
    "groq",
    "hf-inference",
    "hyperbolic",
    "nebius",
    "novita",
    "nscale",
    "nvidia",
    "openai",
    "ovhcloud",
    "publicai",
    "replicate",
    "sambanova",
    "scaleway",
    "together",
    "wavespeed",
    "zai-org",
}


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
        self.mode = os.getenv("AI_MODE", "mock").strip().lower()
        self.hf_token = os.getenv("HF_TOKEN", "").strip()
        self.hf_model_id = os.getenv("HF_MODEL_ID", DEFAULT_HF_MODEL).strip() or DEFAULT_HF_MODEL
        self.hf_provider_policy = os.getenv("HF_PROVIDER_POLICY", "preferred").strip().lower() or "preferred"
        self.hf_provider = self.resolve_hf_provider()
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "risingwave-ai-scorer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.hf_client = (
            InferenceClient(api_key=self.hf_token, provider=self.hf_provider)
            if self.hf_token
            else None
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

    def resolve_hf_provider(self) -> str | None:
        if self.hf_provider_policy in {"", "preferred"}:
            return None
        if self.hf_provider_policy in {"fastest", "cheapest"}:
            return "auto"
        if self.hf_provider_policy in SUPPORTED_HF_PROVIDERS:
            return self.hf_provider_policy
        logging.warning("unsupported HF_PROVIDER_POLICY=%s, falling back to default provider", self.hf_provider_policy)
        return None

    def build_hf_model_version(self) -> str:
        if self.hf_provider:
            return f"{self.hf_model_id}@{self.hf_provider}"
        return self.hf_model_id

    def build_hf_text(self, payload: dict) -> str:
        return (
            f"Transaction amount is {payload.get('amount', 0)}. "
            f"Channel is {payload.get('channel', 'unknown')}. "
            f"User made {payload.get('txn_count', 0)} transactions in the last 10 minutes. "
            f"Message text is '{payload.get('message', '')}'. "
            f"Location is '{payload.get('location', 'unknown')}'. "
            f"Rule risk score is {payload.get('rule_risk_score', 0.2)}."
        )

    def rule_score_event(self, payload: dict, model_version: str | None = None) -> ScoreResult:
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
            model_version=model_version or f"{self.mode}-risk-v1",
            decision_reason="_and_".join(reasons),
        )

    def score_event_hf_api(self, payload: dict) -> ScoreResult:
        if not self.hf_client:
            raise RuntimeError("HF_TOKEN is required for AI_MODE=hf_api")

        labels = [
            "fraudulent transaction",
            "account takeover signal",
            "burst payment behavior",
            "suspicious payment text",
            "legitimate transaction",
        ]
        result = self.hf_client.zero_shot_classification(
            text=self.build_hf_text(payload),
            candidate_labels=labels,
            multi_label=True,
            model=self.hf_model_id,
        )
        if not result:
            raise RuntimeError("Hugging Face returned no classification result")

        scores = {item.label: float(item.score) for item in result}
        risk_labels = [
            "fraudulent transaction",
            "account takeover signal",
            "burst payment behavior",
            "suspicious payment text",
        ]
        risk_score = max(scores.get(label, 0.0) for label in risk_labels)
        selected_reasons = [label.replace(" ", "_") for label in risk_labels if scores.get(label, 0.0) >= 0.45]
        if not selected_reasons:
            top_risk_label = max(risk_labels, key=lambda label: scores.get(label, 0.0))
            selected_reasons.append(top_risk_label.replace(" ", "_"))

        return ScoreResult(
            ai_score=round(max(float(payload.get("rule_risk_score", 0.2)), risk_score), 4),
            confidence=round(max(scores.values()), 4),
            model_version=f"hf_api:{self.build_hf_model_version()}",
            decision_reason="hf_zero_shot_" + "_and_".join(selected_reasons),
        )

    def score_event_hf_local(self, payload: dict) -> ScoreResult:
        try:
            from transformers import pipeline
        except ImportError as exc:
            raise RuntimeError("transformers is not installed for hf_local mode") from exc

        classifier = pipeline("zero-shot-classification", model=self.hf_model_id, device=-1)
        result = classifier(
            self.build_hf_text(payload),
            candidate_labels=[
                "fraudulent transaction",
                "account takeover signal",
                "burst payment behavior",
                "suspicious payment text",
                "legitimate transaction",
            ],
            multi_label=True,
        )
        labels = list(result.get("labels", []))
        scores = list(result.get("scores", []))
        combined = {label: float(score) for label, score in zip(labels, scores)}
        risk_labels = [
            "fraudulent transaction",
            "account takeover signal",
            "burst payment behavior",
            "suspicious payment text",
        ]
        risk_score = max(combined.get(label, 0.0) for label in risk_labels)
        selected_reasons = [label.replace(" ", "_") for label in risk_labels if combined.get(label, 0.0) >= 0.45]
        if not selected_reasons:
            selected_reasons.append(max(risk_labels, key=lambda label: combined.get(label, 0.0)).replace(" ", "_"))

        return ScoreResult(
            ai_score=round(max(float(payload.get("rule_risk_score", 0.2)), risk_score), 4),
            confidence=round(max(combined.values(), default=0.5), 4),
            model_version=f"hf_local:{self.hf_model_id}",
            decision_reason="hf_local_zero_shot_" + "_and_".join(selected_reasons),
        )

    def score_event(self, payload: dict) -> ScoreResult:
        if self.mode == "hf_api":
            try:
                return self.score_event_hf_api(payload)
            except Exception as exc:
                logging.warning("hf_api scoring failed, falling back to rule scoring err=%s", exc)
                return self.rule_score_event(payload, model_version="hf_api_fallback_rule")

        if self.mode == "hf_local":
            try:
                return self.score_event_hf_local(payload)
            except Exception as exc:
                logging.warning("hf_local scoring failed, falling back to rule scoring err=%s", exc)
                return self.rule_score_event(payload, model_version="hf_local_fallback_rule")

        return self.rule_score_event(payload)

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
                        "scored transaction_id=%s ai_score=%.2f mode=%s reason=%s",
                        payload["transaction_id"],
                        result.ai_score,
                        self.mode,
                        result.decision_reason,
                    )
                except (psycopg.Error, KeyError, ValueError, TypeError, RuntimeError) as exc:
                    conn.rollback()
                    logging.exception("failed to score message err=%s", exc)
                    time.sleep(2)
        finally:
            self.consumer.close()
            conn.close()


if __name__ == "__main__":
    configure_logging()
    AiScorerWorker().run()
