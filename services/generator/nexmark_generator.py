import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from confluent_kafka import Producer


PERSON_TOPIC = "nexmark_persons"
AUCTION_TOPIC = "nexmark_auctions"
BID_TOPIC = "nexmark_bids"

CITIES = [
    ("Bangkok", "BKK"),
    ("Hanoi", "HN"),
    ("Ho Chi Minh City", "HCM"),
    ("Singapore", "SG"),
    ("Jakarta", "JK"),
]
CHANNELS = ["web", "mobile", "partner", "unknown"]
DESCRIPTIONS = [
    "electronics merchant",
    "fashion merchant",
    "gaming merchant",
    "travel merchant",
    "home goods merchant",
]
MESSAGES = [
    "",
    "",
    "normal checkout",
    "follow up later",
    "urgent purchase",
    "suspicious retry",
]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def delivery_report(err: Exception | None, msg: Any) -> None:
    if err is not None:
        logging.error("delivery failed topic=%s err=%s", msg.topic(), err)


class NexmarkGenerator:
    def __init__(self) -> None:
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
        seed = int(os.getenv("GENERATOR_SEED", "4242"))
        self.events_per_second = max(1, int(os.getenv("EVENTS_PER_SECOND", "20")))
        self.run_seconds = max(1, int(os.getenv("RUN_SECONDS", "180")))
        self.random = random.Random(seed)
        self.producer = Producer({"bootstrap.servers": bootstrap, "client.id": "nexmark-generator"})
        self.person_id = 100
        self.auction_id = 5000
        self.people_ids: list[int] = []
        self.auction_ids: list[int] = []
        self.stats = {"persons": 0, "auctions": 0, "bids": 0}

    def produce_json(self, topic: str, payload: dict[str, Any]) -> None:
        self.producer.produce(topic, json.dumps(payload).encode("utf-8"), callback=delivery_report)

    def make_person(self) -> dict[str, Any]:
        self.person_id += 1
        city, state = self.random.choice(CITIES)
        person = {
            "id": self.person_id,
            "name": f"user_{self.person_id}",
            "email": f"user{self.person_id}@example.com",
            "city": city,
            "state": state,
            "date_time": iso(now_utc()),
            "extra": {"risk_tier": self.random.choice(["low", "medium", "high"])},
        }
        self.people_ids.append(self.person_id)
        self.stats["persons"] += 1
        return person

    def make_auction(self) -> dict[str, Any]:
        self.auction_id += 1
        seller = self.random.choice(self.people_ids)
        start = now_utc()
        auction = {
            "id": self.auction_id,
            "item_name": f"merchant_{self.auction_id}",
            "description": self.random.choice(DESCRIPTIONS),
            "initial_bid": self.random.randint(50, 300),
            "reserve": self.random.randint(300, 1500),
            "date_time": iso(start),
            "expires": iso(start + timedelta(hours=1)),
            "seller": seller,
            "category": self.random.randint(1, 5),
            "extra": {"segment": self.random.choice(["consumer", "enterprise", "marketplace"])},
        }
        self.auction_ids.append(self.auction_id)
        self.stats["auctions"] += 1
        return auction

    def make_bid(self, index: int) -> dict[str, Any]:
        bidder = self.random.choice(self.people_ids)
        auction = self.random.choice(self.auction_ids)
        city, _ = self.random.choice(CITIES)
        channel = self.random.choices(CHANNELS, weights=[45, 40, 10, 5], k=1)[0]
        amount = float(self.random.randint(60, 1400))
        message = self.random.choice(MESSAGES)

        if index % 11 == 0:
            bidder = self.people_ids[0]
            amount = float(self.random.randint(4500, 7000))
            message = "urgent purchase"
        elif index % 7 == 0:
            bidder = self.people_ids[1]
            amount = float(self.random.randint(1200, 2400))
            channel = "unknown"
        elif index % 5 == 0:
            bidder = self.people_ids[2]
            amount = float(self.random.randint(1000, 1800))
            message = "suspicious retry"

        bid = {
            "auction": auction,
            "bidder": bidder,
            "price": int(amount),
            "channel": channel,
            "url": f"https://example.com/txn/{auction}-{bidder}-{index}",
            "date_time": iso(now_utc()),
            "extra": {"location": city, "message": message},
        }
        self.stats["bids"] += 1
        return bid

    def bootstrap_entities(self) -> None:
        for _ in range(20):
            self.produce_json(PERSON_TOPIC, self.make_person())
        for _ in range(10):
            self.produce_json(AUCTION_TOPIC, self.make_auction())
        self.producer.flush()
        logging.info("bootstrap complete persons=%s auctions=%s", self.stats["persons"], self.stats["auctions"])

    def maybe_emit_dimension(self, index: int) -> None:
        if index % 9 == 0:
            self.produce_json(PERSON_TOPIC, self.make_person())
        if index % 6 == 0:
            self.produce_json(AUCTION_TOPIC, self.make_auction())

    def run(self) -> None:
        self.bootstrap_entities()
        start = time.monotonic()
        deadline = start + self.run_seconds
        next_tick = start
        bid_index = 0

        while time.monotonic() < deadline:
            self.maybe_emit_dimension(bid_index)
            self.produce_json(BID_TOPIC, self.make_bid(bid_index))
            bid_index += 1
            self.producer.poll(0)

            if bid_index % max(1, self.events_per_second) == 0:
                logging.info(
                    "progress persons=%s auctions=%s bids=%s",
                    self.stats["persons"],
                    self.stats["auctions"],
                    self.stats["bids"],
                )

            next_tick += 1.0 / self.events_per_second
            time.sleep(max(0.0, next_tick - time.monotonic()))

        self.producer.flush()
        logging.info("completed stats=%s", self.stats)


if __name__ == "__main__":
    configure_logging()
    NexmarkGenerator().run()

