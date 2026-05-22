import json
import os
import time
from typing import Any


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "etl_practice_events"


def build_messages(count: int = 10) -> list[dict[str, Any]]:
    return [
        {
            "event_id": number,
            "student": f"student_{number}",
            "course": "etl-processes",
            "score": 70 + number,
            "event_time": f"2026-05-22T09:00:{number:02d}Z",
        }
        for number in range(1, count + 1)
    ]


def ensure_topic(bootstrap_servers: str, topic: str) -> None:
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="etl-practice-admin",
    )
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)],
            validate_only=False,
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def send_messages(bootstrap_servers: str, topic: str, count: int) -> None:
    from kafka import KafkaProducer

    ensure_topic(bootstrap_servers, topic)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: str(value).encode("utf-8"),
    )

    for message in build_messages(count=count):
        producer.send(topic, key=message["event_id"], value=message)
        print(f"sent: {message}", flush=True)
        time.sleep(0.2)

    producer.flush()
    producer.close()


def main() -> None:
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    count = int(os.getenv("MESSAGE_COUNT", "10"))

    send_messages(bootstrap_servers=bootstrap_servers, topic=topic, count=count)
    print(f"Finished sending {count} messages to topic '{topic}'.", flush=True)


if __name__ == "__main__":
    main()
