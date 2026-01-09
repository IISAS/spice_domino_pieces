import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from random import randint
from typing import List
from unittest.mock import patch

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs
from mockafka import FakeAdminClientImpl, FakeConsumer, FakeProducer
from mockafka.admin_client import NewTopic

TOPICS = {}
admin = FakeAdminClientImpl()


def create_topic(topic: str, num_partitions: int):
    admin.create_topics([
        NewTopic(topic=topic, num_partitions=num_partitions)
    ])
    TOPICS[topic] = num_partitions


def topic_exists(topic: str) -> bool:
    return topic in TOPICS


def get_num_partitions(topic: str) -> int:
    return TOPICS[topic]


def create_messages_file(messages_file: Path, topic: str, num_messages: int = 10) -> str:
    """
    Create a JSONL file compatible with KafkaConsumerPiece output.
    """
    with open(messages_file, "w", encoding="utf-8") as fp:
        for i in range(num_messages):
            msg = {
                "topic": topic,
                "partition": randint(0, 2),
                "offset": i,
                "key": f"test_key{randint(0, 3)}",
                "timestamp": int(datetime.now().timestamp() * 1000),
                "value": f"test_value{i}"
            }
            fp.write(json.dumps(msg) + "\n")

    return str(messages_file)


def run_piece(
    topic: str,
    bootstrap_servers: List[str],
    messages_filename: str,
    security_protocol: str
):
    KAFKA_CA_CERT_PEM = os.environ.get('KAFKA_CA_CERT_PEM', '').replace("\\n", "\n")
    KAFKA_CERT_PEM = os.environ.get('KAFKA_CERT_PEM', '').replace("\\n", "\n")
    KAFKA_KEY_PEM = os.environ.get('KAFKA_KEY_PEM', '').replace("\\n", "\n")

    return piece_dry_run(
        piece_name="KafkaProducerPiece",
        input_data={
            'topic': topic,
            'bootstrap_servers': bootstrap_servers,
            "messages_filename": messages_filename,
            'security_protocol': security_protocol,
        },
        secrets_data={
            'KAFKA_CA_CERT_PEM': KAFKA_CA_CERT_PEM,
            'KAFKA_CERT_PEM': KAFKA_CERT_PEM,
            'KAFKA_KEY_PEM': KAFKA_KEY_PEM
        }
    )


@skip_envs('github')
def test_kafka_producer_piece():
    piece_kwargs = {
        "topic": "test-output-topic",
        "bootstrap_servers": "fake-broker",
        "security_protocol": "SSL",
    }

    topic = piece_kwargs["topic"]
    num_partitions = 5
    num_messages = 10

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create input messages file
        messages_filename = create_messages_file(
            messages_file=Path(os.path.join(tmp_dir, "messages.jsonl")),
            topic=topic,
            num_messages=num_messages
        )

        producer = FakeProducer()

        with patch("confluent_kafka.Producer", return_value=producer):

            if not topic_exists(topic):
                create_topic(
                    topic=topic,
                    num_partitions=num_partitions
                )

            assert get_num_partitions(topic) == num_partitions

            output = run_piece(
                messages_filename=messages_filename,
                **piece_kwargs
            )
            print(output)

            # Assertions
            assert output is not None

            # Read produced messages using a fake consumer
            consumer = FakeConsumer()
            consumer.subscribe([topic])
            messages = []
            while True:
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    break
                messages.append(msg)
            assert len(messages) == num_messages

            # Check consumed messages
            for msg in messages:
                assert msg.topic() == topic
                assert msg.value() is not None
