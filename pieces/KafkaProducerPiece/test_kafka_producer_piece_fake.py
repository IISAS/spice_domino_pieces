import json
import logging
import os
import tempfile
from datetime import datetime
from random import randint
from unittest.mock import patch

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs
from mockafka import FakeAdminClientImpl, FakeConsumer, FakeProducer
from mockafka.admin_client import NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)

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


def num_digits(n: int) -> int:
    return len(str(abs(n)))


def generate_name(prefix: str, num_items: int, id: int) -> str:
    return str(prefix + "%0" + str(num_digits(num_items)) + "d") % id


def generate_topic_name(num_topics, id) -> str:
    return generate_name("topic", num_topics, id)


def generate_key_name(num_keys, id) -> str:
    return generate_name("key", num_keys, id)


def generate_messages(
        filename: str,
        num_messages: int,
        num_topics: int,
        num_partitions: int,
        num_keys: int | None,
        create_topics: bool = False,
):
    """
    Create a JSONL file compatible with KafkaConsumerPiece output.
    """
    topic_last_offset = [0 for _ in range(num_topics)]
    with open(filename, "w", encoding="utf-8") as fp:
        for i in range(num_messages):
            topic_id = randint(0, num_topics - 1)
            topic = generate_topic_name(num_topics=num_topics, id=topic_id)
            if create_topics and not topic_exists(topic):
                create_topic(
                    topic=topic,
                    num_partitions=num_partitions
                )
                assert get_num_partitions(topic) == num_partitions
            msg = {
                "topic": topic,
                "partition": randint(0, num_partitions - 1),
                "offset": topic_last_offset[topic_id],
                "key": generate_key_name(
                    num_keys=num_keys,
                    id=randint(0, num_keys - 1)
                ) if num_keys is not None else None,
                "timestamp": int(datetime.now().timestamp() * 1000),
                "value": f"test_value{i}"
            }
            topic_last_offset[topic_id] += 1
            fp.write(json.dumps(msg) + "\n")


@skip_envs('github')
def test_kafka_producer_piece_fake_kafka():
    num_messages = 10
    num_topics = 1
    num_partitions = 5
    num_keys = None

    with tempfile.TemporaryDirectory() as tmp_dir:

        # generate mock messages
        messages_path = os.path.join(tmp_dir, "messages.jsonl")

        input_data = {
            "bootstrap_servers": ["fake-broker"],
            "security_protocol": "PLAINTEXT",
            "messages_file_path": messages_path,
        }
        secrets_data = {
        }

        generate_messages(
            filename=messages_path,
            num_messages=num_messages,
            num_topics=num_topics,
            num_partitions=num_partitions,
            num_keys=num_keys,
            create_topics=True
        )

        producer = FakeProducer()
        consumer = FakeConsumer()

        with patch(
                "confluent_kafka.Producer",
                return_value=producer
        ):

            output = piece_dry_run(
                piece_name="KafkaProducerPiece",
                input_data=input_data,
                secrets_data=secrets_data,
            )

            # Assertions
            assert output is not None

            # Read produced messages using a consumer
            consumer.subscribe(TOPICS)
            messages = []

            while True:
                msg = consumer.poll(timeout=60)
                if msg is None:
                    break
                assert msg.error() is None
                messages.append(msg)

            assert len(messages) == num_messages

            # Check consumed messages
            for msg in messages:
                assert msg.topic() in TOPICS
                assert msg.value() is not None
