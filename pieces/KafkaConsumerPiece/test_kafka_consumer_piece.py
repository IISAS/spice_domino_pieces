import base64
import logging
import os
from datetime import datetime
from random import randint
from time import sleep
from unittest.mock import patch

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs
from mockafka import FakeConsumer, FakeProducer, FakeAdminClientImpl
from mockafka.admin_client import NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)


def get_cls_full_name(cls):
    return cls.__module__ + '.' + cls.__name__


def encode_msg_value(msg_value, encoding):
    if msg_value is None:
        return None
    if encoding == "base64":
        return base64.b64encode(bytes(msg_value, "utf-8"))
    elif encoding == "utf-8":
        return msg_value.encode('utf-8')
    return msg_value


def test_with_fake_kafka_cluster():
    input_data = {
        "topics": ['topic.default1', 'topic.default2'],
        "bootstrap_servers": ['fake.broker'],
        "security_protocol": "none",
        "group_id": "group.default",
        "msg_value_encoding": "utf-8",
    }

    secrets_data = {
        'ssl_ca_pem': "",
        'ssl_certificate_pem': "",
        'ssl_key_pem': "",
    }

    num_partitions = 5

    # Create topics
    admin = FakeAdminClientImpl()
    admin.create_topics([
        NewTopic(topic=topic, num_partitions=num_partitions) for topic in input_data['topics']
    ])

    # Produce messages
    producer = FakeProducer()
    for i in range(0, 10):
        topic = input_data['topics'][randint(0, len(input_data['topics']) - 1)]
        key = f'test_key{randint(0, 3)}'
        value = encode_msg_value(f'test_value{i}', input_data['msg_value_encoding'])
        partition = randint(0, num_partitions - 1)
        timestamp = int(datetime.now().timestamp() * 1000)
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp
        )
        sleep(randint(1, 3))

    # Subscribe consumer
    consumer = FakeConsumer()
    consumer.subscribe(topics=input_data['topics'])

    with patch('confluent_kafka.Consumer', new=FakeConsumer):
        output = piece_dry_run(
            piece_name="KafkaConsumerPiece",
            input_data=input_data,
            secrets_data=secrets_data,
        )
        logger.info(f"piece output: {output}")


@skip_envs('github')
def test_with_real_kafka_cluster():
    input_data = {
        "bootstrap_servers": os.getenv("bootstrap_servers", "").split(","),
        "security_protocol": "SSL",
        "topics": ["topic.default1", "topic.default2"],
        "group_id": "group.default",
        "msg_value_encoding": "utf-8",
    }
    secrets_data = {
        "ssl_ca_pem": os.environ.get('ssl_ca_pem', ''),
        "ssl_certificate_pem": os.environ.get('ssl_certificate_pem', ''),
        "ssl_key_pem": os.environ.get('ssl_key_pem', ''),
    }

    output = piece_dry_run(
        piece_name="KafkaConsumerPiece",
        input_data=input_data,
        secrets_data=secrets_data,
    )

    logger.info(f"piece output: {output}")
