import os
from datetime import datetime
from random import randint
from time import sleep
from typing import List
from unittest.mock import patch

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs
from mockafka import FakeConsumer, FakeProducer, FakeAdminClientImpl
from mockafka.admin_client import NewTopic


def get_cls_full_name(cls):
    return cls.__module__ + '.' + cls.__name__


def run_piece(
    topics: List[str],
    bootstrap_servers: List[str],
    group_id: str,
    security_protocol: str
):
    KAFKA_CA_CERT_PEM = os.environ.get('KAFKA_CA_CERT_PEM', '').replace("\\n", "\n")
    KAFKA_CERT_PEM = os.environ.get('KAFKA_CERT_PEM', '').replace("\\n", "\n")
    KAFKA_KEY_PEM = os.environ.get('KAFKA_KEY_PEM', '').replace("\\n", "\n")

    return piece_dry_run(
        piece_name="KafkaConsumerPiece",
        input_data={
            'topics': topics,
            'bootstrap_servers': bootstrap_servers,
            'group_id': group_id,
            'security_protocol': security_protocol,
            'message_polling_timeout': 10.0,
            'no_message_timeout': 60.0,
        },
        secrets_data={
            'KAFKA_CA_CERT_PEM': KAFKA_CA_CERT_PEM,
            'KAFKA_CERT_PEM': KAFKA_CERT_PEM,
            'KAFKA_KEY_PEM': KAFKA_KEY_PEM
        }
    )


@skip_envs('github')
def test_kafka_consumer_piece():
    piece_kwargs = {
        "topics": ['test-topic1', 'test-topic2'],
        "bootstrap_servers": 'spice-kafka-broker-1.stevo.fedcloud.eu:9093',
        "group_id": "test-group",
        "security_protocol": "SSL",
    }

    num_partitions = 5

    # Create topics
    admin = FakeAdminClientImpl()
    admin.create_topics([
        NewTopic(topic=topic, num_partitions=num_partitions) for topic in piece_kwargs['topics']
    ])

    # Produce messages
    producer = FakeProducer()
    for i in range(0, 10):
        topic = piece_kwargs['topics'][randint(0, len(piece_kwargs['topics']) - 1)]
        key = f'test_key{randint(0, 3)}'
        value = f'test_value{i}'
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
    consumer.subscribe(topics=piece_kwargs['topics'])

    with patch('confluent_kafka.Consumer', new=FakeConsumer):
        output = run_piece(
            **piece_kwargs
        )
        print(output)
