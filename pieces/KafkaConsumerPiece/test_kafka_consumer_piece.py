import os
from typing import List

from domino.testing import piece_dry_run


def run_piece(
    topics: List[str],
    bootstrap_servers: List[str],
    group_id: str,
    security_protocol: str
):
    KAFKA_CA_CERT_PEM = os.environ.get('KAFKA_CA_CERT_PEM').replace("\\n", "\n")
    KAFKA_CERT_PEM = os.environ.get('KAFKA_CERT_PEM').replace("\\n", "\n")
    KAFKA_KEY_PEM = os.environ.get('KAFKA_KEY_PEM').replace("\\n", "\n")

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


def test_kafka_consumer_piece():
    piece_kwargs = {
        "topics": ['test-topic1'],
        "bootstrap_servers": 'spice-kafka-broker-1.stevo.fedcloud.eu:9093',
        "group_id": "test-group",
        "security_protocol": "SSL",
    }
    output = run_piece(
        **piece_kwargs
    )
