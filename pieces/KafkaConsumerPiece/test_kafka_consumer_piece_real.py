import logging
import os

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)


@skip_envs('github')
def test_consumer_piece_real_kafka():
    input_data = {
        "bootstrap_servers": [
            "spice-kafka-broker-1.stevo.fedcloud.eu:9093"
        ],
        "group_id": "test-consumer-group1",
        "client_id": "test-client1",
        "security.protocol": "SSL",
        "topics": [
            "topic.test1",
            "topic.test2",
            "topic1",
        ],
        "poll_timeout": 60,
    }

    secrets_data = {
        "ssl_ca_pem": os.environ.get('ssl.ca.pem', ''),
        "ssl_certificate_pem": os.environ.get('ssl.certificate.pem', ''),
        "ssl_key_pem": os.environ.get('ssl.key.pem', ''),
    }

    output = piece_dry_run(
        piece_name="KafkaConsumerPiece",
        input_data=input_data,
        secrets_data=secrets_data,
    )

    logger.info(f"piece output: {output}")
