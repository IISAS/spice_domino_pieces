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
def test_kafka_producer_piece_real_kafka():
    input_data = {
        "bootstrap_servers": [
            "spice-kafka-broker-1.stevo.fedcloud.eu:9093"
        ],
        "security_protocol": "SSL",
        "messages_file_path": "/mnt/data/workspace/SPICE/spice_domino_pieces/dry_run_results/input_messages.jsonl"
    }

    secrets_data = {
        "ssl_ca_pem": os.environ.get('ssl.ca.pem', ''),
        "ssl_certificate_pem": os.environ.get('ssl.certificate.pem', ''),
        "ssl_key_pem": os.environ.get('ssl.key.pem', ''),
    }

    output = piece_dry_run(
        piece_name="KafkaProducerPiece",
        input_data=input_data,
        secrets_data=secrets_data,
    )

    # Assertions
    assert output is not None
