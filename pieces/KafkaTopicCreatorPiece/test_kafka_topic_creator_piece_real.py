import logging
import os

from confluent_kafka.admin import AdminClient
from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


@skip_envs('github')
def test_with_real_kafka_cluster():
    input_data = {
        "bootstrap_servers": os.getenv("bootstrap.servers", "").split(","),
        "ssl_endpoint_identification_algorithm": "none",
        "security_protocol": "SSL",
        "exists_ok": True,
        "topics": ["topic.test1", "topic.test2"],
        "cleanup_policy": ["delete"],
        "retention.ms": 1000,
    }

    secrets_data = {
        "ssl_ca_pem": os.environ.get('ssl.ca.pem', ''),
        "ssl_certificate_pem": os.environ.get('ssl.certificate.pem', ''),
        "ssl_key_pem": os.environ.get('ssl.key.pem', ''),
    }

    output = piece_dry_run(
        piece_name="KafkaTopicCreatorPiece",
        input_data=input_data,
        secrets_data=secrets_data,
    )

    logger.info(f"piece output: {output}")

    # Assertions
    assert output is not None

    admin = AdminClient(conf={
        # 'debug': 'security,broker,conf',
        # 'log_level': 7,
        "bootstrap.servers": ','.join(input_data["bootstrap_servers"]),
        "ssl.endpoint.identification.algorithm": input_data["ssl_endpoint_identification_algorithm"],
        "security.protocol": input_data["security_protocol"],
        "ssl.ca.pem": secrets_data["ssl_ca_pem"].replace("\\n", "\n"),
        "ssl.certificate.pem": secrets_data["ssl_certificate_pem"].replace("\\n", "\n"),
        "ssl.key.pem": secrets_data["ssl_key_pem"].replace("\\n", "\n"),
    })

    md = admin.list_topics(timeout=60)
    topics = md.topics.keys()
    assert all(topic in topics for topic in input_data["topics"])
    assert all(topic in output["topics_created"] for topic in input_data["topics"])
