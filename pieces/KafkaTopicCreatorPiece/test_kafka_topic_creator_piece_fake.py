import logging
from unittest.mock import patch

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs
from mockafka import FakeAdminClientImpl, FakeConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


@skip_envs('github')
class MyFakeAdminClientImpl(FakeAdminClientImpl):
    def __init__(self, clean: bool = False, conf=None, **kwargs):
        super().__init__(clean, conf, **kwargs)

    def create_topics(self, topics):
        super().create_topics(topics)
        # Must return dict[str, Future]
        return {topic.topic: FakeAdminFuture() for topic in topics}


class FakeAdminFuture:
    def result(self, timeout=None):
        return None  # success


def test_with_fake_kafka_cluster():
    input_data = {
        "bootstrap_servers": ["fake-broker"],
        "ssl_endpoint_identification_algorithm": "none",
        "exists_ok": True,
        "topics": ["topic.test1", "topic.test2", "topic.test3"],
        "cleanup_policy": ["delete"],
        "retention.ms": 1000,
    }

    secrets_data = {
    }

    admin = MyFakeAdminClientImpl(clean=True)

    with (
        patch('confluent_kafka.Consumer', new=FakeConsumer),
        patch('confluent_kafka.admin.AdminClient', return_value=admin),
    ):
        output = piece_dry_run(
            piece_name="KafkaTopicCreatorPiece",
            input_data=input_data,
            secrets_data=secrets_data,
        )

        logger.info(f"piece output: {output}")

        # Assertions
        assert output is not None

        md = admin.list_topics(timeout=60)
        topics = list(md.topics.keys())
        assert all(topic in topics for topic in input_data["topics"])
