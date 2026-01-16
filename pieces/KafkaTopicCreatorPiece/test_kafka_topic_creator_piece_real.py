import logging
import os
import time

from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient
from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs
from pydantic import BaseModel

from .models import InputModel, SecretsModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)


def dump_with_secrets(model: BaseModel, by_alias=False) -> dict:
    data = {}
    for k, v in model.__dict__.items():
        k = model.model_fields[k].alias if by_alias else k
        if hasattr(v, "get_secret_value"):
            data[k] = v.get_secret_value()
        else:
            data[k] = v
    return data


@skip_envs('github')
def test_kafka_topic_creator_piece():
    admin_conf = {
        "bootstrap.servers": os.getenv("kafka.bootstrap.servers", ""),
        "security.protocol": "SSL",
        "ssl.ca.pem": os.getenv("kafka.ssl.ca.pem", "").replace("\\n", "\n"),
        "ssl.certificate.pem": os.getenv("kafka.ssl.certificate.pem", "").replace("\\n", "\n"),
        "ssl.key.pem": os.environ.get("kafka.ssl.key.pem", "").replace("\\n", "\n"),
        "ssl.endpoint.identification.algorithm": "none",
    }

    piece_conf = {
        **admin_conf,
        "topics": ["__test-real-topic1", "__test-real-topic2"],
        "cleanup.policy": ["delete"],
        "retention.ms": 1000,
    }

    input_model = InputModel(**piece_conf)
    secrets_model = SecretsModel(**piece_conf)

    admin = AdminClient(conf=admin_conf)

    # 1) Send delete request
    fs = admin.delete_topics(input_model.topics, operation_timeout=10)
    for t, f in fs.items():
        try:
            f.result()
            logger.info(f"Delete request sent for topic: {t}")
        except Exception as e:
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                logger.warning(f"At least one of the topics '{input_model.topics}' does not exist")
                continue
            logger.error(f"Delete request error: {e}")
            raise

    # 2) Now poll metadata until the topics disappear
    start_time = time.time()
    while True:
        if time.time() - start_time > 60:
            logger.error(f"poll timeout")
            raise
        md = admin.list_topics(timeout=10)  # fetch cluster metadata
        topics = md.topics.keys()  # existing topic names

        if all(topic not in topics for topic in input_model.topics):
            logger.info(f"Topics '{input_model.topics}' have been removed.")
            break

        logger.info(f"At least one of the topics '{input_model.topics}' still exists, waiting…")
        time.sleep(1)  # wait a bit before retrying

    output = piece_dry_run(
        piece_name="KafkaTopicCreatorPiece",
        input_data=input_model.model_dump(by_alias=True),
        secrets_data=dump_with_secrets(secrets_model, by_alias=True),
    )

    # Assertions
    assert output is not None

    # Poll metadata until the topics appear
    start_time = time.time()
    while True:
        if time.time() - start_time > 60:
            logger.error(f"poll timeout")
            raise
        md = admin.list_topics(timeout=10)  # fetch cluster metadata
        topics = md.topics.keys()  # existing topic names

        if all(topic in topics for topic in input_model.topics):
            logger.info(f"Topics '{input_model.topics}' have been created.")
            break

        logger.info(f"At least one of the topics '{input_model.topics}' is not yet created, waiting…")
        time.sleep(1)  # wait a bit before retrying
