import importlib
import os
from unittest.mock import patch

from confluent_kafka.admin import ClusterMetadata
from domino.testing import piece_dry_run
from mockafka import FakeAdminClientImpl
from pydantic import BaseModel

piece_name = "KafkaTopicCreatorPiece"


def dump_with_secrets(model: BaseModel, by_alias=False) -> dict:
    data = {}
    for k, v in model.__dict__.items():
        k = model.model_fields[k].alias if by_alias else k
        if hasattr(v, "get_secret_value"):
            data[k] = v.get_secret_value()
        else:
            data[k] = v
    return data


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


def test_kafka_topic_creator_piece():
    admin = MyFakeAdminClientImpl(clean=True)
    with patch("confluent_kafka.admin.AdminClient", return_value=admin):
        piece_conf = {
            "bootstrap.servers": os.getenv("kafka.bootstrap.servers", ""),
            "security.protocol": "SSL",
            "ssl.ca.pem": os.getenv("kafka.ssl.ca.pem", "").replace("\\n", "\n"),
            "ssl.certificate.pem": os.getenv("kafka.ssl.certificate.pem", "").replace("\\n", "\n"),
            "ssl.key.pem": os.environ.get("kafka.ssl.key.pem", "").replace("\\n", "\n"),
            "topics": ["fake-topic1", "fake-topic2"],
            "ssl.endpoint.identification.algorithm": "none",
        }

        piece_model_module = importlib.import_module(f"{piece_name}.models")
        InputModel = getattr(piece_model_module, "InputModel")
        SecretsModel = getattr(piece_model_module, "SecretsModel")

        input_model = InputModel(**piece_conf)
        secrets_model = SecretsModel(**piece_conf)

        output = piece_dry_run(
            piece_name=piece_name,
            input_data=input_model.model_dump(by_alias=True),
            secrets_data=dump_with_secrets(secrets_model, by_alias=True),
        )

        # Assertions
        assert output is not None

        cluster_metadata: ClusterMetadata = admin.list_topics()
        for topic in piece_conf['topics']:
            assert topic in cluster_metadata.topics
