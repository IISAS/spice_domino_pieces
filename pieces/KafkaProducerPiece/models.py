from typing import Literal

from pydantic import BaseModel, Field, SecretStr

_DEFAULT_MESSAGE_POLLING_TIMEOUT = 5.0
_DEFAULT_NO_MESSAGE_TIMEOUT = 10.0


class SecretsModel(BaseModel):
    KAFKA_CA_CERT_PEM: SecretStr = Field(
        description="CA certificate in PEM format",
    )
    KAFKA_CERT_PEM: SecretStr = Field(
        description="Client's certificate in PEM format"
    )
    KAFKA_KEY_PEM: SecretStr = Field(
        description="Client's private key in PEM format",
    )


class InputModel(BaseModel):
    """
    KafkaProducer Piece Input Model
    """

    topic: str = Field(
        title="topic name",
        default="default-output-topic",
        description="Topic name",
        # json_schema_extra={"from_upstream": "always"}
    )

    partition: int | None = Field(
        title="partition",
        default=None,
        description="Partition number",
    )

    acks: Literal["fire_and_forget", "wait_for_leader", "all"] = Field(
        title="acks",
        default="fire_and_forget",
        description="The number of acknowledgments the producer requires the leader to have received before considering a request complete.",
    )

    enable_idempotence: bool = Field(
        title="enable.idempotence",
        default=True,
        description="Whether to ensure that exactly one copy of each message is written in the stream.",
    )

    messages_filename: str = Field(
        title="messages filename",
        default="messages.jsonl",
        description="Topic name",
    )

    bootstrap_servers: str = Field(
        default="spice.stevo.fedcloud.eu:9093",
        description="The Kafka broker address",
    )

    security_protocol: str = Field(
        default="SSL",
        description="Security protocol",
    )


class OutputModel(BaseModel):
    """
    KafkaProducer Piece Output Model
    """
    num_produced_messages: int = Field(
        description="The number of produced messages."
    )
