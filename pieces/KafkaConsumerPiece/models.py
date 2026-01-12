from typing import List, Literal

from pydantic import BaseModel, Field, SecretStr

_DEFAULT_MESSAGE_POLLING_TIMEOUT = 5.0
_DEFAULT_NO_MESSAGE_TIMEOUT = 10.0

_AutoOffsetReset = Literal["smallest", "earliest", "beginning", "largest", "latest", "end", "error"]
_MessageEncodings = Literal["utf-8", "base64"]


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
    KafkaConsumer Piece Input Model
    """

    topics: List[str] = Field(
        title="topic name",
        default=["default-topic"],
        description="Topic name",
        # json_schema_extra={"from_upstream": "always"}
    )

    bootstrap_servers: str = Field(
        default="spice.stevo.fedcloud.eu:9093",
        description="The Kafka broker address",
    )

    security_protocol: str = Field(
        default="SSL",
        description="Security protocol",
    )

    group_id: str = Field(
        default="default-group",
        description="Kafka consumer group",
    )

    auto_offset_reset: _AutoOffsetReset = Field(
        default="earliest",
        description="Kafka consumer auto reset offset",
    )

    message_polling_timeout: float = Field(
        default=_DEFAULT_MESSAGE_POLLING_TIMEOUT,
        description="Timeout in seconds for polling messages.",
    )

    no_message_timeout: float = Field(
        default=_DEFAULT_NO_MESSAGE_TIMEOUT,
        description="Timeout in seconds to stop polling if there are no messages arriving.",
    )

    msg_value_encoding: _MessageEncodings = Field(
        default="utf-8",
        description="Encoding of messages",
    )


class OutputModel(BaseModel):
    """
    KafkaConsumer Piece Output Model
    """
    messages_file_path: str = Field(
        default="consumed_messages.jsonl",
        description="File with consumed messages."
    )
    topics: List[str] = Field(
        title="topic name",
        default=["default-topic"],
        description="Topic name",
    )
    group_id: str = Field(
        default="default-group",
        description="Kafka consumer group",
    )
    msg_value_encoding: _MessageEncodings = Field(
        default="utf-8",
        description="Encoding of messages",
    )
