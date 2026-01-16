from typing import List, Literal

from pydantic import BaseModel, Field, SecretStr

_DEFAULT_MESSAGE_POLLING_TIMEOUT = 5.0
_DEFAULT_NO_MESSAGE_TIMEOUT = 10.0

_AutoOffsetReset = Literal["smallest", "earliest", "beginning", "largest", "latest", "end", "error"]
_MessageEncodings = Literal["utf-8", "base64"]


class SecretsModel(BaseModel):
    ssl_ca_pem: SecretStr = Field(
        alias="ssl.ca.pem",
        description="CA certificate in PEM format",
    )
    ssl_certificate_pem: SecretStr = Field(
        alias="ssl.certificate.pem",
        description="Client's certificate in PEM format"
    )
    ssl_key_pem: SecretStr = Field(
        alias="ssl.key.pem",
        description="Client's private key in PEM format",
    )


class InputModel(BaseModel):
    """
    KafkaConsumer Piece Input Model
    """

    topics: List[str] = Field(
        default=["default-topic"],
        description="Topic name",
        # json_schema_extra={"from_upstream": "always"}
    )

    bootstrap_servers: List[str] = Field(
        alias="bootstrap.servers",
        default=["spice-kafka-broker-1.stevo.fedcloud.eu:9093"],
        description="Kafka broker addresses",
    )

    security_protocol: str = Field(
        alias="security.protocol",
        default="SSL",
        description="Security protocol",
    )

    group_id: str = Field(
        alias="group.id",
        default="default-group",
        description="Kafka consumer group",
    )

    auto_offset_reset: _AutoOffsetReset = Field(
        alias="auto.offset.reset",
        default="earliest",
        description="Kafka consumer auto reset offset",
    )

    message_polling_timeout: float = Field(
        alias="message.polling.timeout",
        default=_DEFAULT_MESSAGE_POLLING_TIMEOUT,
        description="Timeout in seconds for polling messages.",
    )

    no_message_timeout: float = Field(
        alias="no.message.timeout",
        default=_DEFAULT_NO_MESSAGE_TIMEOUT,
        description="Timeout in seconds to stop polling if there are no messages arriving.",
    )

    msg_value_encoding: _MessageEncodings = Field(
        alias="msg.value.encoding",
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
