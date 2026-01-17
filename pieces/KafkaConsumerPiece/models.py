from typing import List

from pydantic import BaseModel, Field, SecretStr

_DEFAULT_MESSAGE_POLLING_TIMEOUT = 5.0
_DEFAULT_NO_MESSAGE_TIMEOUT = 10.0


class SecretsModel(BaseModel):
    ssl_ca_pem: SecretStr = Field(
        default="",
        description="CA certificate in PEM format as a single line string with new line characters replaced with \\n.",
    )
    ssl_certificate_pem: SecretStr = Field(
        default="",
        description="Client's certificate in PEM format as a single line string with new line characters replaced with \\n."
    )
    ssl_key_pem: SecretStr = Field(
        default="",
        description="Client's private key in PEM format as a single line string with new line characters replaced with \\n.",
    )


class InputModel(BaseModel):
    """
    KafkaConsumer Piece Input Model
    """

    topics: List[str] = Field(
        default=["topic.default1", "topic.default2"],
        description="Topic names",
        # json_schema_extra={"from_upstream": "always"}
    )

    bootstrap_servers: List[str] = Field(
        default=["spice-kafka-broker-1.stevo.fedcloud.eu:9093"],
        description="Kafka broker addresses",
    )

    security_protocol: str = Field(
        default="SSL",
        description="Security protocol",
    )

    group_id: str = Field(
        default="group.default",
        description="Kafka consumer group",
    )

    auto_offset_reset: str = Field(
        default="earliest",
        description="Kafka consumer auto reset offset; i.e. 'smallest', 'earliest', 'beginning', 'largest', 'latest', 'end', 'error'",
    )

    message_polling_timeout: float = Field(
        default=_DEFAULT_MESSAGE_POLLING_TIMEOUT,
        description="Timeout in seconds for polling messages.",
    )

    no_message_timeout: float = Field(
        default=_DEFAULT_NO_MESSAGE_TIMEOUT,
        description="Timeout in seconds to stop polling if there are no messages arriving.",
    )

    msg_value_encoding: str = Field(
        default="utf-8",
        description="Encoding of messages",
    )


class OutputModel(BaseModel):
    """
    KafkaConsumer Piece Output Model
    """
    messages_file_path: str = Field(
        default="messages.jsonl",
        description="File with consumed messages."
    )
    topics: List[str] = Field(
        default=["topic.default1", "topic.default2"],
        description="Topic name",
    )
    group_id: str = Field(
        default="group.default",
        description="Kafka consumer group",
    )
    msg_value_encoding: str = Field(
        default="utf-8",
        description="Encoding of messages; i.e., 'utf-8', 'base64'",
    )
