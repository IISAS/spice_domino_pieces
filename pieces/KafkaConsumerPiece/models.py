from __future__ import annotations

import re
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator

# ISO8601 duration regex (simplified for PnDTnHnMn.nS, no negative durations)
ISO8601_DURATION_REGEX = re.compile(
    r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$"
)


class SecretsModel(BaseModel):
    ssl_ca_pem: Optional[str] = Field(
        title="ssl.ca.pem",
        default=None,
        description="CA certificate in PEM format as a single line string with new line characters replaced with \\n.",
    )
    ssl_certificate_pem: Optional[str] = Field(
        title="ssl.certificate.pem",
        default=None,
        description="Client's certificate in PEM format as a single line string with new line characters replaced with \\n."
    )
    ssl_key_pem: Optional[SecretStr] = Field(
        title="ssl.key.pem",
        default=None,
        description="Client's private key in PEM format as a single line string with new line characters replaced with \\n.",
    )


class InputModel(BaseModel):
    bootstrap_servers: List[str] = Field(
        title="bootstrap.servers",
        default=["spice-kafka-broker-1.stevo.fedcloud.eu:9093"],
        description="Kafka broker addresses",
    )

    # https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_security.protocol
    # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#security-protocol
    security_protocol: Literal["PLAINTEXT", "SSL"] = Field(
        title="security.protocol",
        default="PLAINTEXT",
        description="Protocol used to communicate with brokers.",
    )

    @field_validator("security_protocol")
    def validate_security_protocol(cls, value: str) -> str:
        allowed = {"PLAINTEXT", "SSL"}
        normalized = value.upper()  # normalize to uppercase
        if normalized not in allowed:
            raise ValueError(f"Invalid security protocol: {value}. Must be one of (case insensitive) {allowed}")
        return normalized  # return normalized value

    topics: List[str] = Field(
        title="topics",
        default=["topic.default1", "topic.default2"],
        description="Topic names",
    )

    @field_validator("topics")
    def validate_topics(cls, value: List[str]) -> List[str]:
        if value is None or len(value) == 0 or any(topic is None or topic.strip() == "" for topic in value):
            raise ValueError("topics cannot be empty, contain empty strings or None elements")
        return value

    # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#ssl-endpoint-identification-algorithm
    # https://kafka.apache.org/41/configuration/producer-configs/#producerconfigs_ssl.endpoint.identification.algorithm
    ssl_endpoint_identification_algorithm: str = Field(
        title="ssl.endpoint.identification.algorithm",
        default="none",
        description="The endpoint identification algorithm to validate server hostname using server certificate.",
    )

    # https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_client.id
    # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#client-id
    client_id: str = Field(
        title="client.id",
        default="test-client",
        description="An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.",
    )

    # https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_group.id
    # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#group-id
    group_id: str = Field(
        title="group.id",
        default="test-consumer-group",
        description="A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the group management functionality by using subscribe(topic) or the Kafka-based offset management strategy.",
    )

    # https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_auto.offset.reset
    # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#auto-offset-reset
    auto_offset_reset: str = Field(
        title="auto.offset.reset",
        default="latest",
        description="""What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):
earliest: automatically reset the offset to the earliest offset
latest: automatically reset the offset to the latest offset
by_duration:<duration>: automatically reset the offset to a configured <duration> from the current timestamp. <duration> must be specified in ISO8601 format (PnDTnHnMn.nS). Negative duration is not allowed.
none: throw exception to the consumer if no previous offset is found for the consumer's group
anything else: throw exception to the consumer."""
        ,
    )

    @field_validator("auto_offset_reset", mode="before")
    def validate_auto_offset_reset(cls, value: str) -> str:
        v_lower = value.lower()
        allowed_literals = {"latest", "earliest", "none"}

        if v_lower in allowed_literals:
            return v_lower

        # Check for pattern "by_duration:<duration>"
        if v_lower.startswith("by_duration:"):
            duration_str = v_lower[len("by_duration:"):]
            if not ISO8601_DURATION_REGEX.match(duration_str):
                raise ValueError(
                    f"Invalid ISO8601 duration format in auto_offset_reset: {duration_str}"
                )
            return f"by_duration:{duration_str}"  # preserve the original pattern

        raise ValueError(
            f"Invalid auto_offset_reset value: {value}. Must be one of "
            f"{allowed_literals} or by_duration:<ISO8601 duration>"
        )

    poll_timeout: float = Field(
        title="poll.timeout",
        default=60,
        description="Timeout in seconds for polling messages.",
    )

    @field_validator("poll_timeout")
    def validate_poll_timeout(cls, value, info):
        if value <= 0:
            raise ValueError("poll_timeout must be greater than 0")
        return value

    msg_value_encoding: str = Field(
        title="msg.value.encoding",
        default="utf-8",
        description="Encoding of messages",
    )


class OutputModel(BaseModel):
    bootstrap_servers: List[str] = Field(
        title="bootstrap.servers",
        description="Kafka broker addresses",
    )
    security_protocol: Literal["PLAINTEXT", "SSL"] = Field(
        title="security.protocol",
        description="Protocol used to communicate with brokers.",
    )
    messages_file_path: str = Field(
        title="messages.file.path",
        description="File with consumed messages."
    )
    topics: List[str] = Field(
        title="topics",
        description="Topic name",
    )
    group_id: str = Field(
        title="group.id",
        description="Kafka consumer group",
    )
    msg_value_encoding: str = Field(
        title="msg.value.encoding",
        description="Encoding of messages; i.e., 'utf-8', 'base64'",
    )
