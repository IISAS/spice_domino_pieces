from typing import Literal, List, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator


class SecretsModel(BaseModel):
    ssl_ca_pem: Optional[str | None] = Field(
        title="ssl.ca.pem",
        default=None,
        description="CA certificate in PEM format as a single line string with new line characters replaced with \\n.",
    )
    ssl_certificate_pem: Optional[str | None] = Field(
        title="ssl.certificate.pem",
        default=None,
        description="Client's certificate in PEM format as a single line string with new line characters replaced with \\n."
    )
    ssl_key_pem: Optional[SecretStr | None] = Field(
        title="ssl.key.pem",
        default=None,
        description="Client's private key in PEM format as a single line string with new line characters replaced with \\n.",
    )


class InputModel(BaseModel):
    bootstrap_servers: List[str] = Field(
        title="bootstrap.servers",
        default=["spice-kafka-broker-1.stevo.fedcloud.eu:9093"],
        description="The Kafka broker address",
    )

    # https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_security.protocol
    # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#security-protocol
    security_protocol: Optional[Literal["PLAINTEXT", "SSL"]] = Field(
        title="security.protocol",
        default=None,
        description="Protocol used to communicate with brokers.",
    )

    @field_validator("security_protocol", mode="before")
    def validate_security_protocol(cls, v: str) -> str:
        allowed = {"PLAINTEXT", "SSL"}
        normalized = v.upper()  # normalize to uppercase
        if normalized not in allowed:
            raise ValueError(f"Invalid security protocol: {v}. Must be one of (case insensitive) {allowed}")
        return normalized  # return normalized value

    # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#ssl-endpoint-identification-algorithm
    # https://kafka.apache.org/41/configuration/producer-configs/#producerconfigs_ssl.endpoint.identification.algorithm
    ssl_endpoint_identification_algorithm: str = Field(
        title="ssl.endpoint.identification.algorithm",
        default="none",
        description="The endpoint identification algorithm to validate server hostname using server certificate.",
    )

    # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#acks
    # https://kafka.apache.org/41/configuration/producer-configs/#producerconfigs_acks
    acks_raw: Literal["fire_and_forget", "wait_for_leader", "all"] = Field(
        title="acks",
        default="all",
        description="The number of acknowledgments the producer requires the leader to have received before considering a request complete.",
    )

    @property
    def acks(self) -> str:
        return {
            "fire_and_forget": "0",
            "wait_for_leader": "1",
            "all": "all",
        }[self.acks_raw]

    # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#enable-idempotence
    # https://kafka.apache.org/41/configuration/producer-configs/#producerconfigs_enable.idempotence
    enable_idempotence_bool: bool = Field(
        title="enable.idempotence",
        default=True,
        description="Whether to ensure that exactly one copy of each message is written in the stream.",
    )

    @property
    def enable_idempotence(self) -> str:
        return str(self.enable_idempotence_bool)

    messages_file_path: str = Field(
        title="messages.file.path",
        default="messages.jsonl",
        description="Path to a file containing messages to produce by this producer.",
    )


class OutputModel(BaseModel):
    topics: List[str] = Field(
        title="topics",
        default=["topic.default1", "topic.default2"],
        description="Topic name",
    )
    num_produced_messages: int = Field(
        title="num.produced.messages",
        description="The number of produced messages."
    )
    bootstrap_servers: Optional[List[str] | None] = Field(
        title="bootstrap.servers",
        default=None,
        description="Kafka broker addresses",
    )
    security_protocol: Optional[Literal["PLAINTEXT", "SSL"]] = Field(
        title="security.protocol",
        default=None,
        description="Protocol used to communicate with brokers.",
    )
