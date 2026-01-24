from typing import List, Literal, Optional

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
        description="Kafka broker addresses",
    )

    security_protocol: Optional[str | None] = Field(
        title="security.protocol",
        default=None,
        description="Security protocol",
    )

    # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#ssl-endpoint-identification-algorithm
    # https://kafka.apache.org/41/configuration/producer-configs/#producerconfigs_ssl.endpoint.identification.algorithm
    ssl_endpoint_identification_algorithm: str = Field(
        title="ssl.endpoint.identification.algorithm",
        default="none",  # https://github.com/confluentinc/librdkafka/issues/4349
        description="The endpoint identification algorithm to validate server hostname using server certificate.",
    )

    topics: List[str] = Field(
        title="topics",
        default=["topic.test1", "topic.test2"],
        description="Topic names",
    )

    @field_validator("topics")
    def validate_topics(cls, value: List[str]) -> List[str]:
        if value is None or len(value) == 0 or any(topic is None or topic.strip() == "" for topic in value):
            raise ValueError("topics cannot be empty, contain empty strings or None elements")
        return value

    exists_ok: bool = Field(
        title="exists.ok",
        default=False,
        description="If exists.ok and topic exists, the topic is deleted and created again. If exists.ok=False and topic exists an exception is raised.",
    )

    num_partitions: int = Field(
        title="num.partitions",
        default=6,
        description="Number of partitions per topic",
    )

    replication_factor: int = Field(
        title="replication.factor",
        default=3,
        description="Number of replications",
    )

    #
    # Topic configs -->
    # more: https://kafka.apache.org/41/configuration/topic-configs/
    #

    # https://kafka.apache.org/41/configuration/topic-configs/#topicconfigs_cleanup.policy
    cleanup_policy: List[Literal["compact", "delete"]] = Field(
        title="cleanup.policy",
        default=["delete"],
        description="This config designates the retention policy to use on log segments. The \"delete\" policy (which is the default) will discard old segments when their retention time or size limit has been reached. The \"compact\" policy will enable log compaction, which retains the latest value for each key. It is also possible to specify both policies in a comma-separated list (e.g. \"delete,compact\"). In this case, old segments will be discarded per the retention time and size configuration, while retained segments will be compacted.",
    )

    # https://kafka.apache.org/41/configuration/topic-configs/#topicconfigs_retention.ms
    retention_ms: int = Field(
        title="retention.ms",
        default=604800000,  # 7 days
        description="This configuration controls the maximum time we will retain a log before we will discard old log segments to free up space if we are using the \"delete\" retention policy. This represents an SLA on how soon consumers must read their data. If set to -1, no time limit is applied. Additionally, retention.ms configuration operates independently of \"segment.ms\" and \"segment.bytes\" configurations. Moreover, it triggers the rolling of new segment if the retention.ms condition is satisfied.",
    )

    # https://kafka.apache.org/41/configuration/topic-configs/#topicconfigs_min.insync.replicas
    min_insync_replicas: int = Field(
        title="min.insync.replicas",
        default=2,
        description="Specifies the minimum number of in-sync replicas (including the leader) required for a write to succeed when a producer sets acks to \"all\" (or \"-1\"). In the acks=all case, every in-sync replica must acknowledge a write for it to be considered successful. E.g., if a topic has replication.factor of 3 and the ISR set includes all three replicas, then all three replicas must acknowledge an acks=all write for it to succeed, even if min.insync.replicas happens to be less than 3. If acks=all and the current ISR set contains fewer than min.insync.replicas members, then the producer will raise an exception (either NotEnoughReplicas or NotEnoughReplicasAfterAppend).\nRegardless of the acks setting, the messages will not be visible to the consumers until they are replicated to all in-sync replicas and the min.insync.replicas condition is met.\nWhen used together, min.insync.replicas and acks allow you to enforce greater durability guarantees. A typical scenario would be to create a topic with a replication factor of 3, set min.insync.replicas to 2, and produce with acks of \"all\". This ensures that a majority of replicas must persist a write before it's considered successful by the producer and it's visible to consumers.\n\nNote that when the Eligible Leader Replicas feature is enabled, the semantics of this config changes. Please refer to the ELR section for more info.",
    )

    #
    # <-- Topic configs
    #


class OutputModel(BaseModel):
    topics: List[str] = Field(
        title="topics",
        default=[],
        description="Names of created topics",
    )
