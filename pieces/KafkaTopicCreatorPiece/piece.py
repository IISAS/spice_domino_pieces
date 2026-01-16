import base64
import tempfile
import time

from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, KafkaException
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel, SecretsModel


def decode_msg_value(msg_value, encoding):
    if msg_value is None:
        return None
    if encoding == "base64":
        return base64.b64decode(msg_value)
    elif encoding == "utf-8":
        return msg_value.decode('utf-8')
    return msg_value


class KafkaTopicCreatorPiece(BasePiece):

    def piece_function(
        self,
        input_data: InputModel,
        secrets_data: SecretsModel
    ):

        self.logger.info("Creating topics...")
        start_time = time.time()

        with tempfile.TemporaryDirectory() as tmp_dir:

            if input_data.topics is None or any(x is None or x.strip() == "" for x in input_data.topics):
                raise Exception("topics cannot be empty, contain empty strings or None elements")

            if input_data.security_protocol is not None and input_data.security_protocol.lower().strip() == "ssl":

                if secrets_data.ssl_ca_pem is None:
                    raise Exception(
                        "ssl.ca.pem is not set. Please add it to the secrets section of the Piece."
                    )
                else:
                    self.logger.info("ssl.ca.pem: %s" % secrets_data.ssl_ca_pem)

                if secrets_data.ssl_certificate_pem is None:
                    raise Exception(
                        "ssl.certificate.pem is not set. Please add it to the secrets section of the Piece."
                    )
                else:
                    self.logger.info("ssl.certificate.pem: %s" % secrets_data.ssl_certificate_pem)

                if secrets_data.ssl_key_pem is None:
                    raise Exception(
                        "ssl.key.pem is not set. Please add it to the secrets section of the Piece."
                    )
                else:
                    self.logger.info("ssl.key.pem: %s" % secrets_data.ssl_key_pem)

            admin_client_conf = {
                'debug': 'security,broker,conf',
                'log_level': 3,
                'bootstrap.servers': input_data.bootstrap_servers,
                **(
                    {
                        'security.protocol': input_data.security_protocol,
                        'ssl.ca.pem': secrets_data.ssl_ca_pem.get_secret_value(),
                        'ssl.certificate.pem': secrets_data.ssl_certificate_pem.get_secret_value(),
                        # https://github.com/confluentinc/librdkafka/issues/4349
                        'ssl.endpoint.identification.algorithm': 'none',
                        'ssl.key.pem': secrets_data.ssl_key_pem.get_secret_value(),
                    } if input_data.security_protocol is not None
                         and input_data.security_protocol.lower().strip() == 'ssl'
                    else {}
                ),
            }

            admin = AdminClient(admin_client_conf)

            topics = [
                NewTopic(
                    topic=topic_name,
                    num_partitions=input_data.num_partitions,
                    replication_factor=input_data.replication_factor,
                    config={
                        "cleanup.policy": ','.join(input_data.cleanup_policy),
                        "retention.ms": input_data.retention_ms,
                        "min.insync.replicas": input_data.min_insync_replicas,
                    },
                ) for topic_name in input_data.topics
            ]

            topics_created = []
            fs = admin.create_topics(topics)
            for topic, f in fs.items():
                try:
                    f.result()
                    topics_created.append(topic)
                except KafkaException as e:
                    if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                        self.logger.warning(f"Topic '{topic}' already exists")
                    else:
                        self.logger.error(f"Could not create topic '{topic}': {e}")
                        raise

            # Set display result
            self.display_result = {
                "duration": time.time() - start_time,
            }

            # Return output
            return OutputModel(
                topics_created=topics_created,
            )
