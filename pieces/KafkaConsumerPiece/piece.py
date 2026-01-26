import base64
import json
import os
import time
from pathlib import Path

from confluent_kafka import Consumer
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


class KafkaConsumerPiece(BasePiece):

    def validate_ssl_secrets(self, input: InputModel, secrets: SecretsModel) -> None:

        if input.security_protocol and input.security_protocol.upper() == "SSL":
            if secrets is None:
                raise ValueError(
                    "Secrets must be provided when security.protocol is 'SSL'"
                )

            missing = [
                name for name, value in {
                    "ssl.ca.pem": secrets.ssl_ca_pem,
                    "ssl.certificate.pem": secrets.ssl_certificate_pem,
                    "ssl.key.pem": secrets.ssl_key_pem.get_secret_value() if secrets.ssl_key_pem else None,
                }.items()
                if value is None or value.strip() == ""
            ]

            if missing:
                raise ValueError(
                    f"When security.protocol='SSL', the following secrets must be set: "
                    f"{', '.join(missing)}"
                )

    def piece_function(self, input_data: InputModel, secrets_data: SecretsModel):

        self.validate_ssl_secrets(input_data, secrets_data)

        print(input_data.group_id)

        consumer_conf = {
            # 'debug': 'security,broker,conf',
            # 'log_level': 7,
            'auto.offset.reset': input_data.auto_offset_reset,
            'bootstrap.servers': ','.join(input_data.bootstrap_servers),
            'client.id': input_data.client_id,
            'enable.partition.eof': str(False),  # if true, KafkaError._PARTITION_EOF is raised, otherwise nothing
            'group.id': input_data.group_id,
            'security.protocol': input_data.security_protocol,
            **(
                {
                    'ssl.ca.pem': secrets_data.ssl_ca_pem.replace("\\n", "\n"),
                    'ssl.certificate.pem': secrets_data.ssl_certificate_pem.replace("\\n", "\n"),
                    'ssl.key.pem': secrets_data.ssl_key_pem.get_secret_value().replace("\\n", "\n"),
                    # https://github.com/confluentinc/librdkafka/issues/4349
                    'ssl.endpoint.identification.algorithm': input_data.ssl_endpoint_identification_algorithm,
                } if input_data.security_protocol is not None
                     and input_data.security_protocol.lower().strip() == 'ssl'
                else {}
            ),
        }

        consumer = Consumer(consumer_conf, logger=self.logger)
        consumer.subscribe(topics=input_data.topics)

        messages_file_path = str(Path(self.results_path) / "messages.jsonl")
        self.logger.info("creating output file for polled messages: {}".format(messages_file_path))
        fp = open(messages_file_path, "w", encoding="utf-8")

        num_messages = 0

        poll_timeout = input_data.poll_timeout
        start_time = time.time()

        while poll_timeout > 0:
            self.logger.debug(f"poll(timeout={poll_timeout})")
            last_poll_time = time.time()
            msg = consumer.poll(timeout=poll_timeout)
            if msg is None:
                self.logger.debug("no message received")
            else:
                if msg.error():
                    # handle KafkaError._PARTITION_EOF error if enable.partition.eof is True
                    # if msg.error().code() == KafkaError._PARTITION_EOF:
                    #     continue
                    self.logger.error(f"Consumer error: {msg.error()}")
                    break
                msg_value = msg.value()
                msg_value_decoded = decode_msg_value(msg_value, input_data.msg_value_encoding)
                self.logger.info(f"Consumed message: {msg_value_decoded} from topic {msg.topic()}")
                data = {
                    'timestamp': msg.timestamp(),
                    'topic': msg.topic(),
                    'headers': msg.headers(),
                    'key': msg.key().decode('utf-8') if msg.key() else None,
                    'latency': msg.latency(),
                    'leader_epoch': msg.leader_epoch(),
                    'offset': msg.offset(),
                    'partition': msg.partition(),
                    'value': msg_value_decoded,
                }
                fp.write(json.dumps(data) + '\n')
                num_messages += 1
            time_delta = time.time() - last_poll_time
            poll_timeout -= time_delta

        duration = time.time() - start_time

        fp.close()
        consumer.close()

        result = {
            "messages_file_path": messages_file_path,
            "msg_value_encoding": input_data.msg_value_encoding,
            "topics": input_data.topics,
            "group_id": input_data.group_id,
            "duration": duration,
            "num_messages": num_messages,
        }

        result_file_path = os.path.join(Path(self.results_path), "result.json")
        with open(result_file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f)

        # Set display result
        self.display_result = {
            "file_type": "json",
            "file_path": result_file_path
        }

        # Return output
        return OutputModel(
            bootstrap_servers=input_data.bootstrap_servers,
            security_protocol=input_data.security_protocol,
            messages_file_path=messages_file_path,
            topics=input_data.topics,
            group_id=input_data.group_id,
            msg_value_encoding=input_data.msg_value_encoding,
        )
