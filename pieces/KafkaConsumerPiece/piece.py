import base64
import json
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from domino.base_piece import BasePiece

from .models import _DEFAULT_NO_MESSAGE_TIMEOUT, _DEFAULT_MESSAGE_POLLING_TIMEOUT, InputModel, OutputModel, SecretsModel


def decode_msg_value(msg_value, encoding):
    if msg_value is None:
        return None
    if encoding == "base64":
        return base64.b64decode(msg_value)
    elif encoding == "utf-8":
        return msg_value.decode('utf-8')
    return msg_value


class KafkaConsumerPiece(BasePiece):

    def piece_function(self, input_data: InputModel, secrets_data: SecretsModel):

        if input_data.topics is None or any(x is None or x.strip() == "" for x in input_data.topics):
            raise Exception("topics cannot be empty, contain empty strings or None elements")

        if input_data.security_protocol is not None and input_data.security_protocol.lower().strip() == "ssl":
            if secrets_data.KAFKA_CA_CERT_PEM is None:
                raise Exception(
                    "KAFKA_CA_CERT_PEM not found in ENV vars. Please add it to the secrets section of the Piece.")
            if secrets_data.KAFKA_CERT_PEM is None:
                raise Exception(
                    "KAFKA_CERT_PEM not found in ENV vars. Please add it to the secrets section of the Piece.")
            if secrets_data.KAFKA_KEY_PEM is None:
                raise Exception(
                    "KAFKA_KEY_PEM not found in ENV vars. Please add it to the secrets section of the Piece.")

        if input_data.message_polling_timeout is None or input_data.message_polling_timeout <= 0.0:
            self.logger.warning(
                "message_polling_timeout was set to infinite and will be set to {} seconds".format(
                    _DEFAULT_MESSAGE_POLLING_TIMEOUT))
            input_data.message_polling_timeout = _DEFAULT_MESSAGE_POLLING_TIMEOUT

        if input_data.no_message_timeout is None or input_data.no_message_timeout < input_data.message_polling_timeout:
            self.logger.warning(
                "no_message_timeout was set to lower than message_polling_timeout and will be set to {} seconds".format(
                    _DEFAULT_NO_MESSAGE_TIMEOUT))
            input_data.no_message_timeout = _DEFAULT_NO_MESSAGE_TIMEOUT

        conf = {
            # 'debug': 'security,broker,conf',
            # 'log_level': 7,
            'auto.offset.reset': input_data.auto_offset_reset,
            'bootstrap.servers': input_data.bootstrap_servers,
            'group.id': input_data.group_id,
            **(
                {
                    'security.protocol': input_data.security_protocol,
                    'ssl.ca.pem': secrets_data.KAFKA_CA_CERT_PEM.get_secret_value(),
                    'ssl.certificate.pem': secrets_data.KAFKA_CERT_PEM.get_secret_value(),
                    # https://github.com/confluentinc/librdkafka/issues/4349
                    'ssl.endpoint.identification.algorithm': 'none',
                    'ssl.key.pem': secrets_data.KAFKA_KEY_PEM.get_secret_value(),
                } if input_data.security_protocol is not None
                     and input_data.security_protocol.lower().strip() == 'ssl'
                else {}
            ),
        }

        c = Consumer(conf)
        c.subscribe(topics=input_data.topics)

        messages_file_path = str(Path(self.results_path) / "messages.jsonl")
        self.logger.info("creating output file for polled messages: {}".format(messages_file_path))
        fp = open(messages_file_path, "w", encoding="utf-8")

        self.logger.info("Waiting for messages...")
        start_time = time.time()

        while True:
            msg = c.poll(timeout=input_data.message_polling_timeout)  # wait up to message_polling_timeout seconds
            if msg is None:
                if time.time() - start_time > input_data.no_message_timeout:  # stop after no_message_timeout seconds with no messages
                    self.logger.info("No messages received.")
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    self.logger.error(f"Consumer error: {msg.error()}")
                    break
            else:
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

        fp.close()
        c.close()

        # Set display result
        self.display_result = {
            "group.id": input_data.group_id,
            "messages_file_path": messages_file_path,
            "duration": time.time() - start_time,
        }

        # Return output
        return OutputModel(
            messages_file_path=messages_file_path,
            topics=input_data.topics,
            group_id=input_data.group_id,
            msg_value_encoding=input_data.msg_value_encoding,
        )
