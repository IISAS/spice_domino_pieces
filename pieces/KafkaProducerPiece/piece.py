import json
import time
from pathlib import Path

from confluent_kafka import Producer
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel, SecretsModel


class KafkaProducerPiece(BasePiece):

    def piece_function(self, input_data: InputModel, secrets_data: SecretsModel):

        if input_data.topic is None or input_data.topic.strip() == "":
            raise Exception("topic cannot be empty or None")

        if input_data.messages_filename is None or input_data.messages_filename is None:
            raise Exception("messages filename cannot be empty or None")

        messages_path = Path(input_data.messages_filename)
        if not messages_path.exists():
            raise Exception(f"messages file not found: {messages_path}")

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

        conf = {
            # 'debug': 'security,broker,conf',
            # 'log_level': 7,
            'bootstrap.servers': input_data.bootstrap_servers,
            'acks': input_data.acks,
            'enable.idempotence': input_data.enable_idempotence,
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

        producer = Producer(conf)

        num_delivered_messages = 0
        num_undelivered_messages = 0
        start_time = time.time()

        def delivery_report(err, msg):
            nonlocal num_delivered_messages
            nonlocal num_undelivered_messages
            if err is not None:
                num_undelivered_messages += 1
                self.logger.error(f"Message delivery failed: {err}")
            else:
                num_delivered_messages += 1
                self.logger.info(
                    f"Produced message to topic {msg.topic()} "
                    f"[partition {msg.partition()}] @ offset {msg.offset()}"
                )

        with open(messages_path, "r", encoding="utf-8") as fp:

            self.logger.info(f"Producing messages from file: {messages_path}")

            for line in fp:
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    self.logger.warning(f"Skipping invalid JSON line: {line}")
                    continue

                value = record.get("value")
                key = record.get("key")
                partition = record.get("partition")

                producer.produce(
                    topic=input_data.topic,
                    value=value.encode("utf-8") if value is not None else None,
                    key=key.encode("utf-8") if key is not None else None,
                    partition=partition,
                    on_delivery=delivery_report
                )

        self.logger.info("Flushing producer...")
        producer.flush()

        # Set display result
        self.display_result = {
            "topic": input_data.topic,
            "messages_produced": num_delivered_messages,
            "input_file": str(messages_path),
            "duration": time.time() - start_time
        }

        # Return output
        return OutputModel(
            num_produced_messages=num_delivered_messages
        )
