import json
import os
import time
from pathlib import Path

from confluent_kafka import Producer
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel, SecretsModel


class KafkaProducerPiece(BasePiece):

    def piece_function(self, input_data: InputModel, secrets_data: SecretsModel):

        messages_file_path = Path(input_data.messages_file_path)
        if not messages_file_path.exists():
            raise Exception(f"messages file not found: {messages_file_path}")

        if input_data.security_protocol is not None and input_data.security_protocol.lower().strip() == "ssl":
            if secrets_data.ssl_ca_pem is None:
                raise Exception("Please, set the 'ssl_ca_pem' in the Repository Secrets section.")
            else:
                self.logger.info('ssl_ca_pem: %s' % secrets_data.ssl_ca_pem)
            if secrets_data.ssl_certificate_pem is None:
                raise Exception("Please, set the 'ssl_certificate_pem' in the Repository Secrets section.")
            else:
                self.logger.info('ssl_certificate_pem: %s' % secrets_data.ssl_certificate_pem)
            if secrets_data.ssl_key_pem is None:
                raise Exception("Please, set the 'ssl_key_pem' in the Repository Secrets section.")
            else:
                self.logger.info('ssl_key_pem: %s' % secrets_data.ssl_key_pem)

        producer_conf = {
            # 'debug': 'security,broker,conf',
            # 'log_level': 7,
            'acks': input_data.acks,
            'bootstrap.servers': ','.join(input_data.bootstrap_servers),
            'enable.idempotence': input_data.enable_idempotence,
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

        producer = Producer(producer_conf)

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

        topics = set()
        with open(messages_file_path, "r", encoding="utf-8") as fp:

            self.logger.info(f"Producing messages from file: {messages_file_path}")

            for msg_line in fp:
                if not msg_line.strip():
                    continue

                try:
                    record = json.loads(msg_line)
                except json.JSONDecodeError:
                    self.logger.warning(f"Skipping invalid JSON line: {msg_line}")
                    continue

                topic = record.get("topic")
                value = record.get("value")
                key = record.get("key")
                partition = record.get("partition")

                producer.produce(
                    topic=topic,
                    value=value.encode("utf-8") if value is not None else None,
                    key=key.encode("utf-8") if key is not None else None,
                    partition=partition if partition is not None else 0,
                    on_delivery=delivery_report
                )
                topics.add(topic)
                self.logger.info(
                    f"Produced message to topic {topic} with key {key}, partition {partition} and value {value}"
                )

        self.logger.info("Flushing producer...")
        producer.flush()

        duration = time.time() - start_time

        result = {
            "messages_file_path": str(messages_file_path),
            "num_delivered_messages": num_delivered_messages,
            "duration": duration
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
            num_produced_messages=num_delivered_messages,
            topics=list(topics),
        )
