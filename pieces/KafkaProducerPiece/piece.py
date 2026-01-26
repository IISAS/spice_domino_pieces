import json
import os
import time
from pathlib import Path

from confluent_kafka import Producer
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel, SecretsModel
from confluent_kafka.serialization import StringSerializer

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
        num_invalid_json_message_lines = 0
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
        encoding = "utf-8"
        serializer = StringSerializer(encoding)
        with open(messages_file_path, "r", encoding=encoding) as fp:

            self.logger.info(f"Producing messages from file: {messages_file_path}")

            line = 0
            for msg_line in fp:
                # Serve on_delivery callbacks from previous calls to produce()
                producer.poll(0.0)
                line += 1
                if len(msg_line.strip()) < 1 or msg_line.lstrip().startswith('#'):
                    continue

                try:
                    record = json.loads(msg_line)
                    keys = {"topic", "value"}
                    msg = {k: record[k] for k in keys}
                except Exception as e:
                    num_invalid_json_message_lines+=1
                    self.logger.warning(f"Failed to parse JSON message on line {line}: '{msg_line}'\n{e}")
                    continue

                keys_optional = {"key", "partition"}
                msg.update({k: record[k] for k in keys_optional if k in record})
                msg.update(dict(on_delivery=delivery_report))

                keys_serialize = {"key", "value"}
                msg.update({k: serializer(msg[k]) for k in keys_serialize})

                producer.produce(**msg)

                self.logger.debug("Flushing producer...")
                producer.flush()
                topics.add(msg.get("topic"))

        duration = time.time() - start_time

        result = {
            "messages_file_path": str(messages_file_path),
            "num_delivered_messages": num_delivered_messages,
            "num_undelivered_messages": num_undelivered_messages,
            "num_invalid_json_message_lines": num_invalid_json_message_lines,
            "duration": duration,
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
