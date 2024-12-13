import json
import time
import logging
from kafka import KafkaConsumer

class KafkaSensorAlertConsumer:
    def __init__(self, topic_prefix):
        """Initializing KafkaSensorAlertConsumer with the specified topics prefix."""
        try:
            from configs import kafka_config
        except ImportError as err:
            raise ImportError(
                "Failed to import Kafka configuration. "
                "Make sure the configs.py file exists and contains the required kafka_config dictionary."
            ) from err

        self.kafka_config = kafka_config
        self.topic_name_alerts = f"{topic_prefix}_alerts"
        self.consumer = self._configure_consumer()
        self._configure_logging()

    def _configure_logging(self):
        """Set up logging with timestamp in ISO 8601 (UTC)."""
        logging.basicConfig(
            level=logging.INFO,
            format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
            datefmt="%Y-%m-%dT%H:%M:%SZ"
        )
        logging.Formatter.converter = time.gmtime  # Log times in UTC

    def _configure_consumer(self):
        """Setting up Kafka Consumer."""
        return KafkaConsumer(
            self.topic_name_alerts,
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            security_protocol=self.kafka_config['security_protocol'],
            sasl_mechanism=self.kafka_config['sasl_mechanism'],
            sasl_plain_username=self.kafka_config['username'],
            sasl_plain_password=self.kafka_config['password'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def listen(self):
        """The main cycle for listening to messages."""
        logging.info(f"Waiting for notifications from the topics: [{self.topic_name_alerts}]...")
        try:
            for message in self.consumer:
                alert = message.value
                logging.warning(f"A notification has been received: {json.dumps(alert, indent=2)}")
        except KeyboardInterrupt:
            logging.info("The script is stopped by the user.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

def main():
    """Launching KafkaSensorAlertConsumer."""
    topic_prefix = "oleg"
    alerts_consumer = KafkaSensorAlertConsumer(topic_prefix)
    alerts_consumer.listen()

if __name__ == "__main__":
    main()

