import json
import time
import random
import hashlib
import logging
from kafka import KafkaProducer
from datetime import datetime, timezone

class KafkaSensorDataProducer:
    def __init__(self, topic_prefix):
        """Initialize the producer and parameters."""
        try:
            from configs import kafka_config
        except ImportError as err:
            raise ImportError(
                "Failed to import Kafka configuration. "
                "Make sure the configs.py file exists and contains the required kafka_config dictionary."
            ) from err

        self.kafka_config = kafka_config
        self.topic_name = f'{topic_prefix}_building_sensors'
        self.producer = self._configure_producer()
        self.sensor_id = self._generate_sensor_id()
        self._configure_logging()

    def _configure_logging(self):
        """Set up logging with timestamp in ISO 8601 (UTC)."""
        logging.basicConfig(
            level=logging.INFO,
            format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
            datefmt="%Y-%m-%dT%H:%M:%SZ"
        )
        logging.Formatter.converter = time.gmtime  # Log times in UTC

    def _configure_producer(self):
        """Setting up Kafka Producer."""
        return KafkaProducer(
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            security_protocol=self.kafka_config['security_protocol'],
            sasl_mechanism=self.kafka_config['sasl_mechanism'],
            sasl_plain_username=self.kafka_config['username'],
            sasl_plain_password=self.kafka_config['password'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def _generate_sensor_id(self):
        """Generating a hashed sensor ID."""
        random_seed = random.randint(1000, 9999)
        return hashlib.sha1(str(random_seed).encode()).hexdigest()

    def generate_sensor_data(self):
        """Generating data for the sensor."""
        return {
            "sensor_id": self.sensor_id,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "temperature": round(random.uniform(-15, 60), 2),  # Two decimal places
            "humidity": round(random.uniform(0, 100), 2)     # Two decimal places
        }

    def run(self):
        """Start the main data sending cycle."""
        logging.info(f"Starting to send data to topic: {self.topic_name}")
        try:
            while True:
                data = self.generate_sensor_data()
                self.producer.send(self.topic_name, value=data)
                logging.info(f"topic_id: [{self.topic_name}], data: {json.dumps(data, indent=2)}")
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Sending data is stopped.")

def main():
    """Creating an instance of KafkaSensorDataProducer and launching it."""
    topic_prefix = "oleg"
    producer = KafkaSensorDataProducer(topic_prefix)
    producer.run()

if __name__ == "__main__":
    main()
