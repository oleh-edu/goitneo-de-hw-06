from kafka.admin import KafkaAdminClient, NewTopic

try:
    from configs import kafka_config
except ImportError as err:
    raise ImportError(
        "Failed to import Kafka configuration. "
        "Make sure the configs.py file exists and contains the required kafka_config dictionary."
    ) from err

def delete_topics(topic_prefix):
    """Delete Kafka topics."""
    topic_name_sensors = f'{topic_prefix}_building_sensors'
    topic_name_alerts = f'{topic_prefix}_alerts'

    topics_to_delete = [
        topic_name_alerts,
        topic_name_sensors
    ]

    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )

    try:
        admin_client.delete_topics(topics=topics_to_delete)
        print(f"[info] Topics deleted successfully: {', '.join(topics_to_delete)}")
    except Exception as e:
        print(f"[error] Failed to delete topics: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    topic_prefix = "oleg"
    delete_topics(topic_prefix)
