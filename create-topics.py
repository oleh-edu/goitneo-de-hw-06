from kafka.admin import KafkaAdminClient, NewTopic

def main():
    try:
        from configs import kafka_config
    except ImportError as err:
        raise ImportError(
            "Failed to import Kafka configuration. " 
            "Make sure the configs.py file exists and contains the required kafka_config dictionary."
        ) from err

    # Creating a KafkaAdminClient
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )

    # Determining the Kafka topics
    topic_prefix = "oleg"
    topics = [
        NewTopic(name=f'{topic_prefix}_building_sensors', num_partitions=1, replication_factor=1),
        NewTopic(name=f'{topic_prefix}_alerts', num_partitions=1, replication_factor=1),
    ]

    # Creating Kafka topics
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("[info] Kafka topics have been created successfully.")

    # Displaying created topics
    print("[info] List of topics with the prefix:")
    [print(f"- {topic}") for topic in admin_client.list_topics() if topic_prefix in topic]

if __name__ == "__main__":
    main()
