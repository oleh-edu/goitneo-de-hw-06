import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'

import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, col, from_json, to_json, struct, expr, round
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

class SparkStreamingProcessor:
    def __init__(self, topic_prefix):
        """Initializing the SparkSession and basic parameters."""
        try:
            from configs import kafka_config
        except ImportError as err:
            raise ImportError(
                "Failed to import Kafka configuration. "
                "Make sure the configs.py file exists and contains the required kafka_config dictionary."
            ) from err

        self.kafka_config = kafka_config
        self.spark = (SparkSession.builder
                      .appName("SparkStreamingProcessor")
                      .master("local[*]")
                      .getOrCreate())
        self.checkpoint_base = "/tmp/spark-checkpoints"
        self.topic_name_sensors = f"{topic_prefix}_building_sensors"
        self.topic_name_alerts = f"{topic_prefix}_alerts"
        shutil.rmtree(self.checkpoint_base, ignore_errors=True)

    def _configure_kafka_stream(self, topic, schema=None):
        """Setting up a stream for Kafka."""
        kafka_stream = (self.spark.readStream
                        .format("kafka")
                        .option("kafka.bootstrap.servers", ",".join(self.kafka_config['bootstrap_servers']))
                        .option("kafka.security.protocol", self.kafka_config['security_protocol'])
                        .option("kafka.sasl.mechanism", self.kafka_config['sasl_mechanism'])
                        .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                          f"username=\"{self.kafka_config['username']}\" "
                                                          f"password=\"{self.kafka_config['password']}\";")
                        .option("subscribe", topic)
                        .option("startingOffsets", "earliest")  # The start of reading messages is set
                        .load())

        if schema:
            return kafka_stream.selectExpr("CAST(value AS STRING)") \
                .withColumn("data", from_json(col("value"), schema)) \
                .select(col("data.*"))

        return kafka_stream

    def _write_to_kafka(self, df, topic, checkpoint_suffix):
        """Writing data to Kafka."""
        return (df.writeStream
                .trigger(processingTime='5 seconds')
                #.trigger(continuous="1 second")
                .format("kafka")
                .option("kafka.bootstrap.servers", ",".join(self.kafka_config['bootstrap_servers']))
                .option("kafka.security.protocol", self.kafka_config['security_protocol'])
                .option("kafka.sasl.mechanism", self.kafka_config['sasl_mechanism'])
                .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                  f"username=\"{self.kafka_config['username']}\" "
                                                  f"password=\"{self.kafka_config['password']}\";")
                .option("topic", topic)
                .option("checkpointLocation", f"{self.checkpoint_base}/{checkpoint_suffix}")
                .outputMode("append")
                # .outputMode("update")
                .start())

    def _write_to_console(self, df):
        """Displaying the cleared data on the console."""
        return (df.writeStream
                .trigger(processingTime="1 seconds")
                .outputMode("append")
                .format("console")
                .option("truncate", False)
                .start()
                .awaitTermination())

    def run_processing(self, alerts_file):
        """Basic processing logic."""
        schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True)
        ])

        kafka_stream = self._configure_kafka_stream(self.topic_name_sensors, schema)

        aggregated_df = (kafka_stream
                        .withWatermark("timestamp", "10 seconds")
                        .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
                        .agg(
                            avg("temperature").alias("t_avg"),
                            avg("humidity").alias("h_avg")
                        ))

        # Rounding after calculation
        aggregated_df = aggregated_df.withColumn("t_avg", round(col("t_avg"), 2)) \
                                    .withColumn("h_avg", round(col("h_avg"), 2))

        alerts_schema = StructType([
            StructField("id", StringType(), True),
            StructField("humidity_min", DoubleType(), True),
            StructField("humidity_max", DoubleType(), True),
            StructField("temperature_min", DoubleType(), True),
            StructField("temperature_max", DoubleType(), True),
            StructField("code", StringType(), True),
            StructField("message", StringType(), True)
        ])

        alerts_df = self.spark.read.option("header", True).schema(alerts_schema).csv(alerts_file)

        alerts_with_data = (aggregated_df.crossJoin(alerts_df)
                    .filter(
                        ((col("temperature_min").cast("double") == -999.0) | (col("t_avg") >= col("temperature_min").cast("double"))) &
                        ((col("temperature_max").cast("double") == -999.0) | (col("t_avg") <= col("temperature_max").cast("double"))) &
                        ((col("humidity_min").cast("double") == -999.0) | (col("h_avg") >= col("humidity_min").cast("double"))) &
                        ((col("humidity_max").cast("double") == -999.0) | (col("h_avg") <= col("humidity_max").cast("double")))
                    )
                    .select(
                        struct(
                            col("window.start").alias("start"),
                            col("window.end").alias("end")
                        ).alias("window"),
                        col("t_avg"),
                        col("h_avg"),
                        col("code"),
                        col("message"),
                        expr("current_timestamp()").alias("timestamp")
                    ))
        
        kafka_ready_df = alerts_with_data.select(
            to_json(struct(
                col("window"), col("t_avg"), col("h_avg"), col("code"), col("message"), col("timestamp")
            )).alias("value")
        )
        
        # WARNING: Don't use the kafka_ready_df stream to output to the console and send to Kafka at the same time.
        # Or prepare some kind of feature patch for this =)

        # Output to the console
        #self._write_to_console(kafka_ready_df)

        # Sending to Kafka
        query = self._write_to_kafka(kafka_ready_df, self.topic_name_alerts, "alerts-checkpoints")
        query.awaitTermination()


def main():
    """The main function of starting processing."""
    processor = SparkStreamingProcessor(topic_prefix="oleg")

    alerts_file = "./config/alerts_conditions.csv"

    processor.run_processing(alerts_file)


if __name__ == "__main__":
    main()