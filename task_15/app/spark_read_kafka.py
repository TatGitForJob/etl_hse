import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "etl_practice_events"


def main() -> None:
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)

    spark = (
        SparkSession.builder.appName("local-kafka-pyspark-homework")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("event_id", IntegerType(), nullable=False),
            StructField("student", StringType(), nullable=False),
            StructField("course", StringType(), nullable=False),
            StructField("score", IntegerType(), nullable=False),
            StructField("event_time", StringType(), nullable=False),
        ]
    )

    raw_events = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    parsed_events = raw_events.select(
        col("key").cast("string").alias("kafka_key"),
        from_json(col("value").cast("string"), schema).alias("event"),
    ).select("kafka_key", "event.*")

    print("Messages read from Kafka by PySpark:")
    parsed_events.orderBy("event_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
