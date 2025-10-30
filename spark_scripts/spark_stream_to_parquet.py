from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToParquetWithCount") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "200000") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True)
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce.events") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 500000) \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) AS json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

parsed_df = parsed_df.withColumn("event_time", to_timestamp("event_time"))

output_dir = r"E:\Masters\ProjectBDA\EcommerceMultiStore\data_lake\raw_parquet"
checkpoint_dir = r"E:\Masters\ProjectBDA\EcommerceMultiStore\checkpoints\streaming"

total_count = {"value": 0}

def process_batch(batch_df, batch_id):
    count = batch_df.count()
    total_count["value"] += count
    print(f"Batch {batch_id}: {count} records processed (Total so far: {total_count['value']})")

    batch_df.write.mode("append").parquet(output_dir)

query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="30 seconds") \
    .start()

print("Kafka to Parquet streaming started.")
print("Output directory:", output_dir)

query.awaitTermination()
