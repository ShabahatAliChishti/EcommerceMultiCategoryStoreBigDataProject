import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, lower, hour, dayofweek, avg
from pyspark.sql.types import DoubleType, LongType, StringType

spark = SparkSession.builder \
    .appName("DataProcessingEcommerce") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

raw_path = r"/opt/airflow/data_lake/raw_parquet"
parquet_files = [os.path.join(raw_path, f)
                 for f in os.listdir(raw_path)
                 if f.endswith(".parquet")]

df = spark.read.parquet(*parquet_files)

df_clean = df.withColumn("product_id", col("product_id").cast(LongType())) \
             .withColumn("category_id", col("category_id").cast(StringType())) \
             .withColumn("user_id", col("user_id").cast(LongType())) \
             .withColumn("price", col("price").cast(DoubleType()))

avg_price_row = df_clean.select(avg(col("price"))).first()
avg_price = avg_price_row[0] if avg_price_row[0] is not None else 0.0
df_clean = df_clean.fillna({"price": avg_price})

df_clean = df_clean.fillna({"brand": "unknown", "category_code": "unknown"})

for c in ["event_type", "category_code", "brand"]:
    df_clean = df_clean.withColumn(c, lower(regexp_replace(col(c), " ", "_")))

df_features = df_clean.withColumn("hour", hour(col("event_time"))) \
                      .withColumn("day_of_week", dayofweek(col("event_time"))) \
                      .withColumn("price_bucket",
                                  when(col("price") < 50, "low")
                                  .when((col("price") >= 50) & (col("price") < 200), "medium")
                                  .otherwise("high"))

processed_path = r"/opt/airflow/data_lake/processed_parquet"
df_features.write.mode("overwrite").parquet(processed_path)

spark.stop()
