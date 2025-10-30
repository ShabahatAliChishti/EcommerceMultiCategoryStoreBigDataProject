from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyProcessedEcommerceData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

processed_path = r"E:\Masters\ProjectBDA\EcommerceMultiStore\data_lake\processed_parquet"

df_processed = spark.read.parquet(processed_path)

print("[OK] Loaded processed data")
df_processed.printSchema()  
df_processed.show(5, truncate=False)  
print(f"Total records: {df_processed.count():,}")  

print("[OK] Price distribution:")
df_processed.groupBy("price_bucket").count().show()

spark.stop()
