from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifySentimentParquet") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sentiment_path = r"E:\Masters\ProjectBDA\EcommerceMultiStore\data_lake\processed_sentiment_parquet"

df = spark.read.parquet(sentiment_path)

print("[OK] Loaded sentiment-enriched data")
df.printSchema()

df.select("category_code", "brand", "sentiment").show(10, truncate=False)

print("[INFO] Sentiment distribution:")
df.groupBy("sentiment").count().show()

spark.stop()
