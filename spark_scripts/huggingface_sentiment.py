from pyspark.sql import SparkSession
from transformers import pipeline
import pandas as pd
import torch

spark = (
    SparkSession.builder
    .appName("FastEcommerceSentimentAnalysis")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.driver.memory", "12g")
    .config("spark.executor.memory", "12g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)


processed_path = "/opt/airflow/data_lake/processed_parquet"

df = spark.read.parquet(processed_path).select("category_code", "brand")
df = df.filter(df.category_code.isNotNull()).dropDuplicates(["category_code", "brand"])

row_count = df.count()
print(f"[OK] Loaded {row_count:,} unique rows for sentiment analysis")


print("[INFO] Converting to Pandas for sentiment processing...")
pdf = df.toPandas()

# Clean text and combine category_code + brand
pdf["category_code"] = pdf["category_code"].astype(str).str.strip()
pdf["brand"] = pdf["brand"].astype(str).str.strip()

pdf["category_code"].replace("", "unknown", inplace=True)
pdf["brand"].replace("", "unknown", inplace=True)

pdf["text"] = (pdf["category_code"] + " " + pdf["brand"]).str[:512]  

print("[INFO] Loading sentiment model (DistilBERT)...")
device = 0 if torch.cuda.is_available() else -1
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english",
    device=device,
)

batch_size = 64
texts = pdf["text"].tolist()
results = []

print(f"[INFO] Running sentiment inference on {len(texts):,} texts...")

for i in range(0, len(texts), batch_size):
    batch = texts[i:i + batch_size]
    preds = sentiment_pipeline(batch)
    results.extend([p["label"].lower() for p in preds])

pdf["sentiment"] = results
print("[OK] Sentiment analysis completed.")

result_df = spark.createDataFrame(pdf)

sentiment_path = "/opt/airflow/data_lake/processed_sentiment_parquet"
csv_path = "/opt/airflow/data_lake/processed_sentiment_csv"

result_df.coalesce(4).write.mode("overwrite").parquet(sentiment_path)
print("[OK] Sentiment-enriched data saved to:", sentiment_path)
# i have optionally save it to csv for reading data
result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path)
print("[OK] CSV copy saved to:", csv_path)


spark.stop()
print("[OK] Job completed successfully.")
