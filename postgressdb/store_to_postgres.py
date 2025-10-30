from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

jdbc_url = "jdbc:postgresql://postgres:5432/ecommerce_sentiment"
table_name = "sentiment_results"
username = "airflow"
password = "airflow"
jdbc_driver_path = "/opt/airflow/drivers/postgresql-42.7.8.jar"

spark = (
    SparkSession.builder
    .appName("SaveSentimentToPostgres")
    .config("spark.jars", jdbc_driver_path)
    .config("spark.driver.extraClassPath", jdbc_driver_path)
    .config("spark.executor.extraClassPath", jdbc_driver_path)
    .getOrCreate()
)

sentiment_path = "/opt/airflow/data_lake/processed_sentiment_parquet"
df = spark.read.parquet(sentiment_path)
print(f"[INFO] Loaded {df.count():,} records from Parquet.")
df.printSchema()

df_to_save = df.select("category_code", "brand", "text", "sentiment") \
               .withColumn("processed_time", current_timestamp())

df_to_save.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()


print("[OK] Sentiment data successfully stored in PostgreSQL!")

spark.stop()
print("[OK] Spark session stopped successfully.")
