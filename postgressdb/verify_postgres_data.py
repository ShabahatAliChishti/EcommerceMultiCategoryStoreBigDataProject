from pyspark.sql import SparkSession

jdbc_url = "jdbc:postgresql://localhost:5432/ecommerce_sentiment"
table_name = "sentiment_results"
username = "postgres"
password = "Sysltd@2022"

jdbc_driver_path = r"C:\postgres_driver\postgresql-42.7.8.jar"

spark = (
    SparkSession.builder
    .appName("VerifyPostgresData")
    .config("spark.jars", jdbc_driver_path)
    .config("spark.driver.extraClassPath", jdbc_driver_path)
    .config("spark.executor.extraClassPath", jdbc_driver_path)
    .getOrCreate()
)


print("[INFO] Reading data from PostgreSQL table:", table_name)

df_pg = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", table_name)
    .option("user", username)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

record_count = df_pg.count()
print(f"[ok] Successfully loaded {record_count:,} records from PostgreSQL.")

print("\n[INFO] Schema:")
df_pg.printSchema()

print("\n[INFO] Sample rows:")
df_pg.show(10, truncate=False)


spark.stop()
print("[ok] Spark session stopped successfully.")
