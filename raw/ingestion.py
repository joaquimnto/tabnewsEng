# Databricks notebook source
import requests

url = 'https://www.tabnews.com.br/api/v1/contents'

resp = requests.get(url)
data = resp.json()

# COMMAND ----------

class Ingestor:
    def __init__(self, url, per_page, strategy, stop_date) -> None:
        self.url = url
        self.params = {
            'per_page': per_page,
            'strategy': strategy
        }
        self.stop_date = stop_date

    def get_response(self, **params):
        return requests.get(self.url, params=params)

    def get_data(self, **params):
        return self.get_response(**params).json()

myIngestor = Ingestor(
    url='https://www.tabnews.com.br/api/v1/contents', 
    per_page=100, 
    strategy='new', 
    stop_date='2023-05-01')
data = myIngestor.get_data(**myIngestor.params)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import from_utc_timestamp, to_timestamp

schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("owner_id", StringType(), nullable=False),
    StructField("parent_id", StringType(), nullable=True),
    StructField("slug", StringType(), nullable=False),
    StructField("title", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("source_url", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=False),
    StructField("updated_at", StringType(), nullable=False),
    StructField("published_at", StringType(), nullable=False),
    StructField("deleted_at", StringType(), nullable=True),
    StructField("owner_username", StringType(), nullable=False),
    StructField("tabcoins", IntegerType(), nullable=False),
    StructField("children_deep_count", IntegerType(), nullable=False)
])

df = spark.createDataFrame(data, schema=schema)

df = df.withColumn("created_at", to_timestamp(df["created_at"], "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
df = df.withColumn("updated_at", to_timestamp(df["updated_at"], "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
df = df.withColumn("published_at", to_timestamp(df["published_at"], "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

# COMMAND ----------

df.write.mode("append").saveAsTable("rawTabNewsTable")

# COMMAND ----------

df.write.mode("append").parquet('/mnt/tabnewsdata/rawdata')
