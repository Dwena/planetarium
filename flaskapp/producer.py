from kafka import KafkaProducer
import json
import time
from pyspark.sql import SparkSession
import pandas as pd

   
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'topic2'

spark = SparkSession.builder \
	.appName("Read from HDFS") \
	.getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/planetarium/hwc.csv", header=True, inferSchema=True)

#convertir la column P_UPDATE en string
df = df.withColumn("P_UPDATE", df["P_UPDATE"].cast("string"))
df = df.toPandas()
try:
    while True:
        for index, row in df.iterrows():
            message = {col: row[col] for col in df.columns}

            producer.send(topic_name, value=message)
            print(f"Sent: {message}")
            time.sleep(5)
except KeyboardInterrupt:
    print("Stopping producer...")
except Exception as e:
    print(f"An error occured: {e}")

producer.close()



