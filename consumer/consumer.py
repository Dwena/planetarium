from planets import use_planets
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import from_json, col
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder \
    .appName("Planetarium") \
    .config("spark.master", "local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()
model_path = "bestmodel/"
model = DecisionTreeClassificationModel.load(model_path)

schema = StructType([
    StructField("P_NAME", StringType(), True),
    StructField("P_DETECTION", StringType(), True),
    StructField("P_DISCOVERY_FACILITY", StringType(), True),
    StructField("P_YEAR", IntegerType(), True),
    StructField("P_UPDATE", DateType(), True),
    StructField("P_MASS", DoubleType(), True),
    StructField("P_MASS_ERROR_MIN", DoubleType(), True),
    StructField("P_MASS_ERROR_MAX", DoubleType(), True),
    StructField("P_MASS_LIMIT", IntegerType(), True),
    StructField("P_MASS_ORIGIN", StringType(), True),
    StructField("P_RADIUS", DoubleType(), True),
    StructField("P_RADIUS_ERROR_MIN", DoubleType(), True),
    StructField("P_RADIUS_ERROR_MAX", DoubleType(), True),
    StructField("P_RADIUS_LIMIT", IntegerType(), True),
    StructField("P_PERIOD", DoubleType(), True),
    StructField("P_PERIOD_ERROR_MIN", DoubleType(), True),
    StructField("P_PERIOD_ERROR_MAX", DoubleType(), True),
    StructField("P_PERIOD_LIMIT", IntegerType(), True),
    StructField("P_SEMI_MAJOR_AXIS", DoubleType(), True),
    StructField("P_SEMI_MAJOR_AXIS_ERROR_MIN", DoubleType(), True),
    StructField("P_SEMI_MAJOR_AXIS_ERROR_MAX", DoubleType(), True),
    StructField("P_SEMI_MAJOR_AXIS_LIMIT", IntegerType(), True),
    StructField("P_ECCENTRICITY", DoubleType(), True),
    StructField("P_INCLINATION", DoubleType(), True),
    StructField("P_INCLINATION_ERROR_MIN", DoubleType(), True),
    StructField("P_INCLINATION_ERROR_MAX", DoubleType(), True),
    StructField("P_INCLINATION_LIMIT", IntegerType(), True),
    StructField("S_NAME", StringType(), True),
    StructField("S_TYPE", StringType(), True),
    StructField("S_RA", DoubleType(), True),
    StructField("S_DEC", DoubleType(), True),
    StructField("S_MAG", DoubleType(), True),
    StructField("S_DISTANCE", DoubleType(), True),
    StructField("S_TEMPERATURE", DoubleType(), True),
    StructField("S_MASS", DoubleType(), True),
    StructField("S_RADIUS", DoubleType(), True),
    StructField("S_METALLICITY", DoubleType(), True),
    StructField("S_AGE", DoubleType(), True),
    StructField("S_LOG_LUM", DoubleType(), True),
    StructField("S_HZ_OPT_MAX", DoubleType(), True),
    StructField("S_HZ_CON_MIN", DoubleType(), True),
    StructField("S_HZ_CON_MAX", DoubleType(), True),
    StructField("S_SNOW_LINE", DoubleType(), True),
    StructField("S_ABIO_ZONE", DoubleType(), True),
    StructField("S_TIDAL_LOCK", DoubleType(), True),
    StructField("P_HABZONE_OPT", IntegerType(), True),
    StructField("P_HABZONE_CON", IntegerType(), True),
    StructField("P_TYPE_TEMP", StringType(), True),
    StructField("P_HABITABLE", IntegerType(), True),
    StructField("P_ESI", DoubleType(), True),
    StructField("S_CONSTELLATION", StringType(), True),
    StructField("S_CONSTELLATION_ABR", StringType(), True),
    StructField("S_CONSTELLATION_ENG", StringType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topic2") \
    .load()



df = df.selectExpr("CAST(value AS STRING) as json")
df = df.select(from_json(col("json"), schema).alias("data")).select("data.*")

def prediction_model(input_data):

    preprocessed_data = use_planets(input_data)
    all_columns = [col for col in preprocessed_data.columns]
    assembler = VectorAssembler(inputCols=all_columns, outputCol="selectedFeatures")
    assembled_data = assembler.transform(preprocessed_data)
    predictions = model.transform(assembled_data)
    print("ALLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")
    return predictions

def process_batch(batch_df):
    predictions = prediction_model(batch_df)
    predictions.show()


df.writeStream \
  .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df)) \
  .start() \
  .awaitTermination()

