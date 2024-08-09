from planets import use_planets
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import from_json, col
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .appName("Planetarium") \
    .config("spark.master", "local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

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

model_path = "bestmodel/"
model = DecisionTreeClassificationModel.load(model_path)

def prediction_model(input_data,pre_fitted_pipeline1,model1):
    print("Model loaded successfully")

    # Utilisation de la fonction use_planets pour prétraiter les données
    preprocessed_data = use_planets(input_data, pre_fitted_pipeline1)
    print("Data preprocessed successfully")
    
    # Identification des colonnes de type string
    string_cols = [field.name for field in preprocessed_data.schema.fields if isinstance(field.dataType, StringType)]
    print("String columns identified successfully")
    
    # Transformation des colonnes de type string en valeurs numériques avec StringIndexer
    for col_name in string_cols:
        indexed_col_name = col_name + "_indexed"
        if indexed_col_name not in preprocessed_data.columns:
            indexer = StringIndexer(inputCol=col_name, outputCol=indexed_col_name).fit(preprocessed_data)
            preprocessed_data = indexer.transform(preprocessed_data).drop(col_name).withColumnRenamed(indexed_col_name, col_name)
    print("String columns indexed successfully")
        
    # Assemblage des features
    feature_columns = [col for col in preprocessed_data.columns if col != "label"] 
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="selectedFeatures")
    assembled_data = assembler.transform(preprocessed_data)
    print("Features assembled successfully")

    # Génération des prédictions
    predictions = model1.transform(assembled_data)
    return predictions


static_df = spark.read.csv("data/hwc.csv", header=True, inferSchema=True)
categorical_columns = [col_name for col_name, dtype in static_df.dtypes if dtype == 'string']
indexers = [StringIndexer(inputCol=column, outputCol=column + "_indexed").setHandleInvalid("keep") for column in categorical_columns]
pipeline = Pipeline(stages=indexers)
pre_fitted_pipeline = pipeline.fit(static_df)



df.foreach(lambda batch_df, batch_id: prediction_model(batch_df,pre_fitted_pipeline) \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()) \
    .awaitTermination()
