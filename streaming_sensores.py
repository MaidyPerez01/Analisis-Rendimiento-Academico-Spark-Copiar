from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("StreamingSensores").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "1")

esquema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperatura", DoubleType()),
    StructField("humedad", DoubleType())
])

mensajes = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensores")
    .option("startingOffsets", "earliest")
    .load())

datos = (mensajes
    .select(from_json(col("value").cast("string"), esquema).alias("sensor"))
    .select("sensor.*"))

consulta = (datos.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .start())

consulta.awaitTermination()
