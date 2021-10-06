import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("readCsv") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Projeto.netflix?authSource=admin") \
    .config("spark.jars.package", "org.mongodb.spark:mongo-spark-connector_2.12-3.0.1,org.mongodb:mongo-java-driver:3.12.10") \
    .getOrCreate()

df = spark.read.load("netflix/netflix_titles.csv", format="csv", sep=",", inferSchema="true", header="true")

df2 = df.select("title", "director")

df2.show()

df2.printSchema()

df2.write \
.format("mongo") \
.mode("append") \
.save()