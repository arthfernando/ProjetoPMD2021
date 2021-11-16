import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def formatDate(x):
    splitcol = split(x["date_added"], "-")
    mes = from_unixtime(unix_timestamp(splitcol.getItem(0), "MMMM"), "MM")
    dia = splitcol.getItem(1)
    ano = splitcol.getItem(2)
    return (concat_ws("/", dia,mes,ano))

# Cria sessao do spark
spark = SparkSession.builder \
    .master("local") \
    .appName("readCsv") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Projeto.netflix?authSource=admin") \
    .config("spark.jars.package", "org.mongodb.spark:mongo-spark-connector_2.12-3.0.1,org.mongodb:mongo-java-driver:3.12.10") \
    .getOrCreate()

# le o arquivo csv
df = spark.read.options(header='True', inferSchema='True', delimiter=',', quote='\"', escape="\"").csv("netflix/netflix_titles.csv")

# substitui o valor null para NAO_INFORMADO
aux = df.na.fill("NAO_INFORMADO")
df2 = aux.select(\
    col("show_id"),\
    col("type"),\
    col("title"),\
    col("director"),\
    split(col("cast"), ",").alias("cast"),\
    split(col("country"), ",").alias("country"),\
    col("date_added"),\
    col("release_year"),\
    col("rating"),\
    col("duration"),\
    split(col("listed_in"), ",").alias("listed_in"),\
    col("description")
)
df2.show()

# altera classificao indicativa para o padrao brasileiro
df3 = df2.withColumn("rating", when(df2.rating == "G", "L") \
    .when(df2.rating == "UR", "L") \
    .when(df2.rating == "TV-Y", "L") \
    .when(df2.rating == "TV-G", "L") \
    .when(df2.rating == "TV-Y7", "10") \
    .when(df2.rating == "TV-Y7-FV", "10") \
    .when(df2.rating == "PG", "10") \
    .when(df2.rating == "PG-13", "12") \
    .when(df2.rating == "TV-PG", "14") \
    .when(df2.rating == "TV-14", "16") \
    .when(df2.rating == "R", "18") \
    .when(df2.rating == "NC-17", "18") \
    .when(df2.rating == "NR", "18") \
    .when(df2.rating == "TV-MA", "18") \
    .otherwise(df2.rating))

# formata a data para MMMM-dd-yyyy
df4 = df3.withColumn("date_added", regexp_replace(df3.date_added, "\s|(,\s)", "-"))

# formata a data para dd/MM/yyyy
df5 = df4.withColumn("date_added", formatDate(df4))


# Salva o DataFrame no MongoDB
df5.write \
.format("mongo") \
.mode("append") \
.save()
