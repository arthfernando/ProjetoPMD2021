import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Cria sessao do spark
spark = SparkSession.builder \
    .master("local") \
    .appName("readCsv") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Projeto.netflix?authSource=admin") \
    .config("spark.jars.package", "org.mongodb.spark:mongo-spark-connector_2.12-3.0.1,org.mongodb:mongo-java-driver:3.12.10") \
    .getOrCreate()

# le o arquivo csv
# df = spark.read.load("netflix/netflix_titles2.csv", format="csv", sep=",", inferSchema="true", header="true")
df = spark.read.options(header='True', inferSchema='True', delimiter=',', quote='\"', escape="\"").csv("netflix/netflix_titles.csv")

# substitui o valor null para NAO_INFORMADO
df2 = df.na.fill("NAO_INFORMADO")

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


# df3.select("rating").distinct().show()

# df2 = df.createOrReplaceTempView("TABLE")
# spark.sql("SELECT * FROM TABLE where rating = 'Benn Northover'").show()



# df2.show()

# df2.printSchema()

# Salva o DataFrame no MongoDB
df3.write \
.format("mongo") \
.mode("append") \
.save()