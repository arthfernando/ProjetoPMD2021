spark-submit \
    --deploy-mode client \
    --conf spark.pyspark.python=/usr/bin/python3.8 \
    readCsv.py