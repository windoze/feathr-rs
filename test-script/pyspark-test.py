from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
sc = spark.sparkContext

logData = spark.read.text("/abc").cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
