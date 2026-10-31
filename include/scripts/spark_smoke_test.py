from pyspark.sql import SparkSession
spark= (SparkSession.builder.appName("NYC Taxi Spark Test").master("local[2]").getOrCreate())

print("Spark Version: ", spark.version)

df = spark.range(1,11)
print("Number of records",df.count())

df.show()
spark.stop()
