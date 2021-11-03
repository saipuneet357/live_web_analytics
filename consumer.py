from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
spark.sparkContext.setLogLevel('error')
df = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092').option('subscribe', 'test-puneet').option('startingOffsets', 'latest').load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format('console').start().awaitTermination()
