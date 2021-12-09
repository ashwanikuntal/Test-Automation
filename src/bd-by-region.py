from pyspark.sql import SparkSession
import collections

# spark = SparkSession.builder.appName('BdByRegion').getOrCreate()
spark = SparkSession.builder.master("local").appName('BdByRegion').enableHiveSupport().getOrCreate()

# spark.conf.set("spark.executor.memory", '8g')
# spark.conf.set('spark.executor.cores', '3')
# spark.conf.set('spark.cores.max', '3')
# spark.conf.set("spark.driver.memory",'8g')
# sc = spark.sparkContext

df = spark.read.format("csv").option("header", "true").load("files/sample.csv")
result = df.count()

print(result)
print("Task Completed")
