import os

from awsglue.context import GlueContext
from pyspark.sql import SparkSession


def count_csv_file_rows():
    # Create Spark session with Glue JARs included
    jars_path = os.path.join(os.getcwd(), "jars", "*")
    spark = SparkSession \
        .builder \
        .appName("MSSQL to CSV") \
        .config("spark.driver.extraClassPath", jars_path) \
        .config("spark.executor.extraClassPath", jars_path) \
        .getOrCreate()

    sc = spark.sparkContext
    glueContext = GlueContext(sc)

    df = spark.read.format("csv").option("header", "true").load("../files/sample.csv")
    result = df.count()
    return result


res = count_csv_file_rows()
print(f'result is : {res}')
print('Task Completed')