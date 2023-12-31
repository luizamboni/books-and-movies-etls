from pyspark.sql import SparkSession
from os.path import abspath

warehouse_location = abspath('data/metastore')

spark = SparkSession \
    .Builder() \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


spark.sql("create database if not exists vendor")
spark.sql("create database if not exists stream")
spark.sql("show databases").show()
