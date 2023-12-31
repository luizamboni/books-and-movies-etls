from pyspark.sql import SparkSession

spark = SparkSession \
    .Builder() \
    .appName("Python Spark SQL Hive integration example") \
    .enableHiveSupport() \
    .getOrCreate()


spark.sql("create database if not exists vendor")
spark.sql("create database if not exists stream")
spark.sql("show databases").show()
