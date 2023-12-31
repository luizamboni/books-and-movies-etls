from pyspark.sql import SparkSession
import argparse
import sys

def ger_cli_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-path", required=True, type=str)
    parser.add_argument("--destin-path", required=True, type=str)
    parser.add_argument("--job-name", required=True, type=str)
    return parser.parse_args(args)


if __name__ == "__main__":

    args = ger_cli_args(sys.argv[1:])
    origin_path = args.origin_path
    destin_path = args.destin_path
    job_name = args.job_name

    spark = (
        SparkSession
        .Builder()
        .appName(f"{job_name}_replication")
        .getOrCreate()
    )

    df = spark.read.option("header", True).csv(origin_path)


    df.write \
        .format("delta") \
        .mode('overwrite') \
        .option("overwriteSchema", "true") \
        .partitionBy([]) \
        .save(destin_path)