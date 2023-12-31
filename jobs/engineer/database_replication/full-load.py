from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
import argparse
import sys

def ger_cli_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-path", required=True, type=str)
    parser.add_argument("--destin-path", required=True, type=str)
    parser.add_argument("--table-name", required=True, type=str)
    return parser.parse_args(args)


def create_table_from_dataframe(df: DataFrame, table_name: str, location: str):
    DeltaTable.createOrReplace(spark) \
        .location(location) \
        .tableName(table_name) \
        .addColumns(df.schema) \
        .execute()

if __name__ == "__main__":

    args = ger_cli_args(sys.argv[1:])
    origin_path = args.origin_path
    destin_path = args.destin_path
    table_name = args.table_name

    spark = (
        SparkSession
        .Builder()
        .appName(f"{table_name}_replication")
        .enableHiveSupport() \
        .getOrCreate()
    )

    df = spark.read.option("header", True).csv(origin_path)

    df.write \
        .format("delta") \
        .mode('overwrite') \
        .option("overwriteSchema", "true") \
        .save(destin_path)
    
    create_table_from_dataframe(df, table_name, destin_path)