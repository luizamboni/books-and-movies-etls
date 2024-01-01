from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from typing import List, NamedTuple, cast
import argparse
import sys

class Args(NamedTuple):
    origin_path_or_url: str
    destin_path: str
    table_name: str
    transform_expressions: List[str]
    primary_keys: List[str]
    overwrite: bool

def ger_cli_args(args: List[str]) -> Args:
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-path-or-url", required=True, type=str)
    parser.add_argument("--destin-path", required=True, type=str)
    parser.add_argument("--table-name", required=True, type=str)
    parser.add_argument("--transform-expressions", "-t", action="append", default=[], type=str)
    parser.add_argument("--primary-keys", '-pk', action="append", required=True, type=str)
    parser.add_argument("--overwrite", action="store_true", default=False)
    parsed_args = parser.parse_args(args)
    if not parsed_args.transform_expressions:
        parsed_args.transform_expressions = ["*"]
    return cast(Args, parsed_args)


def transform(df: DataFrame, transformations: List[str]) -> DataFrame:
    return df.selectExpr(*transformations)

def table_exists(spark: SparkSession, destin_path: str):
    return DeltaTable.isDeltaTable(spark, destin_path)

def merge_table(spark: SparkSession, df: DataFrame, destin_path: str, primary_keys: List[str]):
    delta_table = DeltaTable.forPath(spark, destin_path)
    delta_table.alias('table') \
    .merge(
        df.alias('updates'), 
        " AND ".join([ 
            f"table.{key} = updates.{key}" for key in primary_keys 
        ])
    ) \
    .whenNotMatchedInsertAll() \
    .whenMatchedUpdateAll() \
    .execute()

def overwrite_table(df: DataFrame, destin_path: str):
    df.write \
        .format("delta") \
        .mode('overwrite') \
        .option("overwriteSchema", "true") \
        .save(destin_path)

def create_table_from_dataframe(df: DataFrame, table_name: str, location: str):
    # just for exam schema
    df.printSchema()
    
    DeltaTable.createOrReplace(spark) \
        .location(location) \
        .tableName(table_name) \
        .addColumns(df.schema) \
        .execute()


def load_data(spark: SparkSession, path: str) -> DataFrame:
    if path.endswith(".json"):
        return spark.read.json(path, multiLine=True)
    elif path.endswith(".csv"):
        return spark.read.option("header", True).csv(path)
    else:
        raise Exception("unknown format")


if __name__ == "__main__":

    args = ger_cli_args(sys.argv[1:])
    
    origin_path_or_url = args.origin_path_or_url
    destin_path = args.destin_path
    table_name = args.table_name
    transform_expressions = args.transform_expressions
    primary_keys = args.primary_keys
    overwrite = args.overwrite

    spark = (
        SparkSession
        .Builder()
        .appName(f"{table_name}-transform-and-load")
        .enableHiveSupport()
        .getOrCreate()
    )

    incomming_df = load_data(spark, origin_path_or_url)

    transformed_df = transform(incomming_df, transform_expressions)

    # just for exam a sample
    transformed_df.show(truncate=False)

    if overwrite:
        overwrite_table(transformed_df, destin_path)
        create_table_from_dataframe(transformed_df, table_name, destin_path)

    elif table_exists(spark, destin_path):
        merge_table(spark, transformed_df, destin_path, primary_keys)
    
    else:
        overwrite_table(transformed_df, destin_path)
        create_table_from_dataframe(transformed_df, table_name, destin_path)
