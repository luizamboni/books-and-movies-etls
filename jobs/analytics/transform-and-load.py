from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from typing import List, NamedTuple, cast
import argparse
import sys

class Args(NamedTuple):
    origin_path_or_url: str
    destin_path: str
    job_name: str
    transform_expressions: List[str]
    primary_keys: List[str]
    overwrite: bool

def ger_cli_args(args: List[str]) -> Args:
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-path-or-url", required=True, type=str)
    parser.add_argument("--destin-path", required=True, type=str)
    parser.add_argument("--job-name", required=True, type=str)
    parser.add_argument("--transform-expressions", "-t", action="append", default=["*"], type=str)
    parser.add_argument("--primary-keys", '-pk', action="append", required=True, type=str)
    parser.add_argument("--overwrite", action="store", default=False, type=bool)

    return cast(Args, parser.parse_args(args))


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

if __name__ == "__main__":

    args = ger_cli_args(sys.argv[1:])
    
    origin_path_or_url = args.origin_path_or_url
    destin_path = args.destin_path
    job_name = args.job_name
    transform_expressions = args.transform_expressions
    primary_keys = args.primary_keys
    overwrite = args.overwrite

    spark = (
        SparkSession
        .Builder()
        .appName(job_name)
        .getOrCreate()
    )

    incomming_df = spark.read.json(origin_path_or_url, multiLine=True)

    transformed_df = transform(incomming_df, transform_expressions)

    if overwrite:
        overwrite_table(transformed_df, destin_path)
    
    elif table_exists(spark, destin_path):
        merge_table(spark, transformed_df, destin_path, primary_keys)
    
    else:
        overwrite_table(transformed_df, destin_path)


