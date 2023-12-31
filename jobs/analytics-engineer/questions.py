from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
import argparse
import sys

def ger_cli_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-path", required=True, type=str)
    parser.add_argument("--destin-path", required=True, type=str)
    parser.add_argument("--table-name", required=True, type=str)
    return parser.parse_args(args)


def load_query(path_or_url: str) -> str:
    return open(path_or_url).read()


def create_table_from_dataframe(spark: SparkSession, table_name: str, location: str):

    persisted_schema = DeltaTable.forPath(spark, location).toDF().schema
    DeltaTable.createOrReplace(spark) \
        .location(location) \
        .tableName(table_name) \
        .addColumns(persisted_schema) \
        .execute()
    

if __name__ == "__main__":
    args = ger_cli_args(sys.argv[1:])
    
    query_path = args.query_path
    destin_path = args.destin_path
    table_name = args.table_name

    spark = (
        SparkSession
        .Builder()
        .appName(f"{table_name}_question")
        .enableHiveSupport() \
        .getOrCreate()
    )

    sql_query = load_query(query_path)

    df = spark.sql(sql_query)

    df.printSchema()

    df.write \
        .format("delta") \
        .mode('overwrite') \
        .option("overwriteSchema", "true") \
        .save(destin_path)

    create_table_from_dataframe(spark, table_name, destin_path)