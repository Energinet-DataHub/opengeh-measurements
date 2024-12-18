from pyspark.sql import SparkSession


class Reader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, table_name: str):
        return self.spark.read.table(table_name)

def _write(df, table_name: str):
    df.write.saveAsTable(table_name)

def execute(spark: SparkSession):
    _execute(spark)

def _execute(spark: SparkSession, reader = Provide[container.Reader]):
    df = reader.read(spark, "input_table")
    _write(df, "output_table_1")
    _write(df, "output_table_2")
