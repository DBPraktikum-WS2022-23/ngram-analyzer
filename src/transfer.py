from src.data_access import NgramDB
from pyspark.sql import SparkSession, DataFrame

class Transferer:
    def __init__(self, spark: SparkSession, db: NgramDB):
        self.__spark = spark
        self.__database = db

    def transfer_textFile(self, source_path: str, schema: str) -> None:
        df = self.__spark.read.textFile(source_path)

        # TODO change to our own schema

        word_df = None # TODO
        occurence_df = None # TODO

        self.__database.write(word_df, "word")
        self.__database.write(occurence_df, "occurence")