""" Module which transfers data from a file to the database """
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, regexp_extract, split


class Transferer:
    def __init__(self, spark: SparkSession, db_url: str, properties: Dict[str, str]):
        self.__spark = spark
        self.__db_url = db_url
        self.__properties = properties

    def __write(self, df: DataFrame, table: str) -> None:
        """Writes the given DataFrame to the given table by using DataFrame."""
        try:
            df.write.jdbc(
                self.__db_url, table, mode="append", properties=self.__properties
            )
        except:
            print(
                f"Writing to {table} failed. \n"
                f"Have you already transfered this data before?"
            )

    def __read(self, table: str) -> DataFrame:
        """Reads the given table and returns it as a DataFrame."""
        return self.__spark.read.jdbc(
            self.__db_url, table, properties=self.__properties
        )

    def transfer_textFile(self, source_path: str) -> None:
        """Transfers the data from the given text file to the database."""
        # regex expression of word[_type]
        regexp = r"^(.*?)(_|_ADJ|_ADP|_ADV|_CONJ|_DET|_NOUN|_PRON|_VERB|_PRT|_X)?$"

        # split data into word, type and occurence
        dataframe = (
            self.__spark.read.text(source_path)
            .withColumn("word_and_type", split(col("value"), "\t", 2)[0])
            .withColumn("occ_all", split(col("value"), "\t", 2)[1])
            .drop("value")
            .withColumn("str_rep", regexp_extract(col("word_and_type"), regexp, 1))
            .withColumn(
                "type", split(regexp_extract(col("word_and_type"), regexp, 2), "_")[1]
            )
            .drop("word_and_type")
        )

        word_df = dataframe.select("str_rep", "type").distinct()
        self.__write(word_df, "word")

        word_df_db = self.__read("word")

        # split occurence to a list
        occurence_df = dataframe.withColumn(
            "occ_sep", split(col("occ_all"), "\t")
        ).select("str_rep", "type", "occ_sep")

        # add foreign key to occurence table
        occurence_df = occurence_df.join(
            word_df_db,
            [
                word_df_db["str_rep"] == occurence_df["str_rep"],
                word_df_db["type"].eqNullSafe(occurence_df["type"]),
            ],
        ).select("id", "occ_sep")

        occurence_df = (
            occurence_df.select("id", explode("occ_sep").alias("occurence"))
            .withColumn("year", split(col("occurence"), ",")[0].cast("int"))
            .withColumn("freq", split(col("occurence"), ",")[1].cast("int"))
            .withColumn("book_count", split(col("occurence"), ",")[2].cast("int"))
            .drop("occurence")
        )

        self.__write(occurence_df, "occurence")
        print("Finished transfer.")
