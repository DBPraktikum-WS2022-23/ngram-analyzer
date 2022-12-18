from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, split, regexp_extract


class Transferer:
    def __init__(self, spark: SparkSession, db_url: str, properties: Dict[str, str]):
        self.__spark = spark
        self.__db_url = db_url
        self.__properties = properties

    def __write(self, df: DataFrame, table: str) -> None:
        """Writes the given DataFrame to the given table by using DataFrame."""
        df.write.jdbc(self.__db_url, table, mode="append", properties=self.__properties)

    def __read(self, table: str) -> DataFrame:
        """Reads the given table and returns it as a DataFrame."""
        return self.__spark.read.jdbc(
            self.__db_url, table, properties=self.__properties
        )

    def transfer_textFile(self, source_path: str) -> None:
        # regex expression of word[_type]
        regexp = r"^(.*?)(_|_ADJ|_ADP|_ADV|_CONJ|_DET|_NOUN|_PRON|_VERB|_PRT|_X)?$"

        # split data into word, type and occurence
        df = (
            self.__spark.read.text(source_path)
            .withColumn("word_and_type", split(col("value"), "\t", 2)[0])
            .withColumn("occ_all", split(col("value"), "\t", 2)[1])
            .drop("value")
            .withColumn("str_rep", regexp_extract(col("word_and_type"), regexp, 1))
            .withColumn("type", split(regexp_extract(col("word_and_type"), regexp, 2), "_")[1])
            .drop("word_and_type")
        )

        word_df = df.select("str_rep", "type").distinct()

        # delete duplicates in word table
        word_df_db = self.__read("word")
        word_df = word_df.join(
            word_df_db.select("str_rep", "type"),
            [
                word_df_db["str_rep"] == word_df["str_rep"],
                word_df_db["type"].eqNullSafe(word_df["type"]),
            ],
            "left_anti"
        )
        temp = word_df.cache().select(col("str_rep").alias("temp_str"), col("type").alias("temp_type")).alias("temp")

        word_df.show()
        self.__write(word_df, "word")

        df = df.join(
            temp,
            [
                df["str_rep"] == temp["temp_str"],
                df["type"].eqNullSafe(temp["temp_type"]),
            ]
        ).select("str_rep", "type", "occ_all")

        word_df_new = self.__read("word")
        occurence_df = (
            df.join(
                word_df_new,
                [
                    word_df_new["str_rep"] == df["str_rep"],
                    word_df_new["type"].eqNullSafe(df["type"])
                ]
            ).select("id", "occ_all")
            .withColumn("occ_sep", split(col("occ_all"), "\t"))
            .drop("occ_all")
            .select("id", explode("occ_sep").alias("occurence"))
            .withColumn("year", split(col("occurence"), ",")[0].cast("int"))
            .withColumn("freq", split(col("occurence"), ",")[1].cast("int"))
            .withColumn("book_count", split(col("occurence"), ",")[2].cast("int"))
            .drop("occurence")
            .dropDuplicates(["id", "year"])
        )

        occurence_df.show()
        self.__write(occurence_df, "occurence")

        """
        occurence_df = (
            df.withColumn("occ_sep", split(col("occ_all"), "\t"))
            .drop("occ_all")
            .select("str_rep","type", explode("occ_sep").alias("occurence"))
            .withColumn("year", split(col("occurence"), ",")[0].cast("int"))
            .withColumn("freq", split(col("occurence"), ",")[1].cast("int"))
            .withColumn("book_count", split(col("occurence"), ",")[2].cast("int"))
            .drop("occurence")
            .dropDuplicates(["str_rep", "type", "year"])
        )

        # add foreign key to occurence table

        occurence_df = occurence_df.join(
            word_df,
            [
                word_df["str_rep"] == occurence_df["str_rep"],
                word_df["type"].eqNullSafe(occurence_df["type"])
            ]
        ).select("id", "year", "freq", "book_count")


        # show occurence table that will be written to db
        occurence_df.show()

        self.__write(occurence_df, "occurence")
        """

