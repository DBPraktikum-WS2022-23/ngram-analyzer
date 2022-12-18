from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, split


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
        # split data into word, type and occurence
        df = (
            self.__spark.read.text(source_path)
            .withColumn("word_and_type", split(col("value"), "\t", 2)[0])
            .withColumn("occ_all", split(col("value"), "\t", 2)[1])
            .drop("value")
            .withColumn("str_rep", split(col("word_and_type"), "_")[0])
            .withColumn("type", split(col("word_and_type"), "_")[1])
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
            "left_anti",
        )

        # show word table that will be written to db
        word_df.show()
        self.__write(word_df, "word")

        occurence_df = (
            df.select("str_rep", "occ_all")
            .withColumn("occ_sep", split(col("occ_all"), "\t"))
            .drop("occ_all")
            .select("str_rep", explode("occ_sep").alias("occurence"))
            .withColumn("year", split(col("occurence"), ",")[0].cast("int"))
            .withColumn("freq", split(col("occurence"), ",")[1].cast("int"))
            .withColumn("book_count", split(col("occurence"), ",")[2].cast("int"))
            .drop("occurence")
        )

        # add foreign key to occurence table
        occurence_df = occurence_df.join(
            word_df_db.select("str_rep", "id"), "str_rep"
        ).select("id", "year", "freq", "book_count")

        # delete duplicates in occurence table
        occ_df_db = self.__read("occurence")
        occurence_df = occurence_df.join(
            occ_df_db.select("id", "year"), ["id", "year"], "left_anti"
        )

        # show occurence table that will be written to db
        occurence_df.show()
        self.__write(occurence_df, "occurence")
