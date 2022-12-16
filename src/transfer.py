from pyspark.sql import SparkSession, DataFrame
from  pyspark.sql.functions import split, col, explode

class Transferer:
    def __init__(self, spark: SparkSession, db_url: str, properties: str):
        self.__spark = spark
        self.__db_url = db_url
        self.__properties = properties

    def __write(self, df: DataFrame, table: str) -> None:
        """ Writes the given DataFrame to the given table by using DataFrame. """
        df.write.jdbc(self.__db_url, table, mode="append", properties=self.__properties)

    def transfer_textFile(self, source_path: str) -> None:
        # split data into word and occurence and make cartesian product on them
        df = self.__spark.read.text(source_path) \
            .withColumn("word_and_type", split(col("value"), "\t")[0]) \
            .withColumn("occurence", split(col("value"), "\t")[1:]) \
            .drop("value") \
            .select("word_and_type", explode("occurence").alias("occurence"))

        word_df = df.select("word_and_type") \
            .withColumn("word", split(col("word_and_type"), "_")[0]) \
            .withColumn("type", split(col("word_and_type"), "_")[1]) \
            .drop("word_and_type")
            
        occurence_df = df.withColumn("year", split(col("occurence"), ",")[0]) \
            .withColumn("frequency", split(col("occurence"), ",")[1]) \
            .withColumn("book_count", split(col("occurence"), ",")[2]) \
            .drop("occurence")

        self.__write(word_df, "word")
        self.__write(occurence_df, "occurence")

        # TODO: error handling