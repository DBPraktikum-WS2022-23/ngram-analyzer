import os
from typing import Any, Dict, List

import matplotlib.pyplot as plt  # type: ignore
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType, FloatType, StructField, StructType


class DatabaseToSparkDF:
    def __init__(self, spark: SparkSession, db_url: str, properties: Dict[str, str]):
        self.__spark = spark
        self.__db_url = db_url
        self.__properties = properties
        self.__set_up()

    def __set_up(self) -> None:
        """Set up the spark session and the dataframes"""
        self.df_word: DataFrame = self.__spark.read.jdbc(
            self.__db_url, "word", properties=self.__properties
        )

        self.df_occurence: DataFrame = self.__spark.read.jdbc(
            self.__db_url, "occurence", properties=self.__properties
        )


class DataBaseStatistics:
    def __init__(self, spark: SparkSession, db_url: str, properties: Dict[str, str]) -> None:
        self.db2df: DatabaseToSparkDF = DatabaseToSparkDF(spark, db_url, properties)
        self.df_word: DataFrame = self.db2df.df_word
        self.df_occurence: DataFrame = self.db2df.df_occurence

    def print_statistics(self) -> None:
        """Print the info about the database"""
        print("Number of words: ", self.get_number_of_words())
        print("Number of occurences: ", self.get_number_of_occurences())
        print("Number of years: ", self.get_number_of_years())
        print("Highest frequency of a word: ", self.get_highest_frequency())

    def get_number_of_words(self) -> int:
        """Get the number of words in the database"""
        return self.df_word.count()

    def get_number_of_occurences(self) -> int:
        """Get the number of occurences in the database"""
        return self.df_occurence.count()

    def get_highest_frequency(self) -> Any:
        """Get the highest frequency of a word"""
        return self.df_occurence.agg(f.max("freq")).collect()[0][0]

    def get_number_of_years(self) -> int:
        """Get the number of years in the database"""
        return self.df_occurence.select("year").distinct().count()


class WordFrequencies:
    def __init__(
        self, spark: SparkSession, db_url: str, properties: Dict[str, str]
    ) -> None:
        """Set uo Word Frequency Object"""
        self.db2df: DatabaseToSparkDF = DatabaseToSparkDF(spark, db_url, properties)
        self.df_word: DataFrame = self.db2df.df_word
        self.df_occurence: DataFrame = self.db2df.df_occurence

    def _get_string_representations(self, words: List[str]) -> List[Row]:
        """Set up the spark session and the dataframes"""
        # create a spark dataframe with the words and years
        return (
            self.df_word.filter(self.df_word.str_rep.isin(words))
            .select("id", "str_rep")
            .distinct()
            .collect()
        )

    def plot_word_frequencies(self, words: List[str], years: List[int]) -> None:
        """Plot the frequency of certain words in certain years"""
        string_representations: List[Row] = self._get_string_representations(words)
        if string_representations is None:
            print("No entries for specified words found")
            return

        fig, ax = plt.subplots()
        ax.set_title("Frequency of words in years")
        ax.set_xlabel("year")
        ax.set_xticks(range(min(years), max(years) + 1))
        ax.set_ylabel("frequency")

        for row in string_representations:
            df: DataFrame = self.df_occurence.filter(
                self.df_occurence.id == row.id
            ).filter(self.df_occurence.year.isin(years))
            ax.scatter(
                df.select("year").collect(),
                df.select("freq").collect(),
                label=row.str_rep,
            )

        ax.legend()
        plt.show()

        # check if the directory output already exists, if not, create it
        if not os.path.exists("output"):
            os.mkdir("output")
        plt_name: str = f"output/word_frequency_plot_{'_'.join(words + [str(year) for year in years])}.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")

    def print_word_frequencies(self, words: List[str], years: List[int]) -> None:
        """Print the frequency of certain words in certain years"""
        string_representations: List[Row] = self._get_string_representations(words)
        if string_representations is None:
            print("No entries for specified words found")
            return
        for row in string_representations:
            print(f"{row.str_rep}: ")
            df: DataFrame = (
                self.df_occurence.filter(self.df_occurence.id == row.id)
                .filter(self.df_occurence.year.isin(years))
                .select("year", "freq")
            )
            df.show()

class StatFunctions:
    def __init__(
        self, spark: SparkSession, db_url: str, properties: Dict[str, str]
    ) -> None:
        self.__spark = spark
        # TODO: maybe redundant?
        self.db2df: DatabaseToSparkDF = DatabaseToSparkDF(spark, db_url, properties)
        self.df_word: DataFrame = self.db2df.df_word
        self.df_occurence: DataFrame = self.db2df.df_occurence

    def __get_f_view(self) -> DataFrame:
        schema_l = [
            StructField("str_rep", StringType(), False),
            StructField("type", StringType(), False)
        ]

        for i in range (1800, 2000, 1):
            schema.append(StructField(str(i), IntegerType(), True))

        schema = StructType(schema_l)

        #TODO: write data

        df = self.__spark.createDataFrame([], schema).createOrReplaceTempView("f_view")
        return df

    def __get_s_df(self) -> DataFrame:
        """Returns a view of words with start year, end year and a result."""
        schema = StructType(
            [
                StructField("str_rep", StringType(), False),
                StructField("type", StringType(), False),
                StructField("start_year", IntegerType(), False),
                StructField("end_year", IntegerType(), False),
                StructField("result", FloatType(), False)
            ]
        )

        df = self.__spark.createDataFrame([], schema)
        return df

    def __get_d_df(self) -> DataFrame:
        """Returns a view of two words with their start years, end years and a result."""
        schema = StructType(
            [
                StructField("str_rep_1", StringType(), False),
                StructField("type_1", StringType(), False),
                StructField("str_rep_2", StringType(), False),
                StructField("type_2", StringType(), False),
                StructField("start_year", IntegerType(), False),
                StructField("end_year", IntegerType(), False),
                StructField("result", FloatType(), False)
            ]
        )

        df = self.__spark.createDataFrame([], schema)
        return df
    
    def hrc(duration: int) -> DataFrame:
        pass

    def pc(start_year: int, end_year: int) -> DataFrame:
        pass
