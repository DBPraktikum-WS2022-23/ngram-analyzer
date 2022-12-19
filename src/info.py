import os
from typing import Any, List

import matplotlib.pyplot as plt  # type: ignore
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as f


class DatabaseToSparkDF:
    def __init__(self) -> None:
        self.spark: SparkSession = (
            SparkSession.builder.appName("Python Spark SQL")
            .config("spark.jars", "/resources/postgresql-42.5.1.jar")
            .getOrCreate()
        )
        self.__set_up()

    def __set_up(self) -> None:
        """Set up the spark session and the dataframes"""
        self.df_word: DataFrame = (
            self.spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/ngram_db")
            .option("dbtable", "word")
            .option("user", "postgres")
            .option("password", "abcd1234")
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        self.df_occurence: DataFrame = (
            self.spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/ngram_db")
            .option("dbtable", "occurence")
            .option("user", "postgres")
            .option("password", "abcd1234")
            .option("driver", "org.postgresql.Driver")
            .load()
        )


class DataBaseStatistics:
    def __init__(self) -> None:
        self.df_word: DataFrame = DatabaseToSparkDF().df_word
        self.df_occurence: DataFrame = DatabaseToSparkDF().df_occurence

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
    def __init__(self, words: List[str], years: List[int]) -> None:
        """Set uo Word Frequency Object"""
        self.words: List[str] = words
        self.years: List[int] = years
        self.df_word: DataFrame = DatabaseToSparkDF().df_word
        self.df_occurence: DataFrame = DatabaseToSparkDF().df_occurence
        self.string_representations = self._get_string_representations()

    def _get_string_representations(self) -> List[Row]:
        """Set up the spark session and the dataframes"""
        # create a spark dataframe with the words and years
        return (
            self.df_word.filter(self.df_word.str_rep.isin(self.words))
            .select("id", "str_rep")
            .distinct()
            .collect()
        )

    def plot_word_frequencies(self) -> None:
        """Plot the frequency of certain words in certain years"""
        if self.string_representations is None:
            print("No entries for specified words found")
            return

        fig, ax = plt.subplots()
        ax.set_title("Frequency of words in years")
        ax.set_xlabel("year")
        ax.set_xticks(range(min(self.years), max(self.years) + 1))
        ax.set_ylabel("frequency")

        for row in self.string_representations:
            df: DataFrame = self.df_occurence.filter(
                self.df_occurence.id == row.id
            ).filter(self.df_occurence.year.isin(self.years))
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
        plt_name: str = f"output/word_frequency_plot_{'_'.join(self.words + [str(year) for year in self.years])}.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")

    def print_word_frequencies(self) -> None:
        """Print the frequency of certain words in certain years"""
        if self.string_representations is None:
            print("No entries for specified words found")
            return
        for row in self.string_representations:
            print(f"{row.str_rep}: ")
            df: DataFrame = (
                self.df_occurence.filter(self.df_occurence.id == row.id)
                .filter(self.df_occurence.year.isin(self.years))
                .select("year", "freq")
            )
            df.show()
