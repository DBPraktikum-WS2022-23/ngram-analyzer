""" Module for creating statistics and plots"""
import os
from typing import Any, Dict, List, Tuple

import matplotlib.pyplot as plt  # type: ignore
import numpy
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


class DatabaseToSparkDF:
    """Module which reads data from the database into spark dataframes"""

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

        years = []
        for i in range(1800, 2001, 1):
            years.append(i)

        self.df_schema_f = (
            self.df_occurence.select("id", "year", "freq")
            .join(self.df_word, "id")
            .select("str_rep", "type", "year", "freq")
            .groupBy("str_rep", "type")
            .pivot("year", years)
            .sum("freq")
            .na.fill(0)
        )


class DataBaseStatistics:
    """Module which creates statistics from the database"""

    def __init__(
        self, spark: SparkSession, db_url: str, properties: Dict[str, str]
    ) -> None:
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
    """Module for creating statistics about word frequencies"""

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

        _, axis = plt.subplots()
        axis.set_title("Frequency of words in years")
        axis.set_xlabel("year")
        axis.set_xticks(range(min(years), max(years) + 1))
        axis.set_ylabel("frequency")

        for row in string_representations:
            dataframe: DataFrame = self.df_occurence.filter(
                self.df_occurence.id == row.id
            ).filter(self.df_occurence.year.isin(years))
            axis.scatter(
                dataframe.select("year").collect(),
                dataframe.select("freq").collect(),
                label=row.str_rep,
            )

        axis.legend()
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
            dataframe: DataFrame = (
                self.df_occurence.filter(self.df_occurence.id == row.id)
                .filter(self.df_occurence.year.isin(years))
                .select("year", "freq")
            )
            dataframe.show()


class StatFunctions:
    def __init__() -> None:
        pass

    """Return type for calculations on time interval of one word."""
    schema_s = StructType(
        [
            StructField("str_rep", StringType(), False),
            StructField("type", StringType(), False),
            StructField("start_year", IntegerType(), False),
            StructField("end_year", IntegerType(), False),
            StructField("result", FloatType(), False),
        ]
    )

    """Return type for calculations on time intervals of two words."""
    schema_d = StructType(
        [
            StructField("str_rep_1", StringType(), False),
            StructField("type_1", StringType(), False),
            StructField("str_rep_2", StringType(), False),
            StructField("type_2", StringType(), False),
            StructField("start_year", IntegerType(), False),
            StructField("end_year", IntegerType(), False),
            StructField("result", FloatType(), False),
        ]
    )

    schema_sf = StructType(
        [
            StructField("mean", FloatType(), False),
            StructField("median", FloatType(), False),
            StructField("var", FloatType(), False),
            StructField("min", FloatType(), False),
            StructField("max", FloatType(), False),
            StructField("q_25", FloatType(), False),
            StructField("q_75", FloatType(), False),
            StructField("hrc", FloatType(), False),
        ]
    )

    schema_sfp = StructType(
        [
            StructField("hrc_year", IntegerType(), False),
            StructField("hrc_max", FloatType(), False),
            StructField("cov", FloatType(), False),
            StructField("spearman_corr", FloatType(), False),
            StructField("pearson_corr", FloatType(), False),
        ]
    )


    @staticmethod
    def hrc(duration, word, w_type, *years):
        """Returns the strongest relative change between any two years that duration years apart."""

        hrc_result = 0.0
        result_start_year = 0
        result_end_year = 0

        #print(f"duration: {duration}")
        #print(f"word: {word}")
        #print(f"type: {w_type}")
        #for i in range(0,4):
        #    print(f"years[{i}]: {years[i]}")

        # duration = int(duration)

        # F-tuple format: str_rep, type, frq_1800, ..., frq_2000
        # 2000-1799 = 201 values, offset 2 -> 203
        for year in range(0, (201 - duration)):

            #print(f"f_tuple[year]: {f_tuple[year]}")

            start: int = int(years[year])
            end: int = int(years[year + duration])

            #print(f"start: {start}")

            # relative change does not exist if one of the entries is 0
            if start == 0 or end == 0:
                continue

            # TODO: Consider direction of change (0.5 vs 2)? Currently change always >1
            change = max((start / end), (end / start)) - 1

            if change > hrc_result:
                hrc_result = change
                result_start_year = start
                result_end_year = end

        if not w_type:
            w_type = ""

        #print(f"hrc: {hrc_result}")

        # TODO: assuming start and end years should be returned here and not their frequencies
        return word, w_type, result_start_year, result_end_year, hrc_result

    @staticmethod
    def pc(start_year: int, end_year: int, *fxf_tuple):
        """Returns the Pearson correlation coefficient of two time series
        (limited to the time period of [start year, end year])."""

        # FxF format: w1, t1, frq1_1800, ..., frq1_2000, w2, t2, frq2_1800, ..., frq2_2000

        # split input tuple
        str_rep_1 = fxf_tuple[0]
        type_1 = fxf_tuple[1]
        frequencies_1 = fxf_tuple[2:203]

        str_rep_2 = fxf_tuple[203]
        type_2 = fxf_tuple[204]
        frequencies_2 = fxf_tuple[205:]

        # limit to interval between start and end year, each inclusive
        start_index = start_year - 1800
        end_index = end_year - 1800 + 1  # end year inclusive
        frequencies_1 = frequencies_1[start_index:end_index]
        frequencies_2 = frequencies_2[start_index:end_index]

        # pearson correlation coefficient in second entry in first row in matrix from numpy
        pearson_corr = numpy.corrcoef(frequencies_1, frequencies_2)[0][1]

        return str_rep_1, type_1, str_rep_2, type_2, start_year, end_year, pearson_corr

    @staticmethod
    def stat_feature(word, type, *f_tuple) -> Tuple[float, float, float, float, float, float, float, float]:
        """Returns a dictionary of statistical features for a given schema f."""

        # F format: str_rep, type, frq_1800, ..., frq_2000

        f_int_list = [int(i) for i in f_tuple]
        f_array = numpy.asarray(f_int_list)

        # calculate statistical features
        mean = numpy.mean(f_array).item()
        median = numpy.median(f_array).item()
        q_25 = numpy.percentile(f_array, 25).item()
        q_75 = numpy.percentile(f_array, 75).item()
        var = numpy.var(f_array).item()
        min = numpy.min(f_array).item()
        max = numpy.max(f_array).item()
        hrc = 0.0  # StatFunctions.hrc(1, word, type, f_tuple)[-1]

        return mean, median, var, min, max, q_25, q_75, hrc

    @staticmethod
    def stat_feature_pairs(*fxf_tuple):
        """Returns a dictionary of statistical features for a given schema fxf."""

        # FxF format: w1, t1, frq1_1800, ..., frq1_2000, w2, t2, frq2_1800, ..., frq2_2000

        # split input tuple
        freq1 = fxf_tuple[2:203]
        freq2 = fxf_tuple[205:]

        # calculate statistical features
        numpy.seterr(divide='ignore', invalid='ignore')
        hrc_l = numpy.divide(numpy.subtract(freq2, freq1), freq1)
        hrc_l = numpy.nan_to_num(numpy.absolute(hrc_l), nan=0.0, posinf=0.0, neginf=0.0)
        hrc_year = numpy.argmax(hrc_l).item() + 1800
        hrc_max = numpy.max(hrc_l).item()
        cov = numpy.cov(freq1, freq2)[0][1].item()
        pearson_corr = 0.0 # StatFunctions.pc(1800, 2000, *fxf_tuple)[-1]
        spearman_corr = cov / (numpy.std(freq1).item() * numpy.std(freq2).item())

        return  hrc_year, hrc_max, cov, spearman_corr, pearson_corr
