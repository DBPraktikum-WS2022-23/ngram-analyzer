""" Simple controller class for pyspark """
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from src.info import DataBaseStatistics, StatFunctions, WordFrequencies
from src.transfer import Transferer
from src.visualiser import Visualiser


class SparkController:
    """Wrapper for the pyspark class"""

    def __init__(self, config: Dict[str, str], log_level: str = "OFF") -> None:
        self.__db_url: str = config["db_url"]
        self.__properties: Dict[str, str] = {
            "user": config["user"],
            "password": config["password"],
        }
        self.__spark: Optional[SparkSession] = (
            SparkSession.builder.appName("ngram_analyzer")
            .master("local[*]")
            .config("spark.driver.extraClassPath", config["jdbc_driver"])
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "1g")
            .getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel(log_level)

        self.__transferer: Optional[Transferer] = Transferer(
            self.__spark, self.__db_url, self.__properties
        )

        self.__wf: WordFrequencies = WordFrequencies(
            self.__spark, self.__db_url, self.__properties
        )

        self.__dbs: DataBaseStatistics = DataBaseStatistics(
            self.__spark, self.__db_url, self.__properties
        )

        self.__functions: Optional[StatFunctions] = StatFunctions()

        self.__visualiser: Visualiser = Visualiser()

        # TODO: this should not be necessary with @udf notation
        self.__spark.udf.register("hrc", StatFunctions.hrc, StatFunctions.schema_s)
        self.__spark.udf.register("pc", StatFunctions.pc, StatFunctions.schema_d)
        self.__spark.udf.register("sf", StatFunctions.stat_feature, StatFunctions.schema_sf)
        self.__spark.udf.register("sfp", StatFunctions.stat_feature_pairs, StatFunctions.schema_sfp)
        self.__spark.udf.register("lr", StatFunctions.lr, StatFunctions.schema_r)

    def get_spark_session(self) -> Optional[SparkSession]:
        """Returns the spark session"""
        return self.__spark

    def close(self) -> None:
        """Closes the spark session"""
        if self.__spark is not None:
            self.__spark.stop()

    def transfer(self, paths: List[str]) -> None:
        """Transfers a list of files to the database"""
        for path in paths:
            if self.__transferer is not None:
                self.__transferer.transfer_textFile(path)

    def execute_sql(self, sql: str) -> Optional[DataFrame]:
        """Executes a SQL query"""

        # select str_rep, type, `1800` from schema_f limit 5

        if self.__spark is not None:
            word_df = self.__spark.read.jdbc(
                url=self.__db_url,
                table="word",
                properties=self.__properties,
            )
            occurence_df = self.__spark.read.jdbc(
                url=self.__db_url,
                table="occurence",
                properties=self.__properties,
            )

            years = []
            for i in range(1800, 2001, 1):
                years.append(i)

            schema_f_df = (
                occurence_df.select("id", "year", "freq")
                .join(word_df, "id")
                .select("str_rep", "type", "year", "freq")
                .groupBy("str_rep", "type")
                .pivot("year", years)
                .sum("freq")
                .na.fill(0)
            )

            word_df.createOrReplaceTempView("word")
            occurence_df.createOrReplaceTempView("occurence")
            schema_f_df.createOrReplaceTempView("schema_f")
            return self.__spark.sql(sql)
        return None

    def print_word_frequencies(self, words: List[str], years: List[int]) -> None:
        self.__wf.print_word_frequencies(words, years)

    def plot_word_frequencies(self, words: List[str], years: List[int]) -> None:
        self.__wf.plot_word_frequencies(words, years)

    def print_db_statistics(self) -> None:
        self.__dbs.print_statistics()

    # TODO: Does (or should) the user interface offer access to hrc and pc functionality
    #       other than through spark SQL?
    # def hrc(self, duration: int) -> DataFrame:
    #     return self.__functions.hrc(duration)

    # def pc(self, start_year: int, end_year: int) -> DataFrame:
    #     return self.__functions.pc(start_year, end_year)

    def plot_kde(self) -> None:
        self.__visualiser.plot_kde()

    def plot_box(self) -> None:
        # TODO: read schema_f from somewhere

        if self.__spark is not None:
            word_df = self.__spark.read.jdbc(
                url=self.__db_url,
                table="word",
                properties=self.__properties,
            )
            occurence_df = self.__spark.read.jdbc(
                url=self.__db_url,
                table="occurence",
                properties=self.__properties,
            )

            years = []
            for i in range(1800, 2001, 1):
                years.append(i)

            schema_f_df = (
                occurence_df.select("id", "year", "freq")
                .join(word_df, "id")
                .select("str_rep", "type", "year", "freq")
                .groupBy("str_rep", "type")
                .pivot("year", years)
                .sum("freq")
                .na.fill(0)
            )
            self.__visualiser.plot_boxplot_all(schema_f_df, 1800, 2000)

    def plot_scatter(self) -> None:
        # TODO: read schema_f from somewhere

        if self.__spark is not None:
            word_df = self.__spark.read.jdbc(
                url=self.__db_url,
                table="word",
                properties=self.__properties,
            )
            occurence_df = self.__spark.read.jdbc(
                url=self.__db_url,
                table="occurence",
                properties=self.__properties,
            )

            years = []
            for i in range(1800, 2001, 1):
                years.append(i)

            schema_f_df = (
                occurence_df.select("id", "year", "freq")
                .join(word_df, "id")
                .select("str_rep", "type", "year", "freq")
                .groupBy("str_rep", "type")
                .pivot("year", years)
                .sum("freq")
                .na.fill(0)
            )
            self.__visualiser.plot_scatter_all(schema_f_df)

