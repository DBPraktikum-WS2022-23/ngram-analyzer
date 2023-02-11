""" Simple controller class for pyspark """
from typing import Dict, List, Optional
import importlib
import pkgutil

from pyspark.sql import DataFrame, SparkSession

from info import DataBaseStatistics, WordFrequencies
from transfer import Transferer
from visualiser import Visualiser
from plugins.base_plugin import BasePlugin

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

    def get_df_word(self) -> DataFrame:
        return self.df_word

    def get_df_occurence(self) -> DataFrame:
        return self.df_occurence

class PluginController:
    def __init__(self, spark: SparkSession) -> None:
        self.__spark = spark

    def register_plugins(self, plugins_path: str = "src/plugins"):
        mod_plugin_path = plugins_path.replace("/", ".")
        mod_plugin_path = mod_plugin_path.replace("\\", ".")
        mod_plugin_path = mod_plugin_path[4:]


        discovered_plugins: List[str] = [f"{mod_plugin_path}.{name}"
                              for _, name, _
                              in pkgutil.iter_modules(path=[plugins_path])
                              if name.endswith('Plugin')]

        print("Discovered plugins: ", discovered_plugins)

        for plugin_path in discovered_plugins:
            module = importlib.import_module(plugin_path)
            current_plugin = plugin_path.split(".")[-1]
            plugin = getattr(module, current_plugin)
            if issubclass(plugin, BasePlugin):
                plugin(spark=self.__spark, ).register_udfs()
                print("Successfully loaded plugin: ", current_plugin)


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

        self.db2df: DatabaseToSparkDF = DatabaseToSparkDF(
            self.__spark, self.__db_url, self.__properties
        )
        self.__word_df: DataFrame = self.db2df.get_df_word()
        self.__occurence_df: DataFrame = self.db2df.get_df_occurence()

        self.__wf: WordFrequencies = WordFrequencies(
            self.__word_df, self.__occurence_df
        )
        self.__dbs: DataBaseStatistics = DataBaseStatistics(
            self.__word_df, self.__occurence_df
        )

        self.__visualiser: Visualiser = Visualiser()

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

    def __get_schema_f_df(self) -> DataFrame:
        years = []
        for i in range(1800, 2001, 1):
            years.append(i)

        schema_f_df = (
            self.__occurence_df.select("id", "year", "freq")
            .join(self.__word_df, "id")
            .select("str_rep", "type", "year", "freq")
            .groupBy("str_rep", "type")
            .pivot("year", years)
            .sum("freq")
            .na.fill(0)
        )
        return schema_f_df

    def execute_sql(self, sql: str) -> Optional[DataFrame]:
        """Executes a SQL query"""
        if self.__spark is not None:
            self.__word_df.createOrReplaceTempView("word")
            self.__occurence_df.createOrReplaceTempView("occurence")
            self.__get_schema_f_df().createOrReplaceTempView("schema_f")
            return self.__spark.sql(sql)
        return None

    def create_ngram_view(self, words: List[str]) -> None:
        """Creates a view for selected ngrams"""
        query = "select * from schema_f where str_rep in (" + ",".join(
            [f"'{word}'" for word in words]
        ) + ")"
        self.execute_sql(query).createOrReplaceTempView("ngram")

    def print_word_frequencies(self, words: List[str], years: List[int]) -> None:
        self.__wf.print_word_frequencies(words, years)

    def plot_word_frequencies(self, words: List[str], years: List[int]) -> None:
        self.__wf.plot_word_frequencies(words, years)

    def print_db_statistics(self) -> None:
        self.__dbs.print_statistics()

    def plot_kde(self, word: str, bandwidth: float, bins: int) -> None:
        schema_f_df = self.__get_schema_f_df()
        self.__visualiser.plot_kde(schema_f_df, 1800, 2000, word, bandwidth, bins)

    def plot_box(self, scaling_factor: float) -> None:
        schema_f_df = self.__get_schema_f_df()

        self.__visualiser.plot_boxplot_all(schema_f_df, 1800, 2000, scaling_factor)

    def plot_scatter(self) -> None:
        schema_f_df = self.__get_schema_f_df()

        # draws without regression line, scaling optional
        self.__visualiser.plot_scatter(schema_f_df, 1800, 2000)

    def plot_scatter_with_regression(self) -> None:
        schema_f_df = self.__get_schema_f_df()

        # draws with regression line, has no scaling
        self.__visualiser.plot_scatter(schema_f_df, 1800, 2000, with_regression_line=True)
