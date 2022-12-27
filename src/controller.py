import os
from abc import ABC
from cmd import Cmd
from getpass import getpass
from typing import Dict, Optional

from pyspark.sql import SparkSession, DataFrame

from src.config_converter import ConfigConverter
from src.database_connection import NgramDB, NgramDBBuilder
from src.transfer import Transferer
from src.info import StatFunctions


class SparkController:
    def __init__(self, config: Dict[str, str]) -> None:
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
        self.__transferer: Optional[Transferer] = Transferer(
            self.__spark,
            self.__db_url,
            self.__properties
        )

        self.__functions: Optional[StatFunctions] = StatFunctions(
            self.__spark,
            self.__db_url,
            self.__properties
        )

        # TODO: add other functions

    def get_spark_session(self) -> SparkSession:
        return self.__spark

    def close(self) -> None:
        self.__spark.stop()

    def transfer(self, path: str) -> None:
        self.__transferer.transfer_textFile(path)

    def execute_sql(self, sql: str) -> DataFrame:
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

        word_df.createOrReplaceTempView("word")
        occurence_df.createOrReplaceTempView("occurence")

        return self.__spark.sql(sql)

    def hrc(self, duration: int) -> DataFrame:
        return self.__functions.hrc(duration)

    def pc(self, start_year: int, end_year: int) -> DataFrame:
        return self.__functions.pc(start_year, end_year)


class DBController:
    def __init__(self, conn_settings: Dict[str, str] = {}) -> None:

        # TODO might be redundant
        self.conn_settings = conn_settings
        self.ngram_db: Optional[NgramDB] = None

    def __init_db(self):
        # TODO: config lesen
        self.ngram_db = NgramDBBuilder(self.conn_settings).connect_to_ngram_db()

        if self.ngram_db is None:
            # TODO return to main menu and retry db init with other connection settings
            print("Connection to DB could not be established. Goodbye!")
            return

        print("Opened connection")
        return


    def close_db_connection(self):
        del self.ngram_db

