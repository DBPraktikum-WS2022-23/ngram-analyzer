""" Module for hosting the shell """
import os
from cmd import Cmd
from typing import Dict, List

from src.config_converter import ConfigConverter
from src.controller import SparkController
from src.info import DataBaseStatistics, WordFrequencies


class Prompt(Cmd):
    intro: str = (
        "Welcome to the ngram_analyzer shell. Type help or ? to list commands.\n"
    )
    prompt: str = "(ngram_analyzer) "

    def preloop(self) -> None:
        """Get database connection information from user and use it
        to set up the current sessions' configuration."""

        # let user choose configuration file
        print(
            "To use this shell, you need to connect to an already existing database.\n"
            "If you don't have a database yet, you can create one via the CLI "
            "using the db_connect command.\n"
            "If you already have a database, please select the correct config file:\n"
        )
        for idx, file in enumerate(os.listdir("settings")):
            print(f"[{idx}] {file}")
        choice: int = int(input("Enter number of config file: "))
        if choice < 0 or choice > len(os.listdir("settings")):
            print("Invalid input. Please restart the shell and try again.")
            return

        # read in configuration data
        config: ConfigConverter = ConfigConverter(
            os.listdir("settings")[choice].split("_")[1].split(".")[0]  # type: ignore
        )
        self.settings: Dict[str, str] = config.get_conn_settings()
        self.spark_controller: SparkController = SparkController(
            self.settings, log_level="OFF"
        )
        self.spark = self.spark_controller.get_spark_session()
        print("Connection settings loaded.")

    def do_print_word_frequencies(self, arg) -> None:
        """Print the frequency of selected words for selected years."""

        words: List[str] = input("Enter words: (separate by space) ").split(" ")
        years: List[str] = input("Enter years: (separate by space) ").split(" ")
        for year in years:
            if not year.isdigit():
                print("Year must be a number.")
                return

        if self.spark is not None:  # type: ignore
            wf: WordFrequencies = WordFrequencies(
                self.spark, self.url, self.spark_properties  # type: ignore
            )
            wf.print_word_frequencies(words, [int(x) for x in years])

    def do_plot_word_frequencies(self, arg) -> None:
        """Plot frequency of words in different years."""

        words: List[str] = input("Enter words: (separate by space) ").split(" ")
        years: List[str] = input("Enter years: (separate by space) ").split(" ")
        for year in years:
            if not year.isdigit():
                print("Year must be a number.")
                return

        if self.spark is not None:
            wf: WordFrequencies = WordFrequencies(
                self.spark, self.url, self.spark_properties  # type: ignore
            )
            wf.plot_word_frequencies(words, [int(x) for x in years])

    def do_print_db_statistics(self, arg) -> None:
        """Print statistics of the database tables."""
        if self.spark is not None:  # type: ignore
            dbs: DataBaseStatistics = DataBaseStatistics(
                self.spark, self.url, self.spark_properties  # type: ignore
            )
        dbs.print_statistics()

    def do_sql(self, arg):
        """Open a prompt to execute SQL queries."""
        print("Welcome to the SQL prompt. Enter your SQL query.")
        print("Enter 'exit' to exit the prompt.")

        while True:
            sql_query = input("SQL> ")
            if sql_query == "exit":
                break
            try:
                self.spark_controller.execute_sql(sql_query).show()
            except Exception as e:
                print(e)  # TODO: invalid sql query

    def do_exit(self, arg):
        """Leave shell"""
        return True

    # overrides class method, is run before cmdloop returns but not in case the shell crashes
    def postloop(self) -> None:
        if self.spark_controller:
            self.spark_controller.close()
        print("Closed connection")
