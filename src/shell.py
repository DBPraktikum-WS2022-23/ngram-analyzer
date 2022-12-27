import os
from cmd import Cmd
from typing import Dict, List

from src.config_converter import ConfigConverter
from src.database_connection import NgramDBBuilder
from src.info import DataBaseStatistics, WordFrequencies
from src.controller import SparkController, DBController


# TODO: über pfeiltasten vorherigen befehl holen
class Prompt(Cmd):
    intro: str = (
        "Welcome to the ngram_analyzer shell. Type help or ? to list commands.\n"
    )
    prompt: str = "(ngram_analyzer) "

    def preloop(self):
        """Get database connection information from user and use it to set up the current sessions' configuration."""

        # let user choose configuration file
        print("To use this shell, you need to connect to an already existing database.\n"
              "If you don't have a database yet, you can create one via the CLI using the db_connect command.\n"
              "If you already have a database, please select the correct config file:\n")
        for idx, file in enumerate(os.listdir("settings")):
            print(f"[{idx}] {file}")
        choice = input("Enter number of config file: ")
        if not choice.isdigit():
            print("Invalid input. Please restart the shell and try again.")
            return
        choice = int(choice)
        if choice < 0 or choice > len(os.listdir("settings")):
            print("Invalid input. Please restart the shell and try again.")
            return

        # read in configuration data
        self.config: ConfigConverter = ConfigConverter(os.listdir("settings")[choice-1])
        self.db_conn_settings: Dict[str, str] = self.config.get_conn_settings()
        self.jdbc_driver: str = self.config.get_jdbc_path()
        self.data_path: str = self.config.get_data_path()
        self.ngram_db: NgramDBBuilder = NgramDBBuilder(self.db_conn_settings).connect_to_ngram_db()
        self.url: str = self.config.get_db_url()
        self.spark_config: Dict[str, str] = self.config.get_spark_config(self.db_conn_settings,
                                                         self.url,
                                                         self.jdbc_driver)
        self.spark_properties: Dict[str, str] = {
            "user": self.db_conn_settings["user"],
            "password": self.db_conn_settings["password"],
        }
        self.spark_controller: SparkController = SparkController(self.spark_config)
        self.spark = self.spark_controller.get_spark_session()
        self.db_controller: DBController = DBController(self.db_conn_settings)
        print("Connection settings loaded.")

    def do_print_word_frequencies(self, arg) -> None:
        """Print the frequency of selected words for selected years."""

        words: List[str] = input("Enter words: (separate by space) ").split(" ")
        years: List[str] = input("Enter years: (separate by space) ").split(" ")
        for year in years:
            if not year.isdigit():
                print("Year must be a number.")
                return

        wf: WordFrequencies = WordFrequencies(self.spark, self.url, self.spark_properties)
        wf.print_word_frequencies(words, [int(x) for x in years])

    def do_plot_word_frequencies(self, arg) -> None:
        """Plot frequency of words in different years."""

        words: List[str] = input("Enter words: (separate by space) ").split(" ")
        years: List[str] = input("Enter years: (separate by space) ").split(" ")
        for year in years:
            if not year.isdigit():
                print("Year must be a number.")
                return

        wf: WordFrequencies = WordFrequencies(self.spark, self.url, self.spark_properties)
        wf.plot_word_frequencies(words, [int(x) for x in years])

    def do_print_db_statistics(self, arg) -> None:
        """Print statistics of the database tables."""
        dbs: DataBaseStatistics = DataBaseStatistics(self.spark, self.url, self.spark_properties)
        dbs.print_statistics()

    def do_sql(self, arg):
        """Open a prompt to execute SQL queries."""
        print("Welcome to the SQL prompt. Enter your SQL query.")
        print("Enter 'exit' to exit the prompt.")

        # connect to database
        if self.ngram_db is None:
            print("No connection to database. Please connect to a database first.")
            return

        while True:
            sql_query = input("SQL> ")
            if sql_query == "exit":
                break
            try:
                self.spark_controller.execute_sql(sql_query).show()
            except Exception as e:
                print(e) # TODO: invalid sql query

    def do_sql(self, arg):
        """Open a prompt to execute SQL queries."""
        print("Welcome to the SQL prompt. Enter your SQL query.")
        print("Enter 'exit' to exit the prompt.")

        # connect to database
        if self.ngram_db is None:
            print("No connection to database. Please connect to a database first.")
            return

        while True:
            sql_query = input("SQL> ")
            if sql_query == "exit":
                break
            try:
                self.spark_controller.execute_sql(sql_query).show()
            except Exception as e:
                print(e) # TODO: invalid sql query

    def do_exit(self, arg):
        """Leave shell"""
        return True

    # overrides class method, is run before cmdloop returns but not in case the shell crashes
    def postloop(self) -> None:
        if self.ngram_db:
            del self.ngram_db
        if self.spark_controller:
            self.spark_controller.close()
        print("Closed connection")
