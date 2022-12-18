import os
from cmd import Cmd
from getpass import getpass
from typing import Dict, Optional

from pyspark.sql import SparkSession

from src.config_converter import ConfigConverter
from src.database_connection import NgramDB, NgramDBBuilder
from src.info import DataBaseStatistics, WordFrequencies
from src.transfer import Transferer


# TODO: über pfeiltasten vorherigen befehl holen
class Prompt(Cmd):
    intro: str = (
        "Welcome to the ngram_analyzer shell. Type help or ? to list commands.\n"
    )
    prompt: str = "(ngram_analyzer) "

    # TODO might be redundant
    conn_settings: Dict[str, str] = {}
    config: Optional[ConfigConverter] = None
    ngram_db: Optional[NgramDB] = None

    spark = (
        SparkSession.builder.appName("ngram_analyzer")
        .master("local[*]")
        .config("spark.driver.extraClassPath", "./resources/postgresql-42.5.1.jar")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )

    transferer: Optional[Transferer] = None

    def do_db_connect(self, arg):
        # init db
        user: str = input("Enter user name:")
        self.config = ConfigConverter(user)
        if not self.config.user_exists:
            password: str = getpass()
            dbname: str = input("Enter database name:")
            self.config.generate_conn_settings(password, dbname)
        self.conn_settings = self.config.get_conn_settings()
        # TODO: this wrapper function might be useless but it appears here more readable to me
        self.ngram_db = NgramDBBuilder(self.conn_settings).connect_to_ngram_db()

        if self.ngram_db is None:
            # TODO return to main menu and retry db init with other connection settings
            print("Connection to DB could not be established. Goodbye!")
            return

        print("Opened connection")
        self.config.save_conn_settings()
        #
        # Work with the database. For instance:
        # result = self.ngram_db.execute('SELECT version()')
        #
        # print(f'PostgreSQL database version: {result}')

    # TODO: hier sollte arg nicht fuer path UND -default stehen.
    # also noch einen param hinzufuegen oder so
    def do_transfer(self, arg: str) -> None:
        """Transfer data from a file to the database."""

        temp_path: str = arg

        if self.ngram_db is None:
            print("No connection to database. Please connect to a database first.")
            return

        if arg == "":
            print("Please provide a path to a file.")
            return

        if arg == "-default":
            temp_path = self.conn_settings["default_filepath"]

        if not os.path.isfile(temp_path):
            print("Please enter a valid path.")
            return

        if self.transferer is None:
            prop_dict = self.conn_settings
            url = (
                "jdbc:postgresql://"
                + prop_dict["host"]
                + ":"
                + prop_dict["port"]
                + "/"
                + prop_dict["dbname"]
            )
            # TODO store name of database
            print(url)
            properties: Dict[str, str] = {
                "user": prop_dict["user"],
                "password": prop_dict["password"],
            }
            self.transferer = Transferer(self.spark, url, properties)

            self.transferer.transfer_textFile(temp_path)

        print("You have successfully transferred the data.")

    def do_set_default_file(self, arg) -> None:
        if self.ngram_db is None:
            print("No connection to database. Please connect to a database first.")
            return

        path = input("Enter default file path:")
        self.config.set_default_path(path)
        self.conn_settings = self.config.get_conn_settings()

        print("Set '" + path + "' as default file path for command 'transfer -default'")

    def do_print_word_frequencies(self, arg) -> None:
        wf: WordFrequencies = WordFrequencies(["1972_NUM", "Ausbreitung"], [1973, 1972])
        wf.print_word_frequencies()

    def do_plot_word_frequencies(self, arg) -> None:
        wf: WordFrequencies = WordFrequencies(["1972_NUM", "Ausbreitung"], [1973, 1972])
        wf.plot_word_frequencies()

    def do_print_db_statistics(self, arg) -> None:
        dbs: DataBaseStatistics = DataBaseStatistics()
        dbs.print_statistics()

    def do_exit(self, arg):
        return True

    # overrides class method, is run before cmdloop returns but not in case the shell crashes
    def postloop(self) -> None:
        if self.ngram_db:
            del self.ngram_db
        self.spark.stop()
        print("Closed connection")
