import os
from cmd import Cmd
from getpass import getpass
from typing import Dict, Optional

from pyspark.sql import SparkSession

from src.config_converter import ConfigConverter
from src.database_connection import NgramDB, NgramDBBuilder
from src.transfer import Transferer


# TODO: über pfeiltasten vorherigen befehl holen
class Prompt(Cmd):
    intro: str = (
        "Welcome to the ngram_analyzer shell. Type help or ? to list commands.\n"
    )
    prompt: str = "(ngram_analyzer) "

    # TODO might be redundant
    conn_settings: Dict[str, str] = {}
    jdbc_driver: str = ''
    data_path: str = ''
    config: Optional[ConfigConverter] = None
    ngram_db: Optional[NgramDB] = None

    spark = None

    transferer: Optional[Transferer] = None

    def do_db_connect(self, arg):
        """Connect to database. This will create the database and relations if they don't exist."""
        # init db
        user: str = input("Enter user name:")
        self.config = ConfigConverter(user)
        if not self.config.user_exists:
            password: str = getpass()
            dbname: str = input("Enter database name:")
            self.config.generate_conn_settings(password, dbname)
        self.conn_settings = self.config.get_conn_settings()
        self.jdbc_driver = self.config.get_jdbc_path()
        self.data_path = self.config.get_data_path()
        # TODO: this wrapper function might be useless but it appears here more readable to me
        self.ngram_db = NgramDBBuilder(self.conn_settings).connect_to_ngram_db()

        if self.ngram_db is None:
            # TODO return to main menu and retry db init with other connection settings
            print("Connection to DB could not be established. Goodbye!")
            return

        print("Opened connection")

    def do_transfer(self, arg: str) -> None:
        """Transfer data from a file to the database."""

        temp_path: str = arg

        if self.ngram_db is None:
            print("No connection to database. Please connect to a database first.")
            return

        if arg == '':
            temp_path = self.data_path

        if not os.path.isfile(temp_path) and not os.path.isdir(temp_path):
            print("Please enter a valid path.")
            return

        driver_path: str = self.jdbc_driver
        if driver_path == '':
            driver_path = "./jdbc-driver/postgresql-42.5.1.jar"

        self.spark = (
            SparkSession.builder.appName("ngram_analyzer")
            .master("local[*]")
            .config("spark.driver.extraClassPath", driver_path)
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "1g")
            .getOrCreate()
        )

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
            properties: Dict[str, str] = {
                "user": prop_dict["user"],
                "password": prop_dict["password"],
            }
            self.transferer = Transferer(self.spark, url, properties)

            if os.path.isfile(temp_path):
                self.transferer.transfer_textFile(temp_path)
            elif os.path.isdir(temp_path):  # handle directory
                for cur_path, _, files in os.walk(temp_path):
                    for file in files:
                        print(f"Transferring {file}")
                        self.transferer.transfer_textFile(os.path.join(cur_path, file))

        print("You have successfully transferred the data.")

    def do_exit(self, arg):
        return True

    # overrides class method, is run before cmdloop returns but not in case the shell crashes
    def postloop(self) -> None:
        if self.ngram_db:
            del self.ngram_db
        if self.spark:
            self.spark.stop()
        print("Closed connection")
