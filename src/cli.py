import argparse
import os
import sys
import time

from src.config_converter import ConfigConverter
from src.database_connection import NgramDB, NgramDBBuilder
from src.transfer import Transferer

from getpass import getpass
from typing import Dict, Optional
from src.shell import Prompt


from pyspark.sql import SparkSession


class Cli:

    def __init__(self) -> None:
        # TODO might be redundant
        self.conn_settings: Dict[str, str] = {}
        self.config: ConfigConverter = ConfigConverter("sample")
        self.ngram_db: Optional[NgramDB] = None
        self.spark = None

    def __exit(self) -> None:

        # TODO: close db connection
        if self.ngram_db:
            del self.ngram_db
        if self.spark:
            self.spark.stop()

        sys.exit()

    def __exit_error(self, message) -> None:

        # TODO: close db connection
        if self.ngram_db:
            del self.ngram_db
        if self.spark:
            self.spark.stop()

        sys.exit(f"Error: {message}.")


    def run_cli(self) -> None:
        psr = argparse.ArgumentParser(
            prog="main.py",
            description="A program to store ngrams in a database and analyse "\
            "them. To start interactive shell call program without parameters",
            epilog="Text at the bottom of help",
        )

        # psr.add_argument(
        #     "-c", "--config", metavar="FILE", help="Path to the config file"
        # )  # option that takes a value

        psr.add_argument(
            "-c",
            "--create-db",
            action="store_true",
            help="Creates necessary database relations if not existing",
        )

        psr.add_argument(
            "-t",
            "--transfer",
            metavar="PATH",
            help="Reads raw data from supplyed file or folder and imports it into"\
            " database, use -r to recurse into subdirectories",
        )

        psr.add_argument(
            "-s",
            "--shell",
            action="store_true",
            help="Open shell",
        )
        
        psr.add_argument(
            "-u",
            "--username",
            metavar="USER",
            help="Username for the database",
        )
        
        psr.add_argument(
            "-p",
            "--password",
            metavar="PASSWORD",
            help="Password for the database",
        )

        psr.add_argument(
            "-db",
            "--dbname",
            metavar="DBNAME",
            help="Name for the database",
        )

        args = vars(psr.parse_args())

        print("Command line arguments are:")  # TODO remove later
        print(args)  # TODO remove later
        print("")  # TODO remove later

        if args["shell"]:
            Prompt().cmdloop()
            return

        if args['username']:
            conn_settings = self.config.get_conn_settings()
            username = args["username"]
            if conn_settings["user"] != username:
                if args["password"] is None or args["dbname"] is None:
                    self.__exit_error(f"config file for user {args['username']} does not exist. Please enter username, password and dbname")
                password = args["password"]
                dbname: str = args["dbname"]
                self.config.generate_conn_settings_sample(username, password, dbname)

        if args["create_db"] or args["transfer"] is not None:
            # check config and open connection
            # TODO: [optional] add option to overwrite username and password
            self.conn_settings = self.config.get_conn_settings()
            ngram_db = NgramDBBuilder(self.conn_settings).connect_to_ngram_db()
            if ngram_db is None:
                self.__exit_error("connection to DB could not be established. Please input username, password and dbname")

        if args["transfer"] is not None:
            path = os.path.abspath(args["transfer"])

            if not os.path.exists(path):
                self.__exit_error(f"path '{path}' does not exist")

            data_files = []  # list of files to process

            self.spark = (
                SparkSession.builder.appName("ngram_analyzer")
                .master("local[*]")
                .config("spark.driver.extraClassPath", "./resources/postgresql-42.5.1.jar")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "1g")
                .getOrCreate()
            )

            # get files from path
            if os.path.isfile(path):
                 data_files.append(path)  # single file
            elif os.path.isdir(path):  # handle directory
                for cur_path, folders, files in os.walk(path):
                    for file in files:
                        data_files.append(os.path.join(cur_path, file))

            # setup transferer
            prop_dict = self.conn_settings
            url = (
                "jdbc:postgresql://"
                + prop_dict["host"]
                + ":"
                + prop_dict["port"]
                + "/"
                + prop_dict["dbname"]
            )
            # store name of database
            properties: Dict[str, str] = {
                "user": prop_dict["user"],
                "password": prop_dict["password"],
            }
            transferer = Transferer(self.spark, url, properties)

            # do transfer for all files in data_files
            for file in data_files:
                transferer.transfer_textFile(file)

        self.__exit()


# python main.py --db-create  # create db and return
# python main.py --transfer  # only do transfer and return
# python main.py --db-create --transfer  # create db, transfer data and return
# python main.py --transfer --db-create  # create db, transfer data and return
#
# python main.py --db-create --transfer --shell # should this be possible???
#
# python main.py # start shell  instead of python main.py --shell