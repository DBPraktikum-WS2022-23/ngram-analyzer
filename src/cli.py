""" Module which provides the CLI interface for the application """
import argparse
import os
import sys
from typing import Dict, Optional

from src.config_converter import ConfigConverter, ConfigCreator
from src.controller import SparkController
from src.database_creation import NgramDB, NgramDBBuilder
from src.shell import Prompt


class Cli:
    """
    Entry Point to the ngram_analyzer program
    """

    def __init__(self) -> None:
        self.conn_settings: Dict[str, str] = {}
        self.ngram_db: Optional[NgramDB] = None
        self.spark = None

    def __exit(self) -> None:
        if self.ngram_db:
            del self.ngram_db
        if self.spark:
            self.spark.stop()

        sys.exit()

    def __exit_error(self, message) -> None:
        if self.ngram_db:
            del self.ngram_db
        if self.spark:
            self.spark.stop()

        sys.exit(f"Error: {message}.")

    def run_cli(self) -> None:
        """Define and analyze the argument structure."""
        psr = argparse.ArgumentParser(
            prog="main.py",
            description="A program to store ngrams in a database and analyse "
            "them. To start interactive shell call program without parameters",
            epilog="Text at the bottom of help",
        )

        psr.add_argument(
            "-c",
            "--create-db",
            action="store_true",
            help="Creates the database incl. its relations, if not already existing.",
        )

        psr.add_argument(
            "-t",
            "--transfer",
            metavar="PATH",
            help="Reads raw data from supplied file or folder and imports it into"
            " database, use -r to recurse into subdirectories",
        )

        psr.add_argument(
            "-s",
            "--shell",
            action="store_true",
            help="Open shell",
        )

        psr.add_argument(
            "-cp",
            "--config_path",
            metavar="PATH",
            help="Path to the config file",
        )

        psr.add_argument(
            "-u",
            "--username",
            metavar="USER",
            help="Username for the database. Note that the user most be an existing role",
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

        # if no arguments are supplied or user asks for shell, start shell
        if (len(sys.argv) == 1) or args["shell"]:
            Prompt().cmdloop()
            return

        if args["create_db"]:
            if (
                args["username"] is None
                or args["password"] is None
                or args["dbname"] is None
            ) and args["config_path"] is None:
                self.__exit_error(
                    "Missing arguments for database creation. "
                    "Please rerun the program providing a user, a password and a dbname"
                    " or a config file"
                )
            if args["config_path"] is not None:
                conn_settings = ConfigConverter(
                    args["config_path"].split("_")[1].split(".")[0]
                ).get_conn_settings()
            else:
                ConfigCreator(
                    args["username"], args["password"], args["dbname"]
                ).generate_new_conn_settings()
                conn_settings: dict[str, str] = ConfigConverter(
                    args["username"]
                ).get_conn_settings()
            NgramDBBuilder(conn_settings).create_ngram_db()

        if args["transfer"] is not None:
            if (
                args["username"] is None
                or args["password"] is None
                or args["dbname"] is None
            ) and (args["config_path"] is None):
                self.__exit_error(
                    "Missing arguments for transfer. "
                    "Please rerun the program providing a user, a password and a dbname"
                    " or a config file"
                )
            path = os.path.abspath(args["transfer"])

            if not os.path.exists(path):
                self.__exit_error(f"path '{path}' does not exist")

            data_files = []  # list of files to process

            # get files from path
            if os.path.isfile(path):
                data_files.append(path)  # single file
            elif os.path.isdir(path):  # handle directory
                for cur_path, _, files in os.walk(path):
                    for file in files:
                        data_files.append(os.path.join(cur_path, file))

            # use SparkController to transfer files
            if args["config_path"] is not None:
                conn_settings = ConfigConverter(
                    args["config_path"].split("_")[1].split(".")[0]
                ).get_conn_settings()
            else:
                ConfigCreator(
                    args["username"], args["password"], args["dbname"]
                ).generate_new_conn_settings()
                conn_settings: dict[str, str] = ConfigConverter(  # type: ignore
                    args["username"]
                ).get_conn_settings()
            spark_controller: SparkController = SparkController(conn_settings)
            spark_controller.transfer(data_files)

        self.__exit()
