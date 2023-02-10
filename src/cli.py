""" Module which provides the CLI interface for the application """
import argparse
import os
import sys
from typing import Dict, Optional

from src.config_converter import ConfigConverter, ConfigCreator
from src.controller import SparkController, PluginController
from src.database_creation import NgramDBBuilder
from src.shell import Prompt


class Cli:
    """
    Entry Point to the ngram_analyzer program
    """

    def __init__(self) -> None:
        self.conn_settings: Dict[str, str] = {}
        self.spark_controller: SparkController = None

    def __exit(self) -> None:
        if self.spark_controller:
            self.spark_controller.get_spark_session().stop()

        sys.exit()

    def __exit_error(self, message) -> None:
        if self.spark_controller:
            self.spark_controller.get_spark_session().stop()

        sys.exit(f"Error: {message}.")

    def run_cli(self) -> None:
        """Execute the program according to the passed arguments."""

        args: dict = self.__parse_args()

        # if no arguments are supplied or user asks for shell, start shell
        if (len(sys.argv) == 1) or args["shell"]:
            Prompt().cmdloop()
            return

        if args["create_db"]:
            self.__create_db(args)
            return

        if args["transfer"]:
            self.__transfer_data(args)
            return


        self.__exit()

    def __parse_args(self) -> dict:
        """Parse the arguments given to the program. Returns a dictionary with the arguments."""
        psr = argparse.ArgumentParser(
        prog="main.py",
        description="A program to store ngrams in a database and analyse "
        "them. To start interactive shell call program without parameters"
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
            action="store_true",
            help="Reads raw data from supplied file or folder via --defefault_path "
            "or from the default path (found in user configuration) "
            "and imports it into database, directories are processed recursively.",
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
            "-dp",
            "--data_path",
            metavar="PATH",
            help="Path to the data folder",
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

        psr.add_argument(
            "-pp",
            "--plugin_path",
            help="Path to the plugin folder, default: plugins",
        )

        return vars(psr.parse_args())

    def __create_db(self, args) -> None:
        """Creates the database incl. its relations, if not already existing."""
        self.__set_config_information(args)
        NgramDBBuilder(self.conn_settings).create_ngram_db()  # create database

        return


    def __transfer_data(self, args) -> None:
        """Reads raw data from supplied file or folder into database"""
        self.__set_config_information(args)

        # no path has been given so use the default path from the sample configuration
        if args["data_path"] is not None:
            path = os.path.abspath(args["data_path"])
            if not os.path.exists(path):
                self.__exit_error(f"data path '{path}' does not exist")
        else:
            path = "./data"

        # check if db with db_name exists (necessary because a new configuration can be generated above)
        db_builder = NgramDBBuilder(self.conn_settings)
        if not db_builder.exists_db():
            self.__exit_error(f"Database {args['dbname']} does not exist. Please use the --db_create option")

        data_files = []  # list of files to process

        # get files from path
        if os.path.isfile(path):
            data_files.append(path)  # single file
        elif os.path.isdir(path):  # handle directory
            for cur_path, _, files in os.walk(path):
                for file in files:
                    data_files.append(os.path.join(cur_path, file))

        spark_controller: SparkController = SparkController(self.conn_settings)
        spark_controller.transfer(data_files)

        return

    def __check_for_config_information(self, args) -> None:
        if (
                args["username"] is None
                or args["password"] is None
                or args["dbname"] is None
        ) and args["config_path"] is None:
            self.__exit_error(
                "Missing arguments. "
                "Please rerun the program providing a user, a password and a dbname"
                " or a config file"
            )

        return

    def __set_config_information(self, args) -> None:
        """ Set the config information from the arguments """
        self.__check_for_config_information(args)

        if args["config_path"] is not None:
            conn_settings = ConfigConverter(
                args["config_path"]
            ).get_conn_settings()
        else:
            config_path = ConfigCreator(
                args["username"], args["password"], args["dbname"]
            ).generate_new_config()
            conn_settings = ConfigConverter(
                config_path
            ).get_conn_settings()

        self.conn_settings = conn_settings
