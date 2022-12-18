import argparse
import os
import sys
import time

from src.config_converter import ConfigConverter
from src.database_connection import NgramDB, NgramDBBuilder
from src.transfer import Transferer

from getpass import getpass
from typing import Dict, Optional

from pyspark.sql import SparkSession







class Cli:

    def __init__(self) -> None:
        # TODO might be redundant
        self.conn_settings: Dict[str, str] = {}
        self.config: Optional[ConfigConverter] = None
        self.ngram_db: Optional[NgramDB] = None
        self.spark = (
            SparkSession.builder.appName("ngram_analyzer")
            .master("local[*]")
            .config("spark.driver.extraClassPath", "./resources/postgresql-42.5.1.jar")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "1g")
            .getOrCreate()
        )

    def __exit(self) -> None:

        # TODO: close db connection
        if self.ngram_db:
            del self.ngram_db
        self.spark.stop()

        sys.exit()

    def __exit_error(self, message) -> None:

        # TODO: close db connection
        if self.ngram_db:
            del self.ngram_db
        self.spark.stop()

        sys.exit(f"Error: {message}.")


    def run_cli(self) -> None:
        psr = argparse.ArgumentParser(
            prog="ngram-analyzer",
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
            "-r",
            "--recursive",
            action="store_true",
            help="Recurse into subdirectories, needs to be called with -t",
        )
        psr.add_argument(
            "-u",
            "--username",
            metavar="USER",
            help="Username for the database",
        )
        # psr.add_argument(
        #     "-p",
        #     "--password",
        #     metavar="PASSWORD",
        #     help="Password for the database",
        # )
        # psr.add_argument(
        #     "-d",
        #     "--database",
        #     metavar="DB_NAME",
        #     help="Name of the database",
        # )


        # psr.add_argument('--version')

        # subparsers = psr.add_subparsers(
        #     title="Sub-commands",
        #     dest="subcmd",
        #     help="The sub-commands",
        #     metavar="[<command> [<args>]]",
        # )
        # psr_shell = subparsers.add_parser("shell", help="Start interactive shell")
        # psr_stats = subparsers.add_parser(
        #     "stats", help="Print statistics about the database"
        # )
        # psr_query = subparsers.add_parser("query", help="Query the database")
        # psr_plot = subparsers.add_parser("plot", help="Create a plot")

        # psr_stats.add_argument(
        #     "stat_type",
        #     choices=["word_count", "occurs_count"],
        #     help="Print statistics about the database",
        # )
        # stat_group = psr_stats.add_mutually_exclusive_group(required=True)
        # stat_group.add_argument('word_count', required=False, help='Size of word table')
        # stat_group.add_argument('occurs_count', required=False, help='Size of occusr_in table')

        # psr_query.add_argument("word", help="A word or phrase to look up")
        # psr_query.add_argument(
        #     "-y",
        #     "--year",
        #     help="Years to look up, comma  separated, ranges possible, e.g. '1970,1975' or 1970,1980-1985",
        # )

        # psr_plot.add_argument("word", help="Word for which to create the plot")
        # psr_plot.add_argument(
        #     "-o", "--output", metavar="PATH", help="Specify output path for the plot"
        # )

        # TODO
        # - [ ] add arguments for:
        #     + [ ] username
        #     + [ ] password
        #     + [ ] database name
        # - [ ] flag options overwrite config values

        args = vars(psr.parse_args())

        print("Command line arguments are:")  # TODO remove later
        print(args)  # TODO remove later
        print("")  # TODO remove later

        # load config
        # config = ConfigConverter(args['config'])  # TODO: need to change ConfigConverter

        # TODO: overwrite username if provided
        # TODO: overwrite password if provided
        # TODO: overwrite dbname if provided
        # config.generate_conn_settings(password, dbname)
        # conn_settings = config.get_conn_settings()

        # TODO: this wrapper function might be useless but it appears here more readable to me
        # ngram_db = NgramDBBuilder(conn_settings).connect_to_ngram_db()

        # if ngram_db is None:
        #    __exit_error("connection to DB could not be established.")

        # TODO: connect to db




        if args["create_db"] or args["transfer"]:

            # check config and open connection
            if not args['username']:
                self.__exit_error("no username provided")
            config = ConfigConverter(args['username'])
            if not config.user_exists:
                self.__exit_error("config file for user '%s' does not exist"%(args['username']))

            # TODO: [optional] add option to overwrite username and password

            conn_settings = config.get_conn_settings()
            ngram_db = NgramDBBuilder(conn_settings).connect_to_ngram_db()
            if ngram_db is None:
                self.__exit_error("connection to DB could not be established")

        else:
            # continue to shell
            return


        if args["create_db"]:
            print("creating db")  # TODO remove later

            # TODO: create db
            # TODO: problem, this is done by default by NgramDB



        if args["transfer"]:
            path = os.path.abspath(args["transfer"])

            if not os.path.exists(path):
                self.__exit_error(f"path '{path}' does not exist")

            data_files = []  # list of files to process

            # get files from path
            if os.path.isfile(path):
                data_files.append(path)  # single file
            else:  # handle directory
                for cur_path, folders, files in os.walk(path):
                    for file in files:
                        data_files.append(os.path.join(cur_path, file))
                    if not args['recurse']:
                        break  # dont recurse

            # TODO: check if db exists

            # setup transferer
            prop_dict = conn_settings
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




    #    if not args["subcmd"]:
    #        __exit() # do nothing
    #    elif args["subcmd"] == "shell":
    #        print("starting shell")  # TODO remove later
    #        return  # back to main to start shell
    #
    #    if args["subcmd"] == "stats":
    #        print("printing stats about", args["stat_type"])  # TODO remove later
    #
    #        # TODO: execute stats
    #
    #        __exit()
    #
    #    elif args["subcmd"] == "query":
    #        print(
    #            "printing query results about", args["word"], end=" "
    #        )  # TODO remove later
    #        if args["year"]:
    #            print("in years", args["year"])
    #        else:
    #            print("in all years")
    #
    #        # TODO: execute query
    #        # Ausgabe zwilenweise: year count
    #
    #        __exit()
    #
    #    elif args["subcmd"] == "plot":
    #
    #        out_file = args["output"]
    #
    #        if out_file:
    #            if os.path.isdir(out_file):
    #                out_file = os.path.join(out_file, time.strftime("%Y-%m-%d_%H%M%S.png"))
    #        else:
    #            out_file = os.path.join(os.getcwd(), time.strftime("%Y-%m-%d_%H%M%S.png"))
    #
    #        print(
    #            "plotting nice graphic about", args["word"], "to", out_file
    #        )  # TODO remove later
    #
    #        # TODO: create plot and save to out_file
    #
    #        __exit()











# python main.py --db-create  # create db and return
# python main.py --transfer  # only do transfer and return
# python main.py --db-create --transfer  # create db, transfer data and return
# python main.py --transfer --db-create  # create db, transfer data and return
#
# python main.py --db-create --transfer --shell # should this be possible???
#
# python main.py # start shell  instead of python main.py --shell