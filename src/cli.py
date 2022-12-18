import argparse
import os
import sys
import time

from src.config_converter import ConfigConverter
from src.database_connection import NgramDB, NgramDBBuilder
from src.transfer import Transferer


def run_cli() -> None:
    psr = argparse.ArgumentParser(
        prog="ngram-analyzer",
        description="A program to store ngrams in a database and analyse them.",
        epilog="Text at the bottom of help",
    )

    psr.add_argument(
        "-c", "--config", metavar="FILE", help="Path to the config file"
    )  # option that takes a value
    psr.add_argument(
        "--create-db",
        action="store_true",
        help="Creates necessary database relations if not existing",
    )
    psr.add_argument(
        "-t",
        "--transfer",
        metavar="PATH",
        help="Reads raw data from supplyed file or folder and imports it into database, use -r to recurse into subdirectories",
    )
    psr.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Recurse into subdirectories, needs to be called with -t",
    )
    # psr.add_argument('--version')

    subparsers = psr.add_subparsers(
        title="Sub-commands",
        dest="subcmd",
        help="The sub-commands",
        metavar="[<command> [<args>]]",
    )
    psr_shell = subparsers.add_parser("shell", help="Start interactive shell (default)")
    psr_stats = subparsers.add_parser(
        "stats", help="Print statistics about the database"
    )
    psr_query = subparsers.add_parser("query", help="Query the database")
    psr_plot = subparsers.add_parser("plot", help="Create a plot")

    psr_stats.add_argument(
        "stat_type",
        choices=["word_count", "occurs_count"],
        help="Print statistics about the database",
    )
    # stat_group = psr_stats.add_mutually_exclusive_group(required=True)
    # stat_group.add_argument('word_count', required=False, help='Size of word table')
    # stat_group.add_argument('occurs_count', required=False, help='Size of occusr_in table')

    psr_query.add_argument("word", help="A word or phrase to look up")
    psr_query.add_argument(
        "-y",
        "--year",
        help="Years to look up, comma  separated, ranges possible, e.g. '1970,1975' or 1970,1980-1985",
    )

    psr_plot.add_argument("word", help="Word for which to create the plot")
    psr_plot.add_argument(
        "-o", "--output", metavar="PATH", help="Specify output path for the plot"
    )

    # TODO
    # - [ ] add arguments for:
    #     + [ ] username
    #     + [ ] password
    #     + [ ] database name
    # - [ ] flag options overwrite config values

    args = vars(psr.parse_args())

    print(args)  # TODO remove later

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
    #    sys.exit("Error, connection to DB could not be established.")

    # TODO: connect to db

    if args["create_db"]:
        print("creating db")  # TODO remove later

        # TODO: create db

    if args["transfer"]:
        path = os.path.abspath(args["transfer"])
        data_files = []  # list of files to process

        # get files from path
        if os.path.isfile(path):
            data_files.append(path)  # single file
        else:
            for cur_path, folders, files in os.walk(path):
                for file in files:
                    data_files.append(os.path.join(cur_path, file))
                if not recurse:
                    break  # dont recurse

        # TODO: do transfer for all files in data_files

    if not (args["subcmd"]) or (args["subcmd"] == "shell"):
        print("starting shell")  # TODO remove later
        return  # back to main to start shell

    if args["subcmd"] == "stats":
        print("printing stats about", args["stat_type"])  # TODO remove later

        # TODO: execute stats

        sys.exit()

    elif args["subcmd"] == "query":
        print(
            "printing query results about", args["word"], end=" "
        )  # TODO remove later
        if args["year"]:
            print("in years", args["year"])
        else:
            print("in all years")

        # TODO: execute query
        # Ausgabe zwilenweise: year count

        sys.exit()

    elif args["subcmd"] == "plot":

        out_file = args["output"]

        if out_file:
            if os.path.isdir(out_file):
                out_file = os.path.join(out_file, time.strftime("%Y-%m-%d_%H%M%S.png"))
        else:
            out_file = os.path.join(os.getcwd(), time.strftime("%Y-%m-%d_%H%M%S.png"))

        print(
            "plotting nice graphic about", args["word"], "to", out_file
        )  # TODO remove later

        # TODO: create plot and save to out_file

        sys.exit()
