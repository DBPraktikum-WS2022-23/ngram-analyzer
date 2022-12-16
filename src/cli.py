from cmd import Cmd
from typing import Dict, Optional
from pyspark.sql import SparkSession

from getpass import getpass
from src.database_connection import NgramDB, NgramDBBuilder
from src.config_converter import ConfigConverter
from src.transfer import Transferer

# TO DO: über pfeiltasten vorherigen befehl holen
class Prompt(Cmd):
    intro: str = ('Welcome to the ngram_analyzer shell. Type help or ? to list commands.\n')
    prompt: str = '(ngram_analyzer) '

    #config = ConfigConverter('')

    ngram_db: NgramDB = None
    spark : SparkSession = None  # TODO: init spark session
    transferer: Transferer = None

    def do_db_connect(self, inp):
        # init db
        user: str = input("Enter user name:")
        config = ConfigConverter(user)
        if not config.user_exists:
            password: str = getpass()
            dbname: str = input("Enter database name:")
            config.generate_conn_settings(password, dbname)
        conn_settings = config.get_conn_settings()
        # TODO: this wrapper function might be useless but it appears here more readable to me
        self.ngram_db =  NgramDBBuilder(conn_settings).connect_to_ngram_db()

        if self.ngram_db is None:
            # TODO return to main menu and retry db init with other connection settings
            print("Connection to DB could not be established. Goodbye!")
            return True

        print('Opened connection')
        #
        # Work with the database. For instance:
        result = self.ngram_db.execute('SELECT version()')
        #
        print(f'PostgreSQL database version: {result}')

    def do_transfer(self, path: str) -> None:
        """ Transfer data from a file to the database. """
        if self.ngram_db is None:
            print("No connection to database. Please connect to a database first.")
            return
        
        if path == '':
            print("Please provide a path to a file.")
            return
        
        if self.transferer is None:
            self.transferer = Transferer(self.spark, url, properties)  # TODO: url and properties are not defined
        
        self.transferer.transfer_textFile(path)

    def do_exit(self, inp):
        return True

    # overrides class method, is run before cmdloop returns but not in case the shell crashes
    def postloop(self) -> None:
        del self.ngram_db
        print('Closed connection')




