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

    #TODO might be redundant
    conn_settings: Dict[str, str] = {}
    config: Optional[ConfigConverter] = None
    ngram_db: Optional[NgramDB] = None

    spark = SparkSession.builder.appName("ngram_analyzer").master("local[*]") \
        .config("spark.driver.extraClassPath", "./resources/postgresql-42.5.1.jar") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    transferer: Optional[Transferer] = None

    def do_db_connect(self, inp):
        # init db
        user: str = input("Enter user name:")
        self.config = ConfigConverter(user)
        if not self.config.user_exists:
            password: str = getpass()
            dbname: str = input("Enter database name:")
            self.config.generate_conn_settings(password, dbname)
        self.conn_settings = self.config.get_conn_settings()
        # TODO: this wrapper function might be useless but it appears here more readable to me
        self.ngram_db =  NgramDBBuilder(self.conn_settings).connect_to_ngram_db()

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
            prop_dict = self.conn_settings
            url = 'jdbc:postgresql://' + prop_dict["host"] + ':' + prop_dict["port"] \
                  + '/ngram_db'
            #TODO store name of database
            print(url)
            properties: Dict[str, str] = {'user': prop_dict['user'],
                                          'password': prop_dict['password']}
            self.transferer = Transferer(self.spark, url, properties)

        if path == "--":
            self.transferer.transfer_textFile(prop_dict['default_filepath'])
        else:
            self.transferer.transfer_textFile(path)

    # TODO error handling, be careful to use this
    def do_set_default_file(self, path: str) -> None:
        if self.ngram_db is None:
            print("No connection to database. Please connect to a database first.")
            return

        path = input("Enter default file path:")
        self.config.set_default_path(path)
        self.conn_settings = self.config.get_conn_settings()

    def do_exit(self, inp):
        return True

    # overrides class method, is run before cmdloop returns but not in case the shell crashes
    def postloop(self) -> None:
        if self.ngram_db:
            del self.ngram_db
        self.spark.stop()
        print('Closed connection')




