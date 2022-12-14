from cmd import Cmd
from typing import Dict, Optional

from ..database_connection import NgramDB, NgramDBBuilder
from ..config_converter import ConfigConverter

# TO DO: über pfeiltasten vorherigen befehl holen
class Prompt(Cmd):
    intro: str = ('Welcome to the ngram_analyzer shell. Type help or ? to list commands.\n')
    prompt: str = '(ngram_analyzer) '

    config = ConfigConverter('settings/config.ini')

    ngram_db: NgramDB = None

    def do_db_connect(self, inp):
        # init db
        conn_settings = self.config.get_conn_settings()
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

    def do_exit(self, inp):
        return True

    # overrides class method, is run before cmdloop returns but not in case the shell crashes
    def postloop(self) -> None:
        del self.ngram_db
        print('Closed connection')




