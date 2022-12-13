import configparser
from typing import Dict, Any
from src.database_connection import NgramDBBuilder, NgramDB

def main():
    # DB init
    config = configparser.ConfigParser()
    config.read('settings/config.ini')

    # convert list of tuples to dict
    connection_settings: Dict[str, str] = {}
    if config.has_section('database'):
        # TODO upon connection. prompt user to input username, password and dbname and store them in config.ini
        params = config.items('database')
        for key, value in params:
            if key == 'schema':
                connection_settings['options'] = f'-c search_path={value}'
            else:
                connection_settings[key] = value
    
    # Create DB, add tables, open connection:
    ngram_db: NgramDB = NgramDBBuilder(connection_settings).connect_to_ngram_db()
    print('Opened connection')
    #
    # Work with the database here. For instance:
    result = ngram_db.execute('SELECT version()')
    #
    print(f'PostgreSQL database version: {result}')

    del ngram_db
    print('Closed connection')


if __name__ == '__main__':
    main()
