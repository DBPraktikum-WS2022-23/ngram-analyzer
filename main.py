from configparser import ConfigParser
from typing import Dict, Any, Optional
from src.database_connection import NgramDBBuilder, NgramDB

def get_conn_settings(config: ConfigParser) -> Dict[str, str]:
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
    return connection_settings

def connect_to_db(conn_settings: Dict[str, str]) -> Optional[NgramDB]:    
    # Create DB, add tables, open connection:
    ngram_db: NgramDB = NgramDBBuilder(conn_settings).connect_to_ngram_db()
    return ngram_db

def main():
    # DB init
    config = ConfigParser()
    config.read('settings/config.ini')

    # init db
    conn_settings = get_conn_settings()
    # TODO: this wrapper function might be useless but it appears here more readable to me
    ngram_db = connect_to_db(conn_settings)

    if ngram_db is None:
        # TODO return to main menu and retry db init with other connection settings
        print("Connection to DB could not be established. Goodbye!")
        exit()

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
