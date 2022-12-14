from configparser import ConfigParser
from typing import Dict

class ConfigConverter():
    def __init__(self, config_path: str) -> None:
        self.config = ConfigParser()
        self.config.read(config_path)

    def get_conn_settings(self) -> Dict[str, str]:
        # convert list of tuples to dict
        connection_settings: Dict[str, str] = {}
        if self.config.has_section('database'):
            # TODO upon connection. prompt user to input username, password and dbname and store them in config.ini
            params = self.config.items('database')
            for key, value in params:
                if key == 'schema':
                    connection_settings['options'] = f'-c search_path={value}'
                else:
                    connection_settings[key] = value
        return connection_settings

    # TODO: get JDBC settings