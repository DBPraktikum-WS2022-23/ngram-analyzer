from configparser import ConfigParser
from typing import Dict
from getpass import getpass
import os.path


class ConfigConverter:
    def __init__(self, username: str) -> None:
        config_path = "./settings/config_" + username + ".ini"
        default_path = "./settings/config_sample.ini"
        self.config = ConfigParser()
        self.on_generate = False
        if os.path.exists(config_path):
            self.config.read(config_path)
        else:
            print("Configuration for user not exists, create a new user")
            self.config.read(default_path)
            self.on_generate = True

    # https://stackoverflow.com/questions/9202224/getting-a-hidden-password-input
    # The explains for the warning while input password
    def generate_conn_settings(self) -> None:
        user: str = input("Enter user name:")
        password: str = getpass()
        dbname: str = input("Enter database name:")
        self.config.set('database', 'user', user)
        self.config.set('database', 'password', password)
        self.config.set('database', 'dbname', dbname)
        with open("./settings/config_" + user + ".ini", 'w') as configfile:
            self.config.write(configfile)
            #self.config.read("../settings/config_" + user + ".ini")

    def get_conn_settings(self) -> Dict[str, str]:
        # convert list of tuples to dict
        connection_settings: Dict[str, str] = {}
        if self.config.has_section('database'):
            if self.on_generate:
                self.generate_conn_settings()
            # TODO upon connection. prompt user to input username, password and dbname and store them in config.ini
            params = self.config.items('database')
            for key, value in params:
                if key == 'schema':
                    connection_settings['options'] = f'-c search_path={value}'
                else:
                    connection_settings[key] = value


        return connection_settings

    # TODO: get JDBC settings