""" Module for creating and reading out configs"""
import os.path
from configparser import ConfigParser
from typing import Dict


class ConfigCreator:
    """
    Creates a config file.
    """

    def __init__(
        self,
        username: str,
        password: str,
        dbname: str,
        template_path: str = "./settings/config_sample.ini",
    ) -> None:
        self.username: str = username
        self.password: str = password
        self.dbname: str = dbname

        if os.path.isfile(template_path):
            self.config = ConfigParser()
            self.config.read(template_path)
        else:
            print("Invalid path to template file")
            return

    def generate_new_conn_settings(self) -> Dict[str, str]:
        """Generates a new config file with the given connection settings.
        Returns a dictionary with the newly generated settings."""
        fpath: str = "./settings/config_" + self.username + ".ini"
        if os.path.isfile(fpath):
            print("Configuration for this user already exists.")
            return ConfigConverter(fpath).get_conn_settings()

        self.config.set("database", "user", self.username)
        self.config.set("database", "password", self.password)
        self.config.set("database", "dbname", self.dbname)

        with open(fpath, "w", encoding="UTF-8") as configfile:
            self.config.write(configfile)

        print("Config file created.")
        return ConfigConverter(fpath).get_conn_settings()

class ConfigConverter:
    """Wrapper around ConfigParser. Used to write and read config files for different users."""

    def __init__(self, path: str) -> None:
        config_path = path
        default_path = "./settings/config_sample.ini"
        self.config = ConfigParser()
        self.user_exists = False
        if os.path.isfile(config_path):
            self.config.read(config_path)
            self.user_exists = True
        else:
            print(f"Config file '{path}' does not exist.")
            # TODO: message "create a new user" necessary here?
            self.config.read(default_path)

    def get_conn_settings(self) -> Dict[str, str]:
        """Returns a dictionary with the connection settings for the database."""
        # convert list of tuples to dict
        connection_settings: Dict[str, str] = {}
        if self.config.has_section("database"):
            params = self.config.items("database")
            for key, value in params:
                if key == "schema":
                    connection_settings["options"] = f"-c search_path={value}"
                else:
                    connection_settings[key] = value

        connection_settings["db_url"] = self.get_db_url()
        connection_settings["jdbc_driver"] = self.get_jdbc_path()
        connection_settings["data_path"] = self.get_data_path()

        return connection_settings

    def get_jdbc_path(self) -> str:
        """Returns the path to the jdbc driver."""
        if self.config.has_section("jdbc"):
            params = self.config.items("jdbc")
            for key, value in params:
                if key == "driver":
                    return value
        return ""

    def get_data_path(self) -> str:
        """Returns the path to the data folder."""
        if self.config.has_section("data"):
            params = self.config.items("data")
            for key, value in params:
                if key == "path":
                    return value
        return ""

    def get_db_url(self) -> str:
        """Returns the database url."""
        if self.config.has_section("database"):
            params = self.config.items("database")
            for key, value in params:
                if key == "host":
                    host = value
                elif key == "port":
                    port = value
                elif key == "dbname":
                    dbname = value
            return f"jdbc:postgresql://{host}:{port}/{dbname}"
        return ""
