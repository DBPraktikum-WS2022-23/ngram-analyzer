import os.path
from configparser import ConfigParser
from typing import Dict


class ConfigConverter:
    def generate_config(
        self, username: str, password: str, dbname: str, host: str, port: str
    ) -> None:
        config = ConfigParser()
        config.set("database", "host", host)
        config.set("database", "port", port)
        config.set("database", "user", username)
        config.set("database", "password", password)
        config.set("database", "dbname", dbname)
        jdbc_url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname
        config.set("jdbc", "url", jdbc_url)
        # TODO: add flexible raw data path
        config.set("data", "path", "data/")
        with open("./settings/config_" + username + ".ini", "w") as configfile:
            config.write(configfile)

    def get_jdbc_url(self) -> None:
        pass

    def get_conn_settings(self) -> Dict[str, str]:
        # convert list of tuples to dict
        connection_settings: Dict[str, str] = {}
        if self.config.has_section("database"):
            params = self.config.items("database")
            for key, value in params:
                if key == "schema":
                    connection_settings["options"] = f"-c search_path={value}"
                else:
                    connection_settings[key] = value
        return connection_settings

    def set_default_path(self, path: str) -> None:
        self.config.set("database", "default_filepath", path)
        with open("./settings/config_" + self.username + ".ini", "w") as configfile:
            self.config.write(configfile)


class DBProfile:
    def __init__(
        self,
        username: str = "postgres",
        password: str = "1234",
        dbname: str = "ngram_db",
        host: str = "localhost",
        port: str = "5432",
    ) -> None:
        self.config_path = "./settings/config_" + username + ".ini"
        if not os.path.exists(self.config_path):
            self.config = ConfigConverter().generate_config(
                username, password, dbname, host, port
            )
