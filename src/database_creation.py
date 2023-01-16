""" Module for creating the database """
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from psycopg import Connection, OperationalError
from psycopg.errors import DuplicateDatabase


class NgramDBBuilder:
    """Creates the ngram database and its corresponding tables."""

    def __init__(self, connection_settings: Dict[str, str]) -> None:
        self.connection_settings = connection_settings

    def __open_connection(self, dbname:str = "postgres") -> Any:
        try:
            connection = Connection.connect(
                host=self.connection_settings["host"],
                port=self.connection_settings["port"],
                user=self.connection_settings["user"],
                password=self.connection_settings["password"],
                dbname=dbname,
                autocommit=True,
            )
        except OperationalError:
            print("Failed to connect to PostgreSQL database. Check login details.")
            return None

        return connection

    @staticmethod
    def __get_sql_cmds(path: str) -> List[str]:
        with open(path, "r", encoding="UTF-8") as file:
            cmds = [i.strip() for i in file.read().split(";")]
        return cmds

    def __create_relations_if_not_exists(self) -> None:
        connection = self.__open_connection(self.connection_settings["dbname"])
        cmds = self.__get_sql_cmds("./src/resources/db_tables.sql")

        with connection.cursor() as cursor:
            for cmd in cmds:
                cursor.execute(cmd)

        connection.close()

    def exists_db(self) -> bool:
        db_name: str = self.connection_settings["dbname"]

        con = self.__open_connection(dbname="postgres")
        if con is None:
            return False

        with con.cursor() as cur:
            cur.execute(
                "SELECT 1 \
                        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname=%s);",
                (db_name,),
            )
            cmd: List[Tuple[Any, ...]] = cur.fetchall()
        con.close()

        if cmd:
            return False
        else:
            return True


    def __create_database(self) -> bool:
        name: str = self.connection_settings["dbname"]

        con = self.__open_connection(dbname="postgres")
        if con is None:
            return False

        try:
            with con.cursor() as cur:
                cur.execute(f"CREATE DATABASE {name};")
                print(f"Created {name} database.")
            return True
        except DuplicateDatabase:
            print("ProgrammingError: DB duplication.")
            return False
        except:
            print("Unknown DB creation error.")
            return False
        finally:
            if con:
                con.close()

    def create_ngram_db(self) -> None:
        """Creates the ngram database if it doesn't exist yet."""
        if self.exists_db():
            name = self.connection_settings["dbname"]
            print(f"{name} database already exists.")
            return
        is_created = self.__create_database()
        if not is_created:
            print("Issue with DB creation")
            return
        self.__create_relations_if_not_exists()
