""" Module for creating the database """
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from psycopg import Connection, OperationalError
from psycopg.errors import DuplicateDatabase

class NgramDBBuilder:
    """Creates the ngram database and its corresponding tables."""

    def __init__(self, connection_settings: Dict[str, str]) -> None:
        self.connection_settings = connection_settings

    def __open_connection(self) -> Any:
        connection = Connection.connect(
            host=self.connection_settings["host"],
            port=self.connection_settings["port"],
            user=self.connection_settings["user"],
            password=self.connection_settings["password"],
            dbname=self.connection_settings["dbname"],
            autocommit=True,
        )
        return connection

    @staticmethod
    def __get_sql_cmds(path: str) -> List[str]:
        with open(path, "r", encoding="UTF-8") as file:
            cmds = [i.strip() for i in file.read().split(";")]
        return cmds

    def __create_relations_if_not_exists(self) -> None:
        connection = self.__open_connection()
        cmds = self.__get_sql_cmds("./src/resources/db_tables.sql")

        with connection.cursor() as cursor:
            for cmd in cmds:
                cursor.execute(cmd)

        connection.close

    def __create_database(self) -> bool:
        con = None
        try:
            con = Connection.connect(
                dbname="postgres",
                host=self.connection_settings["host"],
                port=self.connection_settings["port"],
                user=self.connection_settings["user"],
                password=self.connection_settings["password"],
                autocommit=True,
            )
            with con.cursor() as cur:
                name: str = self.connection_settings["dbname"]
                cur.execute(
                    "SELECT 1 \
                            WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname=%s);",
                    (name,),
                )
                cmd: List[Tuple[Any, ...]] = cur.fetchall()
                # if list is not empty, db does not exist so create it
                if cmd:
                    cur.execute(f"CREATE DATABASE {name};")
                    print("Created DB")
                else:
                    print("DB already exists")
            return True
        except OperationalError:
            print("Failed to create database. Check login settings.")
            return False
        except DuplicateDatabase:
            print("ProgrammingError: DB duplication.")
            return False
        except:
            print("Unknown DB creation error")
            return False
        finally:
            if con:
                con.close()

    def create_ngram_db(self) -> None:
        """Creates the ngram database if it doesn't exist yet."""
        is_created = self.__create_database()
        if not is_created:
            print("Issue with DB creation")
            return
        self.__create_relations_if_not_exists()
