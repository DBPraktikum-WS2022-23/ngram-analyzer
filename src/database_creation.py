""" Module for creating the database """
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from psycopg import Connection, OperationalError
from psycopg.errors import DuplicateDatabase


class NgramDB:
    """A wrapper around psycopg classes. Based on NgramDocumentDB from doc2ngram."""

    def __init__(self, connection_settings: Dict[Any, str]) -> None:
        self.__connection = Connection.connect(
            host=connection_settings["host"],
            port=connection_settings["port"],
            user=connection_settings["user"],
            password=connection_settings["password"],
            dbname=connection_settings["dbname"],
            autocommit=True,
        )

        self.__create_relations_if_not_exists()

    def __del__(self) -> None:
        if self.__connection and not self.__connection.closed:
            self.__connection.close()

    def __enter__(self) -> NgramDB:
        return self

    @staticmethod
    def __get_sql_cmds(path: str) -> List[str]:
        with open(path, "r", encoding="UTF-8") as file:
            cmds = [i.strip() for i in file.read().split(";")]
        return cmds

    def __create_relations_if_not_exists(self) -> None:
        cmds = self.__get_sql_cmds("./src/resources/db_tables.sql")

        with self.__connection.cursor() as cursor:
            for cmd in cmds:
                cursor.execute(cmd)

    def execute(
        self, query: str, values: Tuple[Any, ...] = (), return_result: bool = True
    ) -> List[Tuple[Any, ...]]:
        """Executes the given query and returns the result if desired.
        Requesting a result on queries that don't return anything causes
        an Exception."""

        with self.__connection.cursor() as cur:
            cur.execute(query, values)
            # TODO add flag to return just fetchone
            if return_result:
                return cur.fetchall()

            return []


class NgramDBBuilder:
    """Creates the ngram database and opens connection to it."""

    def __init__(self, connection_settings: Dict[str, str]) -> None:
        self.connection_settings = connection_settings

    # TODO: convert prints to logging info
    def __create_database(self) -> bool:
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
