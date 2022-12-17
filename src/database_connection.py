from __future__ import annotations

from dataclasses import dataclass
from types import TracebackType
from typing import Dict, List, Optional, Type, Tuple, Any

from psycopg import sql, OperationalError, Connection
from psycopg.errors import DuplicateDatabase

@dataclass
class Ngram:
    """ TODO """

    def __init__(self):
        pass
        #TODO


class NgramDB:
    """ A wrapper around psycopg classes."""

    def __init__(self, connection_settings: Dict[Any, str]) -> None:
        self.__connection = Connection.connect(
                                                host=connection_settings['host'],
                                                port=connection_settings['port'],
                                                user=connection_settings['user'],
                                                password=connection_settings['password'],
                                                dbname=connection_settings['dbname'],
                                                autocommit=True
                                              )

        self.__create_relations_if_not_exists()

    def __del__(self) -> None:
        if self.__connection and not self.__connection.closed:
            self.__connection.close()

    def __enter__(self) -> NgramDB:
        return self

    @staticmethod
    def __get_sql_cmds(path: str) -> List[str]:
        with open(path, "r") as f:
            cmds = [i.strip() for i in f.read().split(";")]
        return cmds

    def __create_relations_if_not_exists(self) -> None:
        # TODO: check if tables are read properly
        cmds = self.__get_sql_cmds("./src/resources/db_tables.sql")

        with self.__connection.cursor() as cursor:
            for cmd in cmds:
                cursor.execute(cmd)

    def execute(self, query: str, values: Tuple[Any, ...] = (),
                return_result: bool = True) -> List[Tuple[Any, ...]]:
        """ Executes the given query and returns the result if desired.
        Requesting a result on queries that don't return anything causes
        an Exception. """

        with self.__connection.cursor() as cur:
            cur.execute(query, values)
        # TODO add flag to return just fetchone
            if return_result:
                return cur.fetchall()

            return []

class NgramDBBuilder:

    def __init__(self, connection_settings: Dict[str, str]) -> bool:
        self.connection_settings = connection_settings

    # TODO: convert prints to logging info
    def __create_database(self) -> bool:
        con: Connection = None
        try:
            print("Trying to connect")
            con = Connection.connect(
                                host=self.connection_settings['host'],
                                port=self.connection_settings['port'],
                                user=self.connection_settings['user'],
                                password=self.connection_settings['password'],
                                autocommit=True
                                )
            with con.cursor() as cur:
                name: str = self.connection_settings['dbname']
                cur.execute("SELECT 1 \
                            WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname=%s);", (name, ))
                cmd: str = cur.fetchall()
                # if list is not empty, db does not exist so create it
                if cmd:
                    cur.execute(f"CREATE DATABASE {name};")
                    print("Created DB")
            return True
        except OperationalError:
            print('Failed to connect to database. Check login settings.')
            return False
        except DuplicateDatabase:
            print('ProgrammingError: DB duplication.')
            return False
        except:
            print("Unknown DB creation error")
            return False
        finally:
            if con:
                con.close()

    def connect_to_ngram_db(self) -> Optional[NgramDB]:
        isCreated = self.__create_database()
        if not isCreated:
            print("Issue with DB creation")
            return None        
        database: NgramDB = NgramDB(self.connection_settings)
        print("Connection to database established.")
        return database