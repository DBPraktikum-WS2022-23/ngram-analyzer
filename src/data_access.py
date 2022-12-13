from __future__ import annotations

from dataclasses import dataclass
from types import TracebackType
from typing import Dict, List, Optional, Type, Tuple, Any

from psycopg import Connection

@dataclass
class Ngram:
    """ Alias for Tuple[Document, Dict[Ngram, int]]. """

    def __init__(self):
        pass
        #TODO


class NgramDB:
    """ A wrapper around psycopg classes, specifically designed for our
    purpose. """

    def __init__(self, connection_string: str, schema: str) -> None:
        self.__connection = Connection.connect(
                                              connection_string,
                                              options=f'-c search_path={schema}',
                                              autocommit=True)

        self.__create_if_not_exists()

    def __del__(self) -> None:
        if self.__connection and not self.__connection.closed:
            self.__connection.close()

    def __enter__(self) -> NgramDB:
        return self

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        self.__del__()

    @staticmethod
    def __get_sql_cmds(path: str) -> List[str]:
        with open(path, "r") as f:
            cmds = [i.strip() for i in f.read().split(";")]
        return cmds

    def __create_if_not_exists(self) -> None:
        cmds = self.__get_sql_cmds("./sql-scripts/db_relations.sql")

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

            if return_result:
                return cur.fetchall()

            return []