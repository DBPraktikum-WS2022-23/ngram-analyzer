import psycopg
from psycopg import sql
from typing import List, Any

from src.data_access import NgramDB

class DatabaseConnection:
    def __init(self):
        pass

    def __create_database(name: str = 'ngram_db') -> None:
        con = psycopg.connect(
                            user='postgres',
                            host='localhost',
                            password='1234',
                            autocommit=True
                            )

        with con.cursor() as cur:
            # cur.execute(sql.SQL("CREATE DATABASE {};").format(
            #         sql.Identifier('test'))
            #     )
            cur.execute(f"SELECT 'CREATE DATABASE {name}' \
                        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='{name}');")
            cmd: List[Any] = cur.fetchall()
            if cmd == []:
                return
            cur.execute(cmd[0][0])

        con.close()

    def connect_to_db(self, connection_string: str = 'host=localhost \
                                                port=5432 \
                                                dbname=ngram_db \
                                                connect_timeout=10', 
                        schema:str ='public'
                        ) -> NgramDB:
        self.__create_database() # if not exists
        database: NgramDB = NgramDB(connection_string, schema)
        print("Connection to database established.")
        return database


