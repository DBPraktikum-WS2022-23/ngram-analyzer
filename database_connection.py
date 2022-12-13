from src.data_access import NgramDB

connection_string: str = 'host=localhost port=5432 dbname=ngram_db connect_timeout=10'
schema: str = 'public'
database: NgramDB = NgramDB(connection_string, schema)
print("Connection to database established.")