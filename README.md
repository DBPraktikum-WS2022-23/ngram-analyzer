# group_4: NGRAM_ANALYZER

## Setup
- clone repo
- cd to ./group4
- init poetry environment ```poetry install```
- enter poetry environment ```python -m poetry shell```

## Interaction via CLI
- the following commands are run inside the poetry shell
- List command options: ```main.py -h```
- Login with your local postgres account: ```main.py --username username --password password --dbname database_name```
    - the name of the database should not be taken already
- Create database: ```main.py --create-db```
- Copy your data files to ./data
- Transfer data from ./data into the database: ```main.py transfer```
- Enter shell version of the CLI: ```main.py --shell```
    - extends CLI options

## Interaction via Shell
- the following commands are available within the ngram_analyzer shell:
- ```help``` or ```?``` shows commands
- ```db_connect``` connects to the database in the config of the current user
    - also creates db and its relations if they don't exist (see /src/resources/db_tables.sql)
    - user is prompted to input username of their local postgres account
    - if no config profile exists for the username, then they are prompted for 
        - password of their postgres and
        - a new database name
- ```transfer``` 
- ```plot_word_frequencies```
- ```print_db_statistics```
- ```print_word_frequencies```
