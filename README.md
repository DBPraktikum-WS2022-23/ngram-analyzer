# group_4: NGRAM_ANALYZER

## Setup
- clone repo
- cd to ./group4
- init poetry environment ```poetry install```
- enter poetry environment ```python -m poetry shell```

## Interaction via CLI
- the following commands are run inside the poetry shell
- List command options: ```main.py -h```
- Create database: ```main.py --create-db --username username --password password --dbname database_name```
- Transfer data from ./data into the database: ```main.py --transfer path_to_data --username username --password password --dbname database_name```
- Enter shell version of the CLI: ```main.py --shell``` oder ```main.py```

## Interaction via Shell
- the following commands are available within the ngram_analyzer shell:
- ```help``` or ```?``` shows commands
- ```sql``` opens a sql shell
- ```plot_word_frequencies``` plotting frequency of words against each other for a set of years
- ```print_db_statistics``` prints count for each table, highest frequency and number of years
- ```print_word_frequencies``` prints a table of the word frequencies in different years for different words
    - user is prompted to give the words and years
