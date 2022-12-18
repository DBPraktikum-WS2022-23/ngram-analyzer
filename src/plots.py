import matplotlib.pyplot as plt
from typing import List
from pyspark.sql import SparkSession, DataFrame

def plot_word_frequencies(words, years) -> None:
    """ Plot the frequency of certain words in certain years"""
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL") \
        .config("spark.jars", "/resources/postgresql-42.5.1.jar") \
        .getOrCreate()

    df_word = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/ngram_db") \
        .option("dbtable", "word") \
        .option("user", "postgres") \
        .option("password", "abcd1234") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df_occurence = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/ngram_db") \
        .option("dbtable", "occurence") \
        .option("user", "postgres") \
        .option("password", "abcd1234") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # create a spark dataframe with the words and years
    string_representations: List[str] = df_word.filter(df_word.str_rep.isin(words)).select("id", "str_rep").distinct().collect()

    # plot the frequency of the words in the years into a single line plot
    fig, ax = plt.subplots()
    ax.set_title("Frequency of words in years")
    ax.set_xlabel("year")
    ax.set_xticks(range(min(years), max(years)+1))
    ax.set_ylabel("frequency")

    for row in string_representations:
        df = df_occurence.filter(df_occurence.id == row.id)
        df = df.filter(df_occurence.year.isin(years))
        ax.scatter(df.select("year").collect(), df.select("freq").collect(), label=row.str_rep)

    ax.legend()
    plt.show()
    spark.stop()
    plt.savefig("output/word_frequency_plot.png")
