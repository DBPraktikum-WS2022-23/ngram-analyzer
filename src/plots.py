import matplotlib.pyplot as plt
from typing import Dict
from pyspark.sql import SparkSession, DataFrame

# write a basic function to plot example data
def plot_example_data() -> None:
    x = [1, 2, 3, 4, 5]
    y = [1, 4, 9, 16, 25]
    plt.plot(x, y)
    plt.show()

# plot the frequency of certain words in certain years
def plot_word_frequencies(words, years) -> None:
    spark = SparkSession.builder.master("local[1]")\
        .appName("test")\
        .getOrCreate()

    df = spark.read.csv("data/test_data.csv", header=True, inferSchema=True)

    # plot the frequency of the words in the years into a single line plot
    fig, ax = plt.subplots()
    ax.set_title("Frequency of words in years")
    ax.set_xlabel("year")
    ax.set_xticks(range(min(years), max(years)+1))
    ax.set_ylabel("frequency")

    for word in words:
        df_word = df.filter(df.word == word)
        df_word = df_word.filter(df.year.isin(years))
        ax.scatter(df_word.select("year").collect(), df_word.select("frequency").collect(), label=word)

    ax.legend()
    plt.show()
    spark.stop()
