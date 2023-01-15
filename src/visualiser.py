import os
from typing import Any, Dict, List
from src.info import StatFunctions as sf
import matplotlib.pyplot as plt  # type: ignore
import numpy as np
from scipy.stats import gaussian_kde
from pyspark.sql import DataFrame, Row, SparkSession
from src.info import DatabaseToSparkDF


class Visualiser:
    """Module for visualise statistics"""

    def __init__(self):
        pass

    def plot_boxplot_all(self, df: DataFrame, start_year: int, end_year: int) -> None:
        """Boxplot of all words in certain years"""

        # get data of boxplot
        words = df.rdd.map(lambda row: row['str_rep']).collect()
        data = df.rdd.map(lambda row: sf.get_freqs(row, start_year, end_year)).collect()

        # set size of the figure
        leftmargin = 0.5  # inches
        rightmargin = 0.3  # inches
        categorysize = 1  # inches
        n = len(data)
        figwidth = leftmargin + rightmargin + (n + 1) * categorysize
        fig = plt.figure(figsize=(figwidth, figwidth * 0.75))
        fig.subplots_adjust(left=leftmargin / figwidth, right=1 - rightmargin / figwidth,
                            top=0.94, bottom=0.1)
        ax = fig.add_subplot(111)
        ax.set_xlim(-0.5, n - 0.5)
        ax.boxplot(data, labels=words, positions=np.arange(n))

        # save to output
        if not os.path.exists("output"):
            os.mkdir("output")
        plt_name: str = f"output/boxplot.png"
        plt.savefig(plt_name, bbox_inches='tight', dpi=100)
        print(f"Saved {plt_name} to output directory")

    def plot_scatter_all(self, df: DataFrame, start_year: int, end_year: int) -> None:
        """Plot the frequency as scatter of all words in certain years and the regression line of each word"""

        # TODO: group the data and set colors
        years = list(range(start_year, end_year))
        data = df.rdd.map(lambda row: sf.get_freqs(row, start_year, end_year)).collect()
        slopes = df.rdd.map(lambda row: sf.lr(*row)[1]).collect()
        intercepts = df.rdd.map(lambda row: sf.lr(*row)[2]).collect()
        x_seq = np.linspace(start_year, end_year, num=1000)

        for i in range(len(data)):
            plt.scatter(years, data[i])
            plt.plot(x_seq, intercepts[i] + slopes[i] * x_seq)

        # check if the directory output already exists, if not, create it
        if not os.path.exists("output"):
            os.mkdir("output")
        plt_name: str = f"output/scatter_plot_all.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")

    def plot_kde(self, df: DataFrame, start_year: int, end_year: int, word: str, bandwidth: float) -> None:
        """Plot the Kernel Density Estimation with Gauss-Kernel of a word"""

        word: str = "Aekerund"
        bandwidth: float = 0.25

        years = list(range(start_year, end_year))
        temp = df.rdd.map(lambda row: sf.get_freqs(row, start_year, end_year) if row['str_rep'] == word else None)\
            .collect()
        freqs = None
        for item in temp:
            if item != None:
                freqs = item

        _, axis = plt.subplots()
        axis.set_title(f"Kernel Density Estimation of \"{word}\" with Gauss-Kernel")
        axis.set_xlabel("year")
        axis.set_ylabel("density")

        data = []
        for a, b in zip(years, freqs):
            data += [a] * b
        kde = gaussian_kde(data)
        xs = np.linspace(min(years), max(years), 200)
        kde.set_bandwidth(bw_method=kde.factor * bandwidth)
        plt.hist(data, density=True)
        plt.plot(xs, kde(xs))

        if not os.path.exists("output"):
            os.mkdir("output")
        plt_name: str = f"output/kde_plot_{word}_{bandwidth}.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")
