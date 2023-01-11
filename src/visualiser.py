import os
from typing import Any, Dict, List

import matplotlib.pyplot as plt  # type: ignore
import numpy as np
from scipy.stats import gaussian_kde
from pyspark.sql import DataFrame, Row, SparkSession
from src.info import DatabaseToSparkDF


class Visualiser:
    """Module for visualise statistics"""

    def __init__(
        self, spark: SparkSession, db_url: str, properties: Dict[str, str]
    ) -> None:
        self.db2df: DatabaseToSparkDF = DatabaseToSparkDF(spark, db_url, properties)
        self.df_word: DataFrame = self.db2df.df_word
        self.df_occurence: DataFrame = self.db2df.df_occurence

    def plot_scatter_all(self, df: DataFrame) -> None:
        """Plot the frequency of all words in certain years"""
        years = list(range(1800, 2000))

        _, axis = plt.subplots()
        axis.set_title("Frequency of words in years")
        axis.set_xlabel("year")
        axis.set_xticks(range(min(years), max(years) + 1))
        axis.set_ylabel("frequency")

        for row in df.collect():
            axis.scatter(
                years,
                row.select([c for c in df.columns if c in str(list(range(1800, 2000)))]).collect(),
                label=row.select("str_rep", "type").collect(),
            )
            # TODO: Regression, get a, b from Aufgabe 1
            # TODO: How to differ the plots from different 1-Gramm
            """# Fit linear regression via least squares with numpy.polyfit
            # It returns an slope (b) and intercept (a)
            # deg=1 means linear fit (i.e. polynomial of degree 1)
            b, a = np.polyfit(x, y, deg=1)

            # Create sequence of 100 numbers from 0 to 100 
            xseq = np.linspace(0, 10, num=100)

            # Plot regression line
            ax.plot(xseq, a + b * xseq, color="k", lw=2.5);"""

        axis.legend()
        plt.show()

        # check if the directory output already exists, if not, create it
        if not os.path.exists("output"):
            os.mkdir("output")
        plt_name: str = f"output/scatter_plot_all.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")

    def plot_kde(self) -> None:
        """Plot the Kernel Density Estimation with Gauss-Kernel of a word"""

        # TODO: these are inputs from info.py
        bandwidth: float = 0.25
        word: str = "Aekerund"
        years = [1832, 1854, 1863, 1868, 1887, 1888, 1890]
        freqs = [1, 1, 1, 1, 3, 2, 1]

        # TODO: set axis and legend
        _, axis = plt.subplots()
        axis.set_title("Kernel Density Estimation with Gauss-Kernel")
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
        plt.show()

        axis.legend()
        plt.show()

        if not os.path.exists("output"):
            os.mkdir("output")
        plt_name: str = f"output/kde_plot_{word}.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")
