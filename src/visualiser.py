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
        """Set uo Word Frequency Object"""
        self.db2df: DatabaseToSparkDF = DatabaseToSparkDF(spark, db_url, properties)
        self.df_word: DataFrame = self.db2df.df_word
        self.df_occurence: DataFrame = self.db2df.df_occurence

    def plot_kde(self) -> None:
        """Plot the frequency of certain words in certain years"""

        # TODO: these are inputs from info.py
        bandwidth: float = 0.25
        word: str = "Aekerund"
        years = [1832, 1854, 1863, 1868, 1887, 1888, 1890]
        freqs = [1, 1, 1, 1, 3, 2, 1]

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

        # check if the directory output already exists, if not, create it
        if not os.path.exists("output"):
            os.mkdir("output")
        plt_name: str = f"output/kdeplot_{word}.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")
