import os
from typing import List
from info import StatFunctions as sf
import matplotlib.pyplot as plt  # type: ignore
import numpy as np
from scipy.stats import gaussian_kde
from pyspark.sql import DataFrame, Row, SparkSession


class Visualiser:
    """Module for visualise statistics"""

    def plot_boxplot_all(self, df: DataFrame, start_year: int, end_year: int, scaling_factor: float = 1.0) -> None:
        """Plot boxplot of all words in certain years"""

        words = df.rdd.map(lambda row: row['str_rep']).collect()
        data = df.rdd.map(lambda row: [scaling_factor * float(i) for i in (sf.get_freqs(row, start_year, end_year))])\
            .collect()

        # set size of the figure
        n = len(data)
        size = max(6.4, n * 1.5)
        fig, axis = plt.subplots(figsize=(size, size * 0.75))
        axis.set_title("Boxplot", size=size)
        axis.set_ylabel(f"frequency, scale={scaling_factor}")
        axis.set_xlim(-0.5, n - 0.5)
        axis.boxplot(data, labels=words, positions=np.arange(n))
        # save to output
        if not os.path.exists("output"):
            os.mkdir("output")
        if scaling_factor == 1.0:
            plt_name: str = f"output/boxplot.png"
        else:
            plt_name: str = f"output/boxplot_scale={scaling_factor}.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")

    def plot_scatter(
            self, df: DataFrame, start_year: int, end_year: int,
            scaling_factors: List[int] = [], with_regression_line: bool = False
    ) -> None:
        """Plot the frequency as scatter of all words in certain years
        and the regression line of each word"""

        years = list(range(start_year, end_year))
        data = df.rdd.map(lambda row: sf.get_freqs(row, start_year, end_year)).collect()
        words = df.rdd.map(lambda row:
                           row['str_rep'] + '_' if row['type'] is None else row['str_rep'] + '_' + row['type'])\
            .collect()
        slopes, intercepts, x_seq = [], [], []

        # set all factors to 1 if none are given.
        if not scaling_factors:
            scaling_factors = [1] * len(data)
        scaled_data = []
        for a, b in zip(data, scaling_factors):
            scaled_data.append(list([i * b for i in a]))

        # calculate linear regression if required
        if with_regression_line:
            slopes = df.rdd.map(lambda row: sf.lr(*row)[1]).collect()
            intercepts = df.rdd.map(lambda row: sf.lr(*row)[2]).collect()
            x_seq = np.linspace(start_year, end_year, num=1000)

        # plot each row
        size = max(6.4, len(data))
        fig, axis = plt.subplots(figsize=(size, size * 0.75))
        axis.set_title("Scatter plots", size=size)
        axis.set_xlabel("year")
        axis.set_ylabel("frequency")
        for i in range(len(data)):
            axis.scatter(years, scaled_data[i], label=words[i])
            # assuming that scale factor keeps 1
            if with_regression_line:
                max_distance = (0, [data[i][0]], [start_year])
                for j in years:
                    distance = abs(intercepts[i] + slopes[i] * j - data[i][j - start_year])
                    if distance == max_distance[0]:
                        max_distance[1].append(data[i][j - start_year])
                        max_distance[2].append(j)
                    if distance > max_distance[0]:
                        max_distance = (distance, [data[i][j - start_year]], [j])
                axis.plot(x_seq, intercepts[i] + slopes[i] * x_seq)
                axis.plot(max_distance[2], max_distance[1], marker="D", label="farthest point from " + words[i])
        axis.legend()

        # check if the directory output already exists, if not, create it
        if not os.path.exists("output"):
            os.mkdir("output")
        if with_regression_line:
            plt_name = f"output/scatter_plot_regression.png"
        else:
            plt_name: str = f"output/scatter_plot_all.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")

    def plot_kde(
            self, df: DataFrame, start_year: int, end_year: int, word: str, bandwidth: float, bins: int
    ) -> None:
        """Plot the Kernel Density Estimation with Gauss-Kernel of a word"""

        years = list(range(start_year, end_year))
        temp = df.rdd.map(lambda row: sf.get_freqs(row, start_year, end_year) if row['str_rep'] == word else None)\
            .collect()
        freqs = None
        for item in temp:
            if item is not None:
                freqs = item

        # set size of the figure
        _, axis = plt.subplots()
        axis.set_title(f"Kernel Density Estimation of \"{word}\" with Gauss-Kernel")
        axis.set_xlabel("year")
        axis.set_ylabel("density")

        # plot figure
        data = []
        for a, b in zip(years, freqs):
            data += [a] * b
        kde = gaussian_kde(data)
        xs = np.linspace(min(years), max(years), 200)
        kde.set_bandwidth(bw_method=kde.factor * bandwidth)
        plt.hist(data, density=True, bins=bins)
        plt.plot(xs, kde(xs))

        # save figure to output
        if not os.path.exists("output"):
            os.mkdir("output")
        plt_name: str = f"output/kde_plot_{word}_bandwidth={bandwidth}_bins={bins}.png"
        plt.savefig(plt_name)
        print(f"Saved {plt_name} to output directory")
