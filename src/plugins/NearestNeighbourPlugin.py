from typing import List, Any

import numpy as np
from numpy.linalg import norm
from operator import itemgetter

from src.plugins.base_plugin import BasePlugin


class NearestNeighbourPlugin(BasePlugin):
        """Plugin for calculation of k nearest neighbours."""

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)

        def register_udfs(self) -> None:
            super().register_udf("euclidean_knn", NearestNeighbourPlugin.euclidean_knn, "")


        @staticmethod
        def euclidean_knn(k: int, table: List[List[Any]]):
            """Returns k nearest neighbours for the time series of a word."""

            # TODO: first line as seperate parameter for easier calling (so order in table doesn't matter)
            #def euclidean_knn(k: int, tuple: List[Any], table: List[List[Any]]):


            # tuple whose neighbours are to be calculated
            first_line = table[0]

            # euclidean distances, 2-tuple (word, euclidean dist)
            distances: List[(str, float)] = []

            # compare against all other lines to calculate distance
            for other_line in table[1:]:

                # calculate euclidean distance
                dist = norm(np.array(first_line[1:]) - np.array(other_line[1:]))

                # tuples (word, euclidean distance)
                distances.append((other_line[0], dist))
                # TODO performance enhancement: don't insert if result > k and new entry is worse
                # TODO further increase performance by use of sorted list and truncate list to k after each run

                # sort euclidean distances (second entry of tuples)
                distances.sort(key=itemgetter(1))

                # k nearest neighbours only
                return distances[0:k]











