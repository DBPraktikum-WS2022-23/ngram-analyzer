from typing import List, Any

import numpy as np
from numpy.linalg import norm
from operator import itemgetter

from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from plugins.base_plugin import BasePlugin


class NearestNeighbourPlugin(BasePlugin):
        """Plugin for calculation of k nearest neighbours."""

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)

        def register_udfs(self) -> None:
            # super().register_udf("euclidean_knn", NearestNeighbourPlugin.euclidean_knn, "")
            super().register_udf("euclidean_dist", NearestNeighbourPlugin.euclidean_dist,
                                 NearestNeighbourPlugin.schema_e)

        schema_e = StructType(
            [
                StructField("str_rep", StringType(), False),
                StructField("result", FloatType(), False),
            ]
        )

        # TODO: not working
        # @staticmethod
        # def euclidean_knn(k: int, table: List[List[Any]]):
        #     """Returns k nearest neighbours for the time series of a word."""
        #
        #     # TODO: first line as seperate parameter for easier calling (so order in table doesn't matter)
        #     #def euclidean_knn(k: int, tuple: List[Any], table: List[List[Any]]):
        #
        #
        #     # tuple whose neighbours are to be calculated
        #     first_line = table[0]
        #
        #     # euclidean distances, 2-tuple (word, euclidean dist)
        #     distances: List[(str, float)] = []
        #
        #     # compare against all other lines to calculate distance
        #     for other_line in table[1:]:
        #
        #         # calculate euclidean distance
        #         dist = norm(np.array(first_line[1:]) - np.array(other_line[1:]))
        #
        #         # tuples (word, euclidean distance)
        #         distances.append((other_line[0], dist))
        #         # TODO performance enhancement: don't insert if result > k and new entry is worse
        #         # TODO further increase performance by use of sorted list and truncate list to k after each run
        #
        #         # sort euclidean distances (second entry of tuples)
        #         distances.sort(key=itemgetter(1))
        #
        #         # k nearest neighbours only
        #         return distances[0:k]

        @staticmethod
        def euclidean_dist(*fxf_tuple):
            """Returns the euclidean distance for two timeseries from schema f."""
            """Example usage: select ed.str_rep, ed.result from (select euclidean_dist(*) ed from 
            schema_f a cross join schema_f b where a.str_rep = 'word_of_interest' and 
            b.str_rep != 'word_of_interest') order by result limit k"""

            # FxF format: w1, t1, frq1_1800, ..., frq1_2000, w2, t2, frq2_1800, ..., frq2_2000

            # TODO: remove debugging code before submission
            debug = False

            mid: int = len(fxf_tuple) // 2

            # split tuples
            first_line = fxf_tuple[:mid]
            other_line = fxf_tuple[mid:]

            # word of the second tuple
            other_word = other_line[0]

            # remove str_rep and type
            first_line = first_line[2:]
            other_line = other_line[2:]

            if debug:
                print("")
                print(f"fxf-tuple:\n{fxf_tuple}")
                print("type of fxf_tuple:", type(fxf_tuple))

                print("")
                print(f"first_line:\n{first_line}")
                print(f"other_line:\n{other_line}")

                print("")
                print("type of 'first_line':", type(first_line))
                print("type of 'first_line[5]':", type(first_line[5]))

            # calculate euclidean distance
            dist = float(norm(np.array(first_line) - np.array(other_line)))

            return other_word, dist
