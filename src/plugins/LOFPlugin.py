from abc import ABC, abstractmethod
from typing import List

from scipy.spatial import distance
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType

from src.plugins.base_plugin import BasePlugin

class ExternalOutlierDetector(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def detect_outliers(self, time_series_list: List[List[int]]) -> List[int]:
        pass

class LOFOutlierDetector(ExternalOutlierDetector):
    def __init__(self, k: int, delta: float) -> None:
        super().__init__()
        self.__k = k
        self.__delta = delta

    def __reachability_distance(self, a: List[int], b: List[int], knn: List[int]) -> float:
        reach_distance = max(distance.euclidean(a, b), distance.euclidean(a, knn))
        # print("reachability_distance:", reach_distance, ) 
        return reach_distance    

    def __local_reachability_density(self, a: List[int], knns: List[List[int]]) -> float:
        knn: List[int] = knns[self.__k - 1] # k-nearest neighbour
        mean_reach_distance = sum(self.__reachability_distance(a, n, knn) for n in knns) / len(knns)
        # print("mean reachability distance", mean_reach_distance)
        return 1 / mean_reach_distance if mean_reach_distance != 0 else mean_reach_distance

    def __get_knns(self, time_series: List[int], time_series_list: List[List[int]]) -> List[List[int]]:
        # time_series_list should not contain time_series
        distances = []
        for current_ts in time_series_list:
            current_distance = distance.euclidean(time_series, current_ts)
            distances.append((current_ts, current_distance))
        knns_tuple = sorted(distances, key=lambda t: t[1])[:self.__k]
        knns = [knn_tuple[0] for knn_tuple in knns_tuple]
        return knns

    def detect_outliers(self, time_series_list: List[List[int]]) -> List[int]:
        lof_list = []
        for id, time_series in enumerate(time_series_list):
            # print("---------------------------------")
            # print("current time series:", time_series)
            lof = 0

            possible_knns = time_series_list.copy()
            possible_knns.pop(id)
            knns = self.__get_knns(time_series, possible_knns)
            # print("neighbors of current time series:", knns)

            lrd_x = self.__local_reachability_density(time_series, knns)
            # print("lrd_x:", lrd_x, "for time series", time_series)
            if lrd_x == 0:
                continue
            for neighbor in knns:
                lrd_n = self.__local_reachability_density(neighbor, knns)
                # print("lrd_n:", lrd_n, "for neighbor", neighbor)
                lof += lrd_n / lrd_x
            # print("lof:", lof)
            lof = lof / len(knns)
            lof_list.append(lof)

        lof_index_list = []

        for i in range(len(lof_list)):
            if lof_list[i] > self.__delta:
                lof_index_list.append(i)
        
        return lof_index_list

class LOFPlugin(BasePlugin):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def register_udfs(self) -> None:
        super().register_udf("lof", self.lof, self.schema_lof)

    schema_lof = StructType(
        [
            StructField("outlier", ArrayType(IntegerType(), False), False)
        ]
    )

    @staticmethod
    def lof(k: int, delta: float, *time_series):
        lofod = LOFOutlierDetector(k, delta)
        time_series_list = []

        for i in range (0, len(time_series), 203):
            time_series_list.append(list(time_series[(i + 2):(i + 203)]))

        return [lofod.detect_outliers(time_series_list)]

if __name__ == "__main__":
    ts = [2,3,10,0]
    ts_list = [
        [0,0,0,0], [0,0,0,0], [1,3,4,2], [3,4,5,0], [2,10,10,10], [3,4,5,10],
        [0,0,0,0], [0,0,0,0], [0,0,0,0], [0,0,0,0], [0,0,0,0], [0,0,0,0], 
        ]
    lof = LOFOutlierDetector(2, 1.5)
    print(ts_list, "\n",  lof.detect_outliers(ts_list))
