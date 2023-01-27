from abc import ABC, abstractmethod
from typing import List, Any, Optional, Tuple

from scipy.spatial import distance

class ExternalOutlierDetector(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def detect_outliers(self, time_series_list: List[List[int]]) -> List[int]:
        pass

class LOFOutlierDetector(ExternalOutlierDetector):
    def __init__(self, k: int, delta: int, data: List[List[int]]) -> None:
        super().__init__()
        self.__k = k
        self.__delta = delta

    def __reachability_distance(self, a: List[int], b: List[int], knn: List[int]) -> float:
        return max(distance.euclidean(a, b), distance.euclidean(a, knn))     

    def __local_reachability_density(self, a: List[int], knns: List[List[int]]) -> float:
        knn: List[int] = knns[self.__k] # k-nearest neighbour
        mean_reach_distance = sum(self.__reachability_distance(a, n, knn) for n in knns) / len(knns)
        return 1 / mean_reach_distance 

    def __get_knns(self, time_series: List[int]) -> List[List[int]]:
        # TODO: find knns for current time series
        # https://github.com/Muhammads786/SQLMachineLearningAlgorithms/blob/main/K_Nearest_Neighbors_Classifier_Simple_Demo.ipynb
        return [[]]

    def detect_outliers(self, time_series_list: List[List[int]]) -> List[int]:
        lof_list = []
        for time_series in time_series_list:
            lof = 0
            knns = self.__get_knns(time_series)
            for neighbor in knns:
                lof += self.__local_reachability_density(neighbor, knns) / self.__local_reachability_density(time_series, knns)
            lof = lof / self.__k
            lof_list.append(lof)
        
        lof_index_list = []

        for i in range(len(lof_list)):
            if lof_list[i] > self.__delta:
                lof_index_list.append(i)
        
        return lof_index_list

if __name__ == "__main__":
    print("Try out functions")
