from abc import ABC, abstractmethod
from typing import List, Any, Optional, Tuple

class ExternalOutlierDetector(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def detect_outliers(self, time_series_list: List[List[Tuple[int, int]]]) -> List[List[int]]:
        pass


class LOFOutlierDetector(ExternalOutlierDetector):
    def __init__(self, k: int, delta: int) -> None:
        super().__init__()
        self.__k = k
        self.__delta = delta

    def __reachability_distance(self, a, b) -> float:
        pass

    def __local_reachability_density(self, a) -> float:
        pass

    def detect_outliers(self, time_series_list: List[Any]) -> Optional[List[Any]]:
        lof_list = []
        for time_series in time_series_list:
            N_k = None # TODO: get k nearest neighbors
            lof = 0
            for neighbor in N_k:
                lof += self.__local_reachability_density(neighbor) / self.__local_reachability_density(time_series)
            lof = lof / self.__k
            lof_list.append(lof)
        
        lof_index_list = []

        for i in range(len(lof_list)):
            if lof_list[i] > self.__delta:
                lof_index_list.append(i)
        
        return lof_index_list
