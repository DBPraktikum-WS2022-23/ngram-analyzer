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
        pass