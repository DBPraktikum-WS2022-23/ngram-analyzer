from abc import ABC, abstractmethod
from typing import List, Any, Tuple

class InternalOutlierDetector(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def detect_outliers(self, time_series_list: List[Tuple[int, int]]) -> List[int]:
        pass
