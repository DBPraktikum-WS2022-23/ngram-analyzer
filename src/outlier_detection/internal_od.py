from abc import ABC, abstractmethod
from typing import List, Any, Tuple
import numpy

class InternalOutlierDetector(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def detect_outliers(self, time_series_list: List[Tuple[int, int]]) -> List[int]:
        pass


class MedianDistanceOD(InternalOutlierDetector):
    def __init__(self, threshold: float) -> None:
        super().__init__()
        self.threshold = threshold

    def detect_outliers(self, time_series_list: List[Tuple[int, int]]) -> List[int]:
        # TODO: where do I get the median?
        median: int = 5

        outliers: List[int] = list()

        for time_series in time_series_list:
            if time_series[1] < median * (1 - self.threshold):
                outliers.append(time_series[0])

            if time_series[1] > median * (1 + self.threshold):
                outliers.append(time_series[0])

        return outliers


class ZScoreOD(InternalOutlierDetector):
    def __int__(self) -> None:
        pass

    def detect_outliers(self, time_series_list: List[Tuple[int, int]]) -> List[int]:

        std = numpy.std()


        pass
