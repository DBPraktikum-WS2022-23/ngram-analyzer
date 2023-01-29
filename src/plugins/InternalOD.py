from abc import ABC, abstractmethod
from typing import List, Any, Tuple, Optional
import numpy as np
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from src.plugins.base_plugin import BasePlugin


class InternalOutlierDetector(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def detect_outliers(self, time_series_list: List[Any]) -> Optional[List[Any]]:
        pass


class MedianDistanceOD(InternalOutlierDetector):
    def __init__(self, threshold: float, data_list: List[Any]) -> None:
        super().__init__()
        self.threshold = threshold
        self.__data_list = data_list

    def detect_outliers(self, time_series_list: List[Any]) -> Optional[List[Any]]:
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
    def __init__(self, data_list: List[Any]) -> None:
        super().__init__()
        self.__data_list = data_list

    def detect_outliers(self, time_series_list: List[Any]) -> Optional[List[Any]]:
        # TODO: get std() and
        std = np.std(self.__data_list)
        mean = np.mean(self.__data_list)

        outliers: List[str] = list()

        for i in range(len(self.__data_list)):
            zscore: float = self.__compute_zscore(self.__data_list[i], std[0], mean[0])
            if zscore < -3 or zscore > 3:
                outliers.append(time_series_list[i])
        return outliers

    @staticmethod
    def __compute_zscore(self, occurence: int, std: float, mean: float) -> float:
        return (occurence - mean) / std



