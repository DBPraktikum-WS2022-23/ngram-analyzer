from abc import ABC, abstractmethod
from typing import List, Any, Tuple, Optional
import numpy as np


class InternalOutlierDetector(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def detect_outliers(self, time_series_list: List[Any]) -> Optional[List[Any]]:
        pass


class MedianDistanceOD(InternalOutlierDetector):
    def __init__(self, threshold: float, data_list: List[Any]) -> None:
        super().__init__()
        self.__threshold = float(threshold)
        self.__data_list = data_list

    def detect_outliers(self, time_series_list: List[Any]) -> Optional[List[Any]]:
        median = np.median(self.__data_list)
        np.seterr(divide='ignore', invalid='ignore')
        outliers: List[int] = list()

        for i in range(len(time_series_list)):
            if self.__data_list[i] < median * (1 - self.__threshold):
                outliers.append(time_series_list[i])

            if self.__data_list[i] > median * (1 + self.__threshold):
                outliers.append(time_series_list[i])

        return outliers


class ZScoreOD(InternalOutlierDetector):
    def __init__(self, threshold: float, data_list: List[Any]) -> None:
        super().__init__()
        self.__threshold = float(threshold)
        self.__data_list = data_list

    def detect_outliers(self, time_series_list: List[Any]) -> Optional[List[Any]]:
        std = np.std(self.__data_list)
        mean = np.mean(self.__data_list)
        np.seterr(divide='ignore', invalid='ignore')
        outliers: List[int] = list()

        for i in range(len(time_series_list)):
            zscore = self.__compute_zscore(self.__data_list[i], std, mean)
            if zscore < -self.__threshold or zscore > self.__threshold:
                outliers.append(time_series_list[i])
        return outliers

    @staticmethod
    def __compute_zscore(occurence, std, mean):
        return (occurence - mean) / std



