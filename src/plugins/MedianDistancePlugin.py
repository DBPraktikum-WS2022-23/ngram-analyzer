from src.plugins.base_plugin import BasePlugin
from src.plugins.InternalOD import MedianDistanceOD
from typing import List
from pyspark.sql.types import StructType, StructField, ArrayType, StringType


class MedianDistancePlugin(BasePlugin):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def register_udfs(self) -> None:
        super().register_udf("zscore", self.median_distance, self.schema_median_distance)

    schema_median_distance = StructType(
        [
            StructField("outlier", StringType(), False)
        ]
    )

    @staticmethod
    def median_distance(time_series: List[int], *data_row, threshold: float) -> List[str]:
        assert len(time_series) == len(data_row)
        data_list = list()
        for data in data_row:
            data_list.append(data)
        median_distance_od = MedianDistanceOD(threshold=threshold, data_list=data_list)

        return median_distance_od.detect_outliers(time_series)
