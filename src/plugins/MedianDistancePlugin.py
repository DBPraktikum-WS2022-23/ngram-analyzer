from src.plugins.base_plugin import BasePlugin
from src.plugins.InternalOD import MedianDistanceOD
from typing import List
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType


class MedianDistancePlugin(BasePlugin):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def register_udfs(self) -> None:
        super().register_udf("median_distance", self.median_distance, self.schema_median_distance)

    schema_median_distance = StructType(
        [
            StructField("outlier", ArrayType(IntegerType(), False), False)
        ]
    )

    @staticmethod
    def median_distance(threshold: float, *data_row):
        time_series = list(range(1800, 2000))
        data_list = list()
        for data in data_row[2:203]:
            data_list.append(data)
        median_distance_od = MedianDistanceOD(threshold=threshold, data_list=data_list)

        return [median_distance_od.detect_outliers(time_series)]
    # select median_distance(0.1, *) median_distance from (select * from schema_f)
