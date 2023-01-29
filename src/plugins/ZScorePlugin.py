from src.plugins.base_plugin import BasePlugin
from src.plugins.InternalOD import ZScoreOD
from typing import List
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType
import numpy as np


class ZScorePlugin(BasePlugin):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def register_udfs(self) -> None:
        super().register_udf("zscore", self.zscore, self.schema_zscore)

    schema_zscore = StructType(
        [
            StructField("outlier", ArrayType(IntegerType(), False), False)
        ]
    )

    @staticmethod
    def zscore(threshold: float, *data_row):
        time_series = list(range(1800, 2000))
        data_list = list()
        for data in data_row[2:203]:
            data_list.append(data)
        zscore_od = ZScoreOD(threshold=threshold, data_list=data_list)

        return [zscore_od.detect_outliers(time_series)]
        #select zscore(3, *) zscore from (select * from schema_f)
