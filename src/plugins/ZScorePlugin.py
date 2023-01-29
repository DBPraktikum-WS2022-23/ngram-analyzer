from src.plugins.base_plugin import BasePlugin
from src.plugins.InternalOD import ZScoreOD
from typing import List
from pyspark.sql.types import StructType, StructField, ArrayType, StringType


class ZScorePlugin(BasePlugin):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def register_udfs(self) -> None:
        super().register_udf("zscore", self.zscore, self.schema_zscore)

    schema_zscore = StructType(
        [
            StructField("outlier", StringType(), False)
        ]
    )

    @staticmethod
    def zscore(time_series: List[int], *data_row) -> List[str]:
        assert len(time_series) == len(data_row)
        data_list = list()
        for data in data_row:
            data_list.append(data)
        zscore_od = ZScoreOD(data_list=data_list)

        return zscore_od.detect_outliers(time_series)
