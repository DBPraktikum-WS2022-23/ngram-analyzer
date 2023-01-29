from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType

from src.plugins.base_plugin import BasePlugin
from src.plugins.ExternalOD import LOFOutlierDetector

class LOFPlugin(BasePlugin):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def register_udfs(self) -> None:
        super().register_udf("lof", self.lof, self.schema_lof)

    schema_lof = StructType(
        [
            StructField("outlier", ArrayType(IntegerType(), False), False)
        ]
    )

    @staticmethod
    def lof(k: int, delta: float, *time_series):
        lofod = LOFOutlierDetector(k, delta)
        time_series_list = []

        for i in range (0, len(time_series), 203):
            time_series_list.append(list(time_series[(i + 2):(i + 203)]))

        return [lofod.detect_outliers(time_series_list)]
