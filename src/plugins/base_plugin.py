from abc import ABC 
from uuid import uuid4, UUID

class BasePlugin(ABC):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        __spark = kwargs["spark"]

    # TODO: typing
    def register_udf(self, udf_name: str, udf_fct, udf_schema) -> None:
        self.__spark.udf.register(udf_name, udf_fct, udf_schema)

    def get_uuid(self) -> UUID:
        """
        Returns global ID for given plugin.
        """
        return uuid4()


