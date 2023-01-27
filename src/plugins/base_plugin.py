from abc import ABC, abstractmethod
from uuid import uuid4, UUID

class BasePlugin(ABC):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.__spark = kwargs["spark"]

    @abstractmethod
    def register_udfs(self) -> None:
        raise NotImplementedError("Please implement this method!")

    def register_udf(self, udf_name: str, udf_fct, udf_schema) -> None:
        self.__spark.udf.register(udf_name, udf_fct, udf_schema)

    def get_uuid(self) -> UUID:
        """
        Returns global ID for given plugin.
        """
        return uuid4()


