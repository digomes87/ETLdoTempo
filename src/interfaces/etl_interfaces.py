from abc import ABC, abstractmethod
from typing import Any


class DataTransformer(ABC):
    """
    Interface for data transformation components.
    Defines the contract for transforming data from one state to another.
    """

    @abstractmethod
    def transform(self, data: Any) -> Any:
        """
        Transforms the input data and returns the processed result.

        Args:
            data: The input data (e.g., PySpark DataFrame, Pandas DataFrame, etc.)

        Returns:
            The transformed data in the expected format.
        """
        pass


class DataExtractor(ABC):
    """
    Interface for data extraction components.
    """

    @abstractmethod
    def extract(self, source: str) -> Any:
        """
        Extracts data from a source (URL, Path, API).
        """
        pass


class DataLoader(ABC):
    """
    Interface for data loading components.
    """

    @abstractmethod
    def load(self, destination: str, data: Any) -> None:
        """
        Loads data into a destination (Database, File, Cloud Storage).
        """
        pass
