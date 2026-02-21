"""Abstract connector contract for ETL source integrations."""

from abc import ABC, abstractmethod

from .data_contract import IngestionResult


class BaseConnector(ABC):
    @abstractmethod
    def connect(self) -> None:
        """Initialize and validate access to the external source."""
        pass

    @abstractmethod
    def fetch_data(self, query: str) -> IngestionResult:
        """Fetch source data and return it in standardized ingestion format."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Release all connector resources."""
        pass
