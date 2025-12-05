from abc import ABC, abstractmethod

from src.local.configs import DataConfig


class BaseBenchmark(ABC):
    """Base abstract benchmark class providing common file logic."""

    def __init__(self, data_config: DataConfig, tasks: list[str]):
        self.data_config = data_config
        self.tasks = {
            bm_name: getattr(self, bm_name) for bm_name in dir(self)
            if callable(getattr(self, bm_name)) and not bm_name.startswith("_") and (bm_name in tasks or "*" in tasks)
        }

    @abstractmethod
    def group_by_model(self):
        """Aggregate SMART metrics by model."""

    @abstractmethod
    def union_and_aggregate(self):
        """Union several CSV files and compute overall statistics."""

    @abstractmethod
    def join_adjacent_days(self):
        """Join multiple sequential daily datasets to simulate time correlation."""

    @abstractmethod
    def quarterly_stats(self):
        """Simulate quarterly aggregation of SMART data."""
