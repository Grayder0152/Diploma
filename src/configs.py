from __future__ import annotations

import json
import os
import random

from dataclasses import dataclass, field
from glob import glob
from typing import Any, Optional

from src.settings import EnginesEnum


@dataclass
class DataConfig:
    """Configuration for controlling data volume and loading behavior."""

    base_dir: str
    n_files: Optional[int] = None

    files: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.files = self._list_csv_files()

    def _list_csv_files(self) -> list[str]:
        """Recursively list CSV files under base_dir."""

        pattern = os.path.join(self.base_dir, "**", "*.csv")
        all_files = glob(pattern, recursive=True)
        if not all_files:
            raise FileNotFoundError(f"No CSV files found in {self.base_dir}")
        if self.n_files:
            all_files = all_files[: self.n_files]
        return all_files

    def __repr__(self):
        return f"{self.__class__.__name__}(base_dir={self.base_dir!r}, n_files={self.n_files!r})"

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""

        return {"base_dir": self.base_dir, "n_files": self.n_files}


@dataclass
class EngineConfig:
    """Engine-level configuration for Spark/Polars benchmarking."""

    cpu_count: int
    memory_limit_gb: int

    lazy_mode: Optional[bool] = None
    default_parallelism: Optional[int] = None
    partition_count: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""

        return {"cpu_count": self.cpu_count, "memory_limit_gb": self.memory_limit_gb}

    def describe(self) -> None:
        """Print configuration in human-readable format."""

        print("\n[ENGINE CONFIGURATION]")
        for key, value in self.__dict__.items():
            print(f"  {key}: {value}")


@dataclass
class BenchmarksConfig:
    """Main configuration for a benchmark experiment case."""

    name: str
    engines: list[EnginesEnum]
    data_configs: list[DataConfig]
    engine_config: EngineConfig
    tasks: list[str]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BenchmarksConfig:
        raise NotImplementedError()

    @classmethod
    def load_from_json(cls, path: str) -> list[BenchmarksConfig]:
        """Load all experiment configurations from JSON file."""

        with open(path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if not isinstance(raw_data, list):
            raise ValueError("Root JSON element must be a list of experiments")

        return [cls.from_dict(item) for item in raw_data]


@dataclass
class DatabricksBenchmarksConfig(BenchmarksConfig):

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BenchmarksConfig:
        """Parse experiment configuration from dictionary (e.g., loaded JSON)."""

        engine = EngineConfig(cpu_count=0, memory_limit_gb=0)
        datasets = [DataConfig(**d) for d in data["data_configs"]]
        return cls(
            name=data["name"],
            engines=data["engines"],
            engine_config=engine,
            data_configs=datasets,
            tasks=data.get("tasks", []),
        )


@dataclass
class LocalBenchmarksConfig(BenchmarksConfig):
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BenchmarksConfig:
        """Parse experiment configuration from dictionary (e.g., loaded JSON)."""

        engine = EngineConfig(**data["engine_config"])
        datasets = [DataConfig(**d) for d in data["data_configs"]]
        return cls(
            name=data["name"],
            engines=data["engines"],
            engine_config=engine,
            data_configs=datasets,
            tasks=data.get("tasks", []),
        )
