from abc import ABC, abstractmethod
from typing import Optional

from src.benchmarks.bm_base import BaseBenchmark
from src.configs import BenchmarksConfig


class BaseEngine(ABC):
    """Base class for data processing operations."""

    def __init__(self, config: BenchmarksConfig):
        self.cfg = config
        # self._measure_metrics = MeasureMetrics()
        self.benchmarks: Optional[list[BaseBenchmark]] = None

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, *args, **kwargs):
        pass

    def run(self):
        pass

    # def run_benchmark(benchmark: BaseBenchmark):
    #     results = {
    #         "data": benchmark.data_config,
    #         "tasks_measure": []
    #     }
    #     for task_name, task in benchmark.tasks:
    #         results["tasks_measure"].append(
    #             {"task_name": task_name, "duration": measure_time(task)}
    #         )
    #
    #     return results

    # def run(self, task, *args, **kwargs):
    # """Run a benchmarked function under controlled resources."""
    #
    # self._metric.measure_memory()
    # result = self._metric.measure_time(task, *args, **kwargs)
    # self._metric.measure_cpu()
    # self._logger.info(str(self._metric.summary()))
    # return result
