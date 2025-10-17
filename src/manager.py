import traceback

from pprint import pprint
from typing import Any

from src.benchmarks.bm_base import BaseBenchmark
from src.configs import BenchmarksConfig
from src.engines.en_base import BaseEngine
from src.engines.en_polars import PolarsEngine
from src.engines.en_spark import SparkEngine
from src.logger import Logger
from src.metrics import measure_time
from src.report import BenchmarkReporter
from src.settings import EnginesEnum, REPORT_FILE_NAME


class BenchmarksManager:
    """Class for managing benchmark experiments with detailed logging."""

    def __init__(self, configs: list[BenchmarksConfig], report_output_file_name: str = REPORT_FILE_NAME):
        self.configs = configs
        self.logger = Logger("BenchmarksManager")
        self.report = BenchmarkReporter(report_output_file_name, self.logger)

    def measure_engine(self, engine: BaseEngine) -> dict:
        """Measure execution time of all benchmarks for a given engine."""

        self.logger.info(f"[ENGINE] Measuring benchmarks for engine: {engine.name}")
        engine_results = {
            "engine": engine.name,
            "tasks_measure": [],
        }

        for benchmark in engine.benchmarks:
            self.logger.info(f"[ENGINE:{engine.name}] Running benchmark")
            try:
                benchmark_result = self.measure_benchmark(benchmark)
            except Exception as e:
                self.logger.error(f"[ENGINE:{engine.name}] Benchmark failed: {e}\n{traceback.format_exc()}")
                engine_results["tasks_measure"].append({"benchmark": benchmark.name,"error": str(e)})
            else:
                engine_results["tasks_measure"].append(benchmark_result)
                self.logger.debug(f"[ENGINE:{engine.name}] Benchmark finished successfully.")
        return engine_results

    def measure_benchmark(self, benchmark: BaseBenchmark) -> dict:
        """Run and measure all tasks of a single benchmark."""

        results = {"data": benchmark.data_config.to_dict(), "tasks_measure": []}
        for task_name, task in benchmark.tasks.items():
            self.logger.info(f"[TASK:{task_name}] Starting task execution...")
            try:
                duration = measure_time(task)
            except Exception as e:
                self.logger.error(f"[TASK:{task_name}] Task failed with error: {e}\n{traceback.format_exc()}")
                results["tasks_measure"].append({"task_name": task_name,"duration": 0,"error": str(e)})
            else:
                self.logger.info(f"[TASK:{task_name}] Completed in {duration:.4f} seconds.")
                results["tasks_measure"].append({"task_name": task_name, "duration": duration})

        return results

    def run_benchmark(self) -> list[dict]:
        """Run all benchmark experiments."""

        self.logger.info("=" * 80)
        self.logger.info("Starting benchmark experiments...")
        results = []

        for config_idx, config in enumerate(self.configs, start=1):
            self.logger.info(f"[CONFIG {config_idx}] Loaded benchmark config:\n{config}")
            engine_result = {"engine_config": config.engine_config.to_dict(), "benchmarks": []}

            if EnginesEnum.SPARK.value in config.engines:
                self.logger.info(f"[CONFIG {config_idx}] Starting Spark experiment...")
                try:
                    with SparkEngine(config=config) as engine:
                        self.logger.debug(f"[CONFIG {config_idx}] Spark engine initialized successfully.")
                        engine_result["benchmarks"].append(self.measure_engine(engine))
                except Exception as e:
                    self.logger.error(
                        f"[CONFIG {config_idx}] Spark engine failed to execute: {e}\n{traceback.format_exc()}"
                    )

            if EnginesEnum.POLARS.value in config.engines:
                self.logger.info(f"[CONFIG {config_idx}] Starting Polars experiment...")
                try:
                    with PolarsEngine(config=config) as engine:
                        self.logger.debug(f"[CONFIG {config_idx}] Polars engine initialized successfully.")
                        engine_result["benchmarks"].append(self.measure_engine(engine))
                except Exception as e:
                    self.logger.error(
                        f"[CONFIG {config_idx}] Polars engine failed to execute: {e}\n{traceback.format_exc()}"
                    )

            results.append(engine_result)
            self.logger.info(f"[CONFIG {config_idx}] Experiment finished for config.\n")

        self.logger.info("Finished all benchmark experiments successfully.")
        self.logger.info("=" * 80)
        return results

    def run(self) -> list[dict[str, Any]]:
        """Main entrypoint for running benchmark experiments."""

        results = self.run_benchmark()
        print(results)
        self.report.summarize(results)
        return results


if __name__ == '__main__':
    cfg = BenchmarksConfig.load_from_json("../benchmarks.json")
    manager = BenchmarksManager(configs=cfg)
    result = manager.run()
    pprint(result)
