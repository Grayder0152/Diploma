from typing import Optional

from pyspark.sql import SparkSession

from src.local.benchmarks.bm_spark import SparkBenchmark
from src.local.engines.en_base import BaseEngine
from src.local.settings import EnginesEnum


class SparkEngine(BaseEngine):
    """Process runner for Spark engine."""

    name = EnginesEnum.SPARK.value
    spark: Optional[SparkSession] = None

    # def __init__(self, config: BenchmarksConfig):
    #     super().__init__(config=config)

    def __enter__(self):
        if self.cfg.engine_config is not None:
            self.spark = (
                SparkSession.builder
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.executor.instances", self.cfg.engine_config.cpu_count)
                .config("spark.executor.memory", f"{self.cfg.engine_config.memory_limit_gb // self.cfg.engine_config.cpu_count}g")
                .config("spark.sql.shuffle.partitions", self.cfg.engine_config.partition_count or 16)
                .config("spark.default.parallelism", self.cfg.engine_config.default_parallelism or 16)
                .getOrCreate()
            )
            self.spark.sparkContext.setLogLevel('ERROR')
        else:
            self.spark = SparkSession.builder.getOrCreate()

        self.benchmarks = [SparkBenchmark(data_config, self.cfg.tasks,self.spark) for data_config in self.cfg.data_configs]

        return self

    def __exit__(self, *args, **kwargs):
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
            self.benchmarks = None