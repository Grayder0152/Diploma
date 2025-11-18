import os

import polars as pl

from src.benchmarks.bm_polars import PolarsBenchmark
from src.configs import BenchmarksConfig
from src.engines.en_base import BaseEngine
from src.settings import EnginesEnum


class PolarsEngine(BaseEngine):
    """Process runner for Polars engine."""

    name = EnginesEnum.POLARS.value

    def __init__(self, config: BenchmarksConfig):
        super().__init__(config=config)

    def __enter__(self):
        """Configure thread count and lazy mode for Polars."""

        self.benchmarks = [PolarsBenchmark(data_config, self.cfg.tasks) for data_config in self.cfg.data_configs]
        if self.cfg.engines is not None:
            if self.cfg.engine_config.cpu_count:
                n_threads = self.cfg.engine_config.cpu_count * 2
                os.environ["POLARS_MAX_THREADS"] = str(n_threads)

            if self.cfg.engine_config.lazy_mode:
                pl.Config.set_tbl_formatting("UTF8_FULL")

        return self

    def __exit__(self, *args, **kwargs):
        pl.Config.restore_defaults()
        self.benchmarks = None
