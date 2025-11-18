import polars as pl
from src.benchmarks.bm_base import BaseBenchmark


class PolarsBenchmark(BaseBenchmark):
    """Scalable benchmark suite for Polars with distinct task types."""

    def _read_multiple_csv_files(self) -> list[pl.LazyFrame]:
        """Read multiple CSVs lazily."""

        return [pl.scan_csv(f) for f in self.data_config.files]

    def group_by_model(self):
        """
        Benchmark type: Aggregation + statistics.
        Measures group-by performance and aggregation efficiency.
        """

        df = pl.concat(self._read_multiple_csv_files())

        return (
            df.group_by("model")
            .agg([
                pl.col("smart_5_raw").mean().alias("avg_smart_5_raw"),
                pl.col("smart_5_raw").std().alias("std_smart_5_raw"),
                pl.col("serial_number").n_unique().alias("unique_disks"),
                pl.col("failure").sum().alias("failures_total"),
            ])
            .sort("avg_smart_5_raw", descending=True)
        ).collect()[-1]

    def union_and_aggregate(self):
        """
        Benchmark type: Multi-file concatenation + global aggregation.
        Measures concatenation (union) overhead and subsequent global reduce.
        """

        lazy_frames = self._read_multiple_csv_files()
        merged = pl.concat(lazy_frames)

        return (
            merged
            .group_by([
                pl.col("smart_5_raw"),
                pl.col("smart_187_raw"),
                pl.col("failure"),
            ])
            .agg([
                pl.col("smart_5_raw").mean().alias("mean_smart_5"),
                pl.col("smart_187_raw").mean().alias("mean_smart_187"),
                pl.col("failure").sum().alias("total_failures"),
                pl.count().alias("records_total"),
            ])
            .with_columns(
                (pl.col("total_failures") / pl.col("records_total")).alias("failure_rate")
            )
        ).collect(streaming=True)[-1]

    def join_adjacent_days(self):
        """
        Benchmark type: Join between adjacent files.
        Tests join performance and shuffle behavior.
        """

        files = sorted(self.data_config.files)
        if len(files) < 2:
            raise ValueError("At least two files are required for join test.")

        base_df = pl.scan_csv(files[0]).select(["serial_number", "smart_5_raw", "date"])

        for i, next_file in enumerate(files[1:], start=1):
            next_df = pl.scan_csv(next_file).select(["serial_number", "smart_5_raw", "date"])
            base_df = base_df.join(
                next_df,
                on="serial_number",
                how="inner",
                suffix=f"_next_{i}"
            )

        return base_df.collect()[-1]

    def quarterly_stats(self):
        """
        Benchmark type: Multi-aggregation with per-model join.
        Simulates more complex multi-metric computation.
        """

        df = pl.concat(self._read_multiple_csv_files())

        mean_stats = (
            df.group_by("model")
            .agg([
                pl.col("smart_5_raw").mean().alias("mean_5"),
                pl.col("smart_187_raw").mean().alias("mean_187"),
                pl.count().alias("count"),
            ])
        )

        failure_stats = (
            df.group_by("model")
            .agg(pl.col("failure").sum().alias("failures"))
        )

        joined = mean_stats.join(failure_stats, on="model", how="inner")

        return (
            joined.with_columns(
                (pl.col("failures") / pl.col("count")).alias("failure_rate")
            )
            .sort("failure_rate", descending=True)
        ).collect()[-1]