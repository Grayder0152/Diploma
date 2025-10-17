import polars as pl

from src.benchmarks.bm_base import BaseBenchmark


class PolarsBenchmark(BaseBenchmark):
    """Scalable benchmark suite for Polars."""

    def _read_multiple_csv_files(self) -> pl.LazyFrame:
        lazy_frames = [pl.scan_csv(f) for f in self.data_config.files]
        return pl.concat(lazy_frames)

    def group_by_model(self):
        df = self._read_multiple_csv_files()
        return (
            df.group_by("model")
            .agg([
                pl.col("smart_5_raw").mean().alias("avg_smart_5_raw"),
                pl.col("smart_5_raw").std().alias("std_smart_5_raw"),
                pl.col("serial_number").n_unique().alias("unique_disks"),
                pl.col("failure").sum().alias("failures_total"),
            ])
            .sort("avg_smart_5_raw", descending=True)
        ).collect()

    def union_and_aggregate(self):
        df = self._read_multiple_csv_files()
        return (
            df.group_by("model")
            .agg([
                pl.col("smart_5_raw").mean().alias("avg_5"),
                pl.col("smart_5_raw").quantile(0.9).alias("p90_5"),
                pl.col("failure").sum().alias("failures"),
                pl.count().alias("records"),
            ])
            .with_columns((pl.col("failures") / pl.col("records")).alias("failure_rate"))
            .sort("records", descending=True)
        ).collect()

    def join_adjacent_days(self):
        files = sorted(self.data_config.files)
        if len(files) < 2:
            return
            # raise ValueError("At least two files are required for multi-day join test.")

        df1 = pl.scan_csv(files[0]).select(["serial_number", "smart_5_raw", "date"])
        df2 = pl.scan_csv(files[1]).select(["serial_number", "smart_5_raw", "date"])
        joined = df1.join(df2, on="serial_number", how="inner", suffix="_next")
        return joined.select([
            "serial_number",
            "date",
            "date_next",
            (pl.col("smart_5_raw_next") - pl.col("smart_5_raw")).alias("smart_delta"),
        ]).collect()

    def quarterly_stats(self):
        df = self._read_multiple_csv_files()
        return (
            df.with_columns(pl.col("model").cast(pl.Utf8))
            .group_by("model")
            .agg([
                pl.col("smart_5_raw").mean().alias("mean_5"),
                pl.col("smart_187_raw").mean().alias("mean_187"),
                pl.col("failure").sum().alias("failures"),
                pl.count().alias("count"),
            ])
            .with_columns(
                (pl.col("failures") / pl.col("count")).alias("failure_rate")
            )
        ).collect()
