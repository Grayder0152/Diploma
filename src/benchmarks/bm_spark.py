from pyspark.sql import SparkSession, functions as f

from src.benchmarks.bm_base import BaseBenchmark
from src.configs import DataConfig


class SparkBenchmark(BaseBenchmark):
    """Scalable benchmark suite for Apache Spark."""

    def __init__(self, data_config: DataConfig, tasks: list[str], spark: SparkSession):
        super().__init__(data_config, tasks)
        self.spark = spark

    def _read_multiple_csv_files(self):
        """Read multiple CSV files based on configuration."""

        return self.spark.read.csv(self.data_config.files, header=True, inferSchema=True)

    def group_by_model(self):
        df = self._read_multiple_csv_files()
        return (
            df.groupBy("model")
            .agg(
                f.mean("smart_5_raw").alias("avg_smart_5_raw"),
                f.stddev("smart_5_raw").alias("std_smart_5_raw"),
                f.countDistinct("serial_number").alias("unique_disks"),
                f.sum("failure").alias("failures_total"),
            )
            .orderBy(f.desc("avg_smart_5_raw"))
        ).count()

    def union_and_aggregate(self):
        df = self._read_multiple_csv_files()
        return (
            df.groupBy("model")
            .agg(
                f.mean("smart_5_raw").alias("avg_5"),
                f.expr("percentile_approx(smart_5_raw, 0.9)").alias("p90_5"),
                f.sum("failure").alias("failures"),
                f.count("*").alias("records"),
            )
            .withColumn("failure_rate", f.col("failures") / f.col("records"))
            .orderBy(f.desc("records"))
        ).count()

    def join_adjacent_days(self):
        files = sorted(self.data_config.files)
        if len(files) < 2:
            return
            # raise ValueError("At least two files are required for multi-day join test.")

        df1 = self.spark.read.csv(files[0], header=True, inferSchema=True)
        df2 = self.spark.read.csv(files[1], header=True, inferSchema=True)

        df1 = df1.select("serial_number", "smart_5_raw", "date")
        df2 = df2.select(
            f.col("serial_number").alias("serial_number_2"),
            f.col("smart_5_raw").alias("smart_5_raw_next"),
            f.col("date").alias("next_date"),
        )

        joined = df1.join(df2, df1.serial_number == df2.serial_number_2, "inner")
        return joined.select(
            df1.serial_number,
            "date",
            "next_date",
            (f.col("smart_5_raw_next") - f.col("smart_5_raw")).alias("smart_delta"),
        ).count()

    def quarterly_stats(self):
        df = self._read_multiple_csv_files()
        return (
            df.groupBy("model")
            .agg(
                f.mean("smart_5_raw").alias("mean_5"),
                f.mean("smart_187_raw").alias("mean_187"),
                f.sum("failure").alias("failures"),
                f.count("*").alias("count"),
            )
            .withColumn("failure_rate", f.col("failures") / f.col("count"))
        ).count()
