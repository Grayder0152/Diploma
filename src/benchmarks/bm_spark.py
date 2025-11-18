from pyspark.sql import SparkSession, functions as f
from src.benchmarks.bm_base import BaseBenchmark
from src.configs import DataConfig


class SparkBenchmark(BaseBenchmark):
    """Scalable benchmark suite for Apache Spark with diverse task coverage."""

    def __init__(self, data_config: DataConfig, tasks: list[str], spark: SparkSession):
        super().__init__(data_config, tasks)
        self.spark = spark

    def _read_multiple_csv_files(self):
        """Read multiple CSV files based on configuration."""

        return self.spark.read.csv(self.data_config.base_dir, header=True, inferSchema=True)

    def group_by_model(self):
        """
        Benchmark type: Aggregation + statistics.
        Measures group-by and aggregation kernel efficiency.
        """

        df = self._read_multiple_csv_files()
        result = (
            df.groupBy("model")
            .agg(
                f.mean("smart_5_raw").alias("avg_smart_5_raw"),
                f.stddev("smart_5_raw").alias("std_smart_5_raw"),
                f.countDistinct("serial_number").alias("unique_disks"),
                f.sum("failure").alias("failures_total"),
            )
            .orderBy(f.desc("avg_smart_5_raw"))
        )
        return result.count()

    def union_and_aggregate(self):
        """
        Benchmark type: Multi-file union + global reduction.
        Tests concatenation (unionAll) performance and overall aggregation.
        """

        files = self.data_config.files
        base_df = self.spark.read.csv(files[0], header=True, inferSchema=True)

        for path in files[1:]:
            next_df = self.spark.read.csv(path, header=True, inferSchema=True)
            base_df = base_df.unionByName(next_df, allowMissingColumns=True)

        result = (
            base_df.agg(
                f.mean("smart_5_raw").alias("mean_smart_5"),
                f.mean("smart_187_raw").alias("mean_smart_187"),
                f.sum("failure").alias("total_failures"),
                f.count("*").alias("records_total"),
            )
            .withColumn(
                "failure_rate",
                f.col("total_failures") / f.col("records_total"),
            )
        )
        return result.count()

    def join_adjacent_days(self):
        """
        Benchmark type: Join between adjacent files.
        Tests join performance and shuffle behavior (Spark equivalent of Polars version).
        """

        files = sorted(self.data_config.files)
        if len(files) < 2:
            raise ValueError("At least two files are required for join test.")

        base_df = (
            self.spark.read.csv(files[0], header=True, inferSchema=True)
            .select("serial_number", "smart_5_raw", "date")
        )

        for i, next_file in enumerate(files[1:], start=1):
            next_df = (
                self.spark.read.csv(next_file, header=True, inferSchema=True)
                .select(
                    f.col("serial_number").alias("serial_number_next"),
                    f.col("smart_5_raw").alias(f"smart_5_raw_next_{i}"),
                    f.col("date").alias(f"date_next_{i}")
                )
            )

            base_df = (
                base_df.join(
                    next_df,
                    base_df.serial_number == next_df.serial_number_next,
                    "inner"
                )
                .drop("serial_number_next")
            )

        last_suffix = f"_{i}"
        base_df = base_df.withColumn(
            f"smart_delta{last_suffix}",
            f.col(f"smart_5_raw_next{last_suffix}") - f.col("smart_5_raw")
        )

        return base_df.count()

    def quarterly_stats(self):
        """
        Benchmark type: Multi-aggregation and join.
        Simulates analytical workloads with multiple stages.
        """
        df = self._read_multiple_csv_files()

        mean_stats = (
            df.groupBy("model")
            .agg(
                f.mean("smart_5_raw").alias("mean_5"),
                f.mean("smart_187_raw").alias("mean_187"),
                f.count("*").alias("count"),
            )
        )

        failure_stats = (
            df.groupBy("model")
            .agg(f.sum("failure").alias("failures"))
        )

        joined = mean_stats.join(failure_stats, on="model", how="inner").withColumn(
            "failure_rate", f.col("failures") / f.col("count")
        )

        return joined.orderBy(f.desc("failure_rate")).count()
