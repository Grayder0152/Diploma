import os

import pandas as pd
from datetime import datetime
from typing import Any

from src.logger import Logger
from src.settings import REPORT_FOLDER_NAME


class BenchmarkReporter:
    """Handles summarizing and appending benchmark results."""

    def __init__(self,  output_file_name: str, logger: Logger):
        self.output_path = os.path.join(REPORT_FOLDER_NAME, output_file_name)
        self.log = logger

    def summarize(self, run_result: list[dict[str, Any]]) -> pd.DataFrame:
        """Transform benchmark JSON into a unified CSV with append mode."""

        self.log.info("Preparing benchmark summary table...")

        pivot = self._process_records(run_result)
        self._save_results(pivot)
        return pivot

    def _process_records(self, run_result: list[dict[str, Any]]) -> pd.DataFrame:
        """Flatten JSON structure and compute metrics."""

        records = []
        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for exp in run_result:
            cpu = exp["engine_config"]["cpu_count"]
            mem = exp["engine_config"]["memory_limit_gb"]

            for benchmark in exp["benchmarks"]:
                engine = benchmark["engine"]

                for group in benchmark["tasks_measure"]:
                    base_dir = group["data"]["base_dir"]
                    n_files = group["data"]["n_files"]

                    for task in group["tasks_measure"]:
                        records.append({
                            "date": date_now,
                            "engine": engine,
                            "cpu_count": cpu,
                            "memory_gb": mem,
                            "n_files": n_files,
                            "base_dir": base_dir,
                            "task": task["task_name"],
                            "duration_sec": task["duration"],
                        })

        df = pd.DataFrame(records)
        if df.empty:
            self.log.warning("No records found in run result.")
            return df

        pivot = (
            df.pivot_table(
                index=["date", "cpu_count", "memory_gb", "n_files", "base_dir", "task"],
                columns="engine",
                values="duration_sec"
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )

        if "spark" not in pivot or "polars" not in pivot:
            self.log.error("Both Spark and Polars results must be present in JSON.")
            raise ValueError("Both Spark and Polars results must be present in JSON.")

        pivot["polars(s)"] = pivot["polars"]
        pivot["spark(s)"] = pivot["spark"]
        pivot["speedup (Spark/Polars)"] = pivot["spark"] / pivot["polars"]
        pivot["speedup (Polars/Spark)"] = pivot["polars"] / pivot["spark"]
        pivot["engine_win"] = pivot.apply(
            lambda r: "spark" if r["spark"] < r["polars"] else "polars", axis=1
        )

        pivot = pivot[
            [
                "date", "cpu_count", "memory_gb", "n_files", "base_dir", "task",
                "polars(s)", "spark(s)",
                "speedup (Spark/Polars)", "speedup (Polars/Spark)", "engine_win"
            ]
        ]

        self.log.info(f"Processed {len(pivot)} benchmark entries.")
        return pivot

    def _save_results(self, pivot: pd.DataFrame) -> None:
        """Append or create the CSV summary file."""

        if os.path.exists(self.output_path):
            old = pd.read_csv(self.output_path)
            combined = pd.concat([old, pivot], ignore_index=True)
            combined.to_csv(self.output_path, index=False)
            self.log.info(f"Appended {len(pivot)} new rows to {self.output_path}")
        else:
            os.makedirs(REPORT_FOLDER_NAME, exist_ok=True)
            pivot.to_csv(self.output_path, index=False)
            self.log.info(f"Created new report file: {self.output_path}")
