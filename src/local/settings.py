import os
from enum import Enum
from pathlib import Path

ROOT_FOLDER = Path(__file__).resolve().parent.parent
DATA_FOLDER = os.path.join(ROOT_FOLDER, "..", "data")

REPORT_FOLDER_NAME = os.path.join(ROOT_FOLDER, "..", "reports")
REPORT_FILE_NAME = "benchmark_comparison.csv"

class DataFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"
    DELTA = "delta"


class EnginesEnum(Enum):
    POLARS = "polars"
    SPARK = "spark"
