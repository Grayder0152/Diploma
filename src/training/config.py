import os

DATA_FOLDER = 'data'
MODELS_FOLDER = os.path.join(DATA_FOLDER, "models")
# FEATURES = (
#     "task_name",
#     "data_size", "row_count", "column_count",
#     "cpu", "memory", "instance_count"
# )
# SELECT_COLUMNS = [
#     "task_name", "data_size",
#     "cpu", "memory", "instance_count",
#     "duration"
# ]
# CATEGORICAL_FEATURE = ("task_name", )

FEATURES = (
    "task_name", "instance_type", "instance_count", "data_size"
)
SELECT_COLUMNS = [
    "task_name", "instance_type", "instance_count", "data_size", "duration"
]
CATEGORICAL_FEATURE = ("task_name", "instance_type")

IS_SUCCESS_POLARS_COL = 'success_polars'
IS_SUCCESS_SPARK_COL = 'success_spark'
DURATION_POLARS_COL = 'duration_polars'
DURATION_SPARK_COL = 'duration_spark'

TRAIN_FILE_NAME = "train_benchmarks_3000_new"
TEST_FILE_NAME = "test_benchmarks_new"