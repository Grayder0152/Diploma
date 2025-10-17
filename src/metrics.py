import time
from typing import Callable


def measure_time(func: Callable, *args, **kwargs) -> float:
    """
    Measures execution time for a given function.

    Args:
        func (Callable): Function to execute.
        *args: Positional arguments for the function.
        **kwargs: Keyword arguments for the function.

    Returns:
        Any: The return value from the executed function.
    """

    start = time.perf_counter()
    func(*args, **kwargs)
    end = time.perf_counter()
    return round(end - start, 4)

# class MeasureMetrics:
#     """A class for measuring and storing performance metrics of data operations."""
#
#     def __init__(self):
#         """
#         Initializes the PerformanceMetric instance.
#         """
#
#         self.results: dict[str, Any] = {
#             "execution_time_sec": None,
#             "cpu_usage_percent": None,
#             "memory_usage_mb": None,
#             "throughput_rows_per_sec": None,
#         }
#
#     def measure_time(self, func: Callable, *args, **kwargs) -> float:
#         """
#         Measures execution time for a given function.
#
#         Args:
#             func (Callable): Function to execute.
#             *args: Positional arguments for the function.
#             **kwargs: Keyword arguments for the function.
#
#         Returns:
#             Any: The return value from the executed function.
#         """
#
#         start = time.perf_counter()
#         func(*args, **kwargs)
#         end = time.perf_counter()
#         return round(end - start, 4)
#
#     def measure_memory(self) -> float:
#         """
#         Measures the current process memory usage in megabytes.
#
#         Returns:
#             float: Memory usage in MB.
#         """
#
#         process = psutil.Process(os.getpid())
#         mem_mb = round(process.memory_info().rss / (1024 ** 2), 2)
#         self.results["memory_usage_mb"] = mem_mb
#         return mem_mb
#
#     def measure_cpu(self) -> float:
#         """
#         Measures CPU utilization percentage during a short interval.
#
#         Returns:
#             float: CPU usage percentage.
#         """
#
#         cpu_percent = psutil.cpu_percent(interval=0.5)
#         self.results["cpu_usage_percent"] = cpu_percent
#         return cpu_percent
#
#     def calculate_throughput(self, rows_processed: int) -> Optional[float]:
#         """
#         Calculates throughput as rows processed per second.
#
#         Args:
#             rows_processed (int): Number of rows processed during operation.
#
#         Returns:
#             Optional[float]: Throughput (rows/second), or None if execution time is missing.
#         """
#
#         exec_time = self.results.get("execution_time_sec")
#         if exec_time and exec_time > 0:
#             throughput = round(rows_processed / exec_time, 2)
#             self.results["throughput_rows_per_sec"] = throughput
#             return throughput
#         return None
