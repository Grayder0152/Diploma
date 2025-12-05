import logging


class Logger:
    """Custom logger for benchmarking framework."""

    def __init__(self, name: str = "benchmark", level: int = logging.INFO):
        """
        Initializes a logger instance.

        Args:
            name (str): Logger name.
            level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
        """

        self.name = name
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        if not self._logger.handlers:
            formatter = logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)

    def debug(self, msg: str): self._logger.debug(msg)

    def info(self, msg: str): self._logger.info(msg)

    def warning(self, msg: str): self._logger.warning(msg)

    def error(self, msg: str): self._logger.error(msg)

    def critical(self, msg: str): self._logger.critical(msg)
