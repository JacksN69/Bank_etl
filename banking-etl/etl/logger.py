"""
Banking ETL Pipeline - Logging Module

This module sets up centralized logging for the entire ETL pipeline.
Logs are written to both console and file with proper formatting.

Author: Data Engineering Team
Date: 2024
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Optional
from etl.config import Config


class ETLLogger:
    """
    Centralized logger for ETL pipeline operations.
    Handles both console and file logging with appropriate formatting.
    """

    _instance = None
    _loggers = {}

    def __new__(cls):
        """Implement singleton pattern for logger."""
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
        return cls._instance

    @staticmethod
    def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
        """
        Get or create a logger with the specified name.

        Args:
            name: Module name for the logger
            level: Optional log level (defaults to config value)

        Returns:
            logging.Logger: Configured logger instance
        """
        if name in ETLLogger._loggers:
            return ETLLogger._loggers[name]

        logger = logging.getLogger(name)
        log_level = level or Config.LOG_LEVEL

        # Avoid duplicate handlers
        if logger.handlers:
            return logger

        logger.setLevel(getattr(logging, log_level.upper()))

        # Create formatters
        formatter = logging.Formatter(Config.LOG_FORMAT)

        # ====================================================================
        # Console Handler (INFO and above)
        # ====================================================================
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # ====================================================================
        # File Handler (DEBUG and above, with rotation)
        # ====================================================================
        log_file = Path(Config.LOG_FILE)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            Config.LOG_FILE,
            maxBytes=10485760,  # 10 MB
            backupCount=10     # Keep 10 backup files
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # ====================================================================
        # Error File Handler (ERROR and above)
        # ====================================================================
        error_log_file = log_file.parent / 'etl_errors.log'
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=10485760,  # 10 MB
            backupCount=5
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)

        ETLLogger._loggers[name] = logger
        return logger


def get_logger(name: str) -> logging.Logger:
    """
    Convenience function to get a logger.

    Args:
        name: Module name

    Returns:
        logging.Logger: Configured logger instance
    """
    return ETLLogger.get_logger(name)


# Initialize root logger
root_logger = ETLLogger.get_logger('banking_etl')
root_logger.info("Logging initialized for banking ETL pipeline")
