"""
Banking ETL Pipeline - Configuration Module

This module manages all configuration settings for the ETL pipeline,
including database connections, paths, and quality thresholds.

Author: Data Engineering Team
Date: 2024
"""

import os
from urllib.parse import urlparse
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, Any
import logging

# Load environment variables from .env file
load_dotenv()


class Config:
    """
    Central configuration class for the banking ETL pipeline.
    Retrieves all settings from environment variables.
    """

    # ========================================================================
    # PostgreSQL Database Configuration
    # ========================================================================
    POSTGRES_USER: str = os.getenv('POSTGRES_USER', 'airflow')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD', 'airflow_secure_password_123')
    POSTGRES_DB: str = os.getenv('POSTGRES_DB', 'banking_warehouse')
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST', 'postgres')
    POSTGRES_PORT: int = int(os.getenv('POSTGRES_PORT', '5432'))

    # Canonical DB URL derived from POSTGRES_* to avoid credential drift.
    CANONICAL_DATABASE_URL: str = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    DATABASE_URL_FROM_ENV: str = os.getenv('DATABASE_URL', '')

    # Database URL for SQLAlchemy
    DATABASE_URL: str = CANONICAL_DATABASE_URL
    if DATABASE_URL_FROM_ENV:
        try:
            parsed = urlparse(DATABASE_URL_FROM_ENV)
            db_name = parsed.path.lstrip('/')
            env_matches_canonical = (
                parsed.username == POSTGRES_USER
                and parsed.password == POSTGRES_PASSWORD
                and parsed.hostname == POSTGRES_HOST
                and (parsed.port or 5432) == POSTGRES_PORT
                and db_name == POSTGRES_DB
            )
            if env_matches_canonical:
                DATABASE_URL = DATABASE_URL_FROM_ENV
        except Exception:
            # Keep canonical URL if DATABASE_URL cannot be parsed.
            pass

    # ========================================================================
    # ETL Pipeline Configuration
    # ========================================================================
    DATA_INPUT_PATH: str = os.getenv(
        'DATA_INPUT_PATH',
        '/data/Comprehensive_Banking_Database.csv.xlsx'
    )
    DW_SCHEMA_NAME: str = os.getenv('DW_SCHEMA_NAME', 'banking_dw')
    STAGING_SCHEMA_NAME: str = os.getenv('STAGING_SCHEMA_NAME', 'staging')

    # Project root and log directories
    PROJECT_ROOT: Path = Path(__file__).parent.parent
    LOG_DIR: Path = PROJECT_ROOT / 'logs'
    LOG_DIR.mkdir(exist_ok=True)

    # ========================================================================
    # Logging Configuration
    # ========================================================================
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT: str = os.getenv(
        'LOG_FORMAT',
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    LOG_FILE: str = str(LOG_DIR / 'etl_pipeline.log')

    # ========================================================================
    # Data Quality Configuration
    # ========================================================================
    MIN_COMPLETENESS_PCT: float = float(os.getenv('MIN_COMPLETENESS_PCT', '95'))
    MAX_NULL_PCT: float = float(os.getenv('MAX_NULL_PCT', '5'))
    DUPLICATE_CHECK_ENABLED: bool = os.getenv('DUPLICATE_CHECK_ENABLED', 'True') == 'True'

    # ========================================================================
    # Processing Configuration
    # ========================================================================
    BATCH_SIZE: int = 5000  # Number of rows to process in each batch
    MAX_WORKERS: int = 4    # Number of parallel workers
    CHUNK_SIZE: int = 10000  # DataFrame chunk size

    # ========================================================================
    # Airflow Configuration
    # ========================================================================
    AIRFLOW_HOME: str = os.getenv('AIRFLOW_HOME', '/home/airflow/airflow')
    AIRFLOW_DAG_ID: str = 'banking_etl_pipeline'
    AIRFLOW_OWNER: str = 'data-engineering'
    AIRFLOW_RETRY_ATTEMPTS: int = 3
    AIRFLOW_RETRY_DELAY_SECONDS: int = 300
    ALERT_EMAIL: str = os.getenv('ALERT_EMAIL', '')

    @classmethod
    def validate_config(cls) -> bool:
        """
        Validate that all critical configuration is present.

        Returns:
            bool: True if all critical configs are present, raises Exception otherwise
        """
        critical_configs = [
            cls.POSTGRES_USER,
            cls.POSTGRES_PASSWORD,
            cls.POSTGRES_DB,
            cls.POSTGRES_HOST,
        ]

        if not all(critical_configs):
            raise ValueError(
                "Critical configuration missing. Check .env file for "
                "POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST"
            )

        return True

    @classmethod
    def get_config_dict(cls) -> Dict[str, Any]:
        """
        Return all configuration as a dictionary.

        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        return {
            'database': {
                'user': cls.POSTGRES_USER,
                'password': cls.POSTGRES_PASSWORD,
                'database': cls.POSTGRES_DB,
                'host': cls.POSTGRES_HOST,
                'port': cls.POSTGRES_PORT,
                'url': cls.DATABASE_URL,
            },
            'etl': {
                'data_input_path': cls.DATA_INPUT_PATH,
                'dw_schema': cls.DW_SCHEMA_NAME,
                'staging_schema': cls.STAGING_SCHEMA_NAME,
                'batch_size': cls.BATCH_SIZE,
            },
            'logging': {
                'level': cls.LOG_LEVEL,
                'format': cls.LOG_FORMAT,
                'log_file': cls.LOG_FILE,
            },
            'data_quality': {
                'min_completeness': cls.MIN_COMPLETENESS_PCT,
                'max_null_pct': cls.MAX_NULL_PCT,
                'duplicate_check_enabled': cls.DUPLICATE_CHECK_ENABLED,
            },
        }

    @staticmethod
    def log_config(logger: logging.Logger) -> None:
        """
        Log all configuration settings (with password masked).

        Args:
            logger: Logger instance
        """
        config_dict = Config.get_config_dict()
        # Mask password in logs
        config_dict['database']['password'] = '***MASKED***'
        logger.info(f"Configuration loaded: {config_dict}")


if __name__ == '__main__':
    # Test configuration
    print(Config.get_config_dict())
