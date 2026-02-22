"""
ETL Package Initialization

This module initializes the ETL package and exposes main functions.
"""

from etl.config import Config
from etl.logger import get_logger, ETLLogger
from etl.database import DatabaseConnection, get_db_session
from etl.extract import DataExtractor, extract_and_stage_data
from etl.transform import DataTransformer, transform_banking_data
from etl.load import DataLoader, load_banking_data
from etl.quality_checks import DataQualityChecker, run_data_quality_checks

__version__ = '1.0.0'
__author__ = 'Data Engineering Team'

__all__ = [
    'Config',
    'get_logger',
    'ETLLogger',
    'DatabaseConnection',
    'get_db_session',
    'DataExtractor',
    'extract_and_stage_data',
    'DataTransformer',
    'transform_banking_data',
    'DataLoader',
    'load_banking_data',
    'DataQualityChecker',
    'run_data_quality_checks',
]
