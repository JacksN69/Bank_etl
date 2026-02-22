"""
Banking ETL Pipeline - Unit Tests

This module contains unit tests for the ETL pipeline components.
Run with: pytest tests/

Author: Data Engineering Team
Date: 2024
"""

import unittest
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add ETL module to path
etl_path = Path(__file__).parent.parent
sys.path.insert(0, str(etl_path))

from etl.config import Config
from etl.logger import get_logger
from etl.utils import generate_batch_id, compute_hash, calculate_metrics, format_duration

logger = get_logger(__name__)


class TestConfig(unittest.TestCase):
    """Test configuration management"""

    def test_config_loaded(self):
        """Test that configuration loads without errors"""
        self.assertIsNotNone(Config.POSTGRES_HOST)
        self.assertIsNotNone(Config.DATABASE_URL)

    def test_config_validation(self):
        """Test configuration validation"""
        self.assertTrue(Config.validate_config())

    def test_config_dict(self):
        """Test configuration dictionary generation"""
        config_dict = Config.get_config_dict()
        self.assertIn('database', config_dict)
        self.assertIn('etl', config_dict)
        self.assertIn('logging', config_dict)
        self.assertIn('data_quality', config_dict)


class TestUtilities(unittest.TestCase):
    """Test utility functions"""

    def test_generate_batch_id(self):
        """Test batch ID generation"""
        batch_id = generate_batch_id()
        self.assertIsNotNone(batch_id)
        self.assertTrue(len(batch_id) > 0)
        # Should be unique
        batch_id2 = generate_batch_id()
        self.assertNotEqual(batch_id, batch_id2)

    def test_compute_hash(self):
        """Test hash computation"""
        data = "test_data"
        hash_value = compute_hash(data)
        self.assertEqual(len(hash_value), 32)  # MD5 hash length
        # Same input should produce same hash
        hash_value2 = compute_hash(data)
        self.assertEqual(hash_value, hash_value2)

    def test_calculate_metrics(self):
        """Test metrics calculation"""
        metrics = calculate_metrics(total=1000, processed=950, failed=50)
        self.assertEqual(metrics['total'], 1000)
        self.assertEqual(metrics['processed'], 950)
        self.assertEqual(metrics['failed'], 50)
        self.assertEqual(metrics['success_rate'], 95.0)
        self.assertEqual(metrics['failure_rate'], 5.0)

    def test_format_duration(self):
        """Test duration formatting"""
        self.assertEqual(format_duration(30), "30s")
        self.assertEqual(format_duration(90), "1m 30s")
        self.assertEqual(format_duration(3661), "1h 1m")


class TestDataValidation(unittest.TestCase):
    """Test data validation functions"""

    def setUp(self):
        """Set up test data"""
        self.sample_df = pd.DataFrame({
            'customer_id': ['C001', 'C002', 'C003'],
            'transaction_id': ['T001', 'T002', 'T003'],
            'transaction_amount': [100.0, 200.0, 150.0],
            'transaction_date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        })

    def test_dataframe_creation(self):
        """Test sample dataframe creation"""
        self.assertEqual(len(self.sample_df), 3)
        self.assertEqual(list(self.sample_df.columns), 
                        ['customer_id', 'transaction_id', 'transaction_amount', 'transaction_date'])

    def test_dataframe_not_empty(self):
        """Test that dataframe is not empty"""
        self.assertFalse(self.sample_df.empty)

    def test_date_format(self):
        """Test date column format"""
        self.assertEqual(self.sample_df['transaction_date'].dtype, 'datetime64[ns]')

    def test_amount_numeric(self):
        """Test amount column is numeric"""
        self.assertTrue(pd.api.types.is_numeric_dtype(self.sample_df['transaction_amount']))


class TestDataQuality(unittest.TestCase):
    """Test data quality checks"""

    def setUp(self):
        """Set up test data"""
        self.sample_df = pd.DataFrame({
            'col1': [1, 2, 3, None],
            'col2': ['a', 'b', 'c', 'd'],
            'col3': [1.1, 2.2, 3.3, 4.4],
        })

    def test_null_count(self):
        """Test null value counting"""
        null_count = self.sample_df.isnull().sum().sum()
        self.assertEqual(null_count, 1)

    def test_completeness(self):
        """Test completeness calculation"""
        total_cells = self.sample_df.size
        null_cells = self.sample_df.isnull().sum().sum()
        completeness = ((total_cells - null_cells) / total_cells * 100)
        self.assertGreater(completeness, 90)

    def test_null_percentage_per_column(self):
        """Test null percentage by column"""
        null_pcts = (self.sample_df.isnull().sum() / len(self.sample_df) * 100).to_dict()
        self.assertEqual(null_pcts['col1'], 25.0)
        self.assertEqual(null_pcts['col2'], 0.0)
        self.assertEqual(null_pcts['col3'], 0.0)


class TestLogging(unittest.TestCase):
    """Test logging functionality"""

    def test_logger_creation(self):
        """Test logger creation"""
        test_logger = get_logger(__name__)
        self.assertIsNotNone(test_logger)

    def test_logger_name(self):
        """Test logger naming"""
        test_logger = get_logger('test_module')
        self.assertEqual(test_logger.name, 'test_module')

    def test_log_level(self):
        """Test log level setting"""
        test_logger = get_logger('test_level')
        self.assertGreater(test_logger.level, 0)


class TestFileOperations(unittest.TestCase):
    """Test file operations"""

    def test_project_root_exists(self):
        """Test that project root exists"""
        self.assertTrue(Config.PROJECT_ROOT.exists())

    def test_log_directory_exists(self):
        """Test that log directory exists or can be created"""
        log_dir = Config.LOG_DIR
        log_dir.mkdir(exist_ok=True)
        self.assertTrue(log_dir.exists())


class TestIntegration(unittest.TestCase):
    """Integration tests for pipeline components"""

    def test_config_can_be_logged(self):
        """Test that config can be logged without errors"""
        try:
            Config.log_config(logger)
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"Config logging failed: {e}")

    def test_batch_id_uniqueness(self):
        """Test that generated batch IDs are unique"""
        batch_ids = [generate_batch_id() for _ in range(100)]
        self.assertEqual(len(batch_ids), len(set(batch_ids)))


def suite():
    """Create test suite"""
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(TestConfig))
    test_suite.addTest(unittest.makeSuite(TestUtilities))
    test_suite.addTest(unittest.makeSuite(TestDataValidation))
    test_suite.addTest(unittest.makeSuite(TestDataQuality))
    test_suite.addTest(unittest.makeSuite(TestLogging))
    test_suite.addTest(unittest.makeSuite(TestFileOperations))
    test_suite.addTest(unittest.makeSuite(TestIntegration))
    return test_suite


if __name__ == '__main__':
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
