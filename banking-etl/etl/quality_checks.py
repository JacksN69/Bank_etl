"""
Banking ETL Pipeline - Data Quality Framework

This module implements comprehensive data quality checks and metrics.
Validates completeness, null percentages, duplicates, and schema compliance.

Author: Data Engineering Team
Date: 2024
"""

import pandas as pd
from typing import Dict, List, Tuple
from sqlalchemy import text
from etl.config import Config
from etl.logger import get_logger
from etl.database import get_db_session

logger = get_logger(__name__)


class DataQualityChecker:
    """
    Comprehensive data quality validation framework.
    Tracks and reports quality metrics for each ETL run.
    """

    def __init__(self, batch_id: str):
        """
        Initialize quality checker.

        Args:
            batch_id: Unique identifier for ETL batch
        """
        self.batch_id = batch_id
        self.quality_metrics = []
        self.quality_status = 'PASS'

    def _update_status(self, candidate: str) -> None:
        """Update overall status while preserving severity ordering."""
        order = {'PASS': 0, 'WARNING': 1, 'FAIL': 2}
        if order.get(candidate, 0) > order.get(self.quality_status, 0):
            self.quality_status = candidate

    # ========================================================================
    # Quality Check Methods
    # ========================================================================

    def check_completeness(self, df: pd.DataFrame, table_name: str) -> Dict:
        """
        Check data completeness (percentage of non-null values).

        Args:
            df: Dataframe to check
            table_name: Name of table being checked

        Returns:
            Dict: Completeness metrics
        """
        try:
            logger.info(f"Checking completeness for {table_name}")

            total_cells = df.size
            null_cells = df.isnull().sum().sum()
            completeness_pct = ((total_cells - null_cells) / total_cells * 100) if total_cells > 0 else 0

            metric_status = 'PASS' if completeness_pct >= Config.MIN_COMPLETENESS_PCT else 'FAIL'

            metric = {
                'table': table_name,
                'metric': 'COMPLETENESS_PCT',
                'value': round(completeness_pct, 2),
                'threshold': Config.MIN_COMPLETENESS_PCT,
                'status': metric_status,
                'record_count': len(df),
            }

            logger.info(f"Completeness: {completeness_pct:.2f}% (threshold: {Config.MIN_COMPLETENESS_PCT}%)")

            self._update_status(metric_status)

            return metric

        except Exception as e:
            logger.error(f"Completeness check failed: {str(e)}", exc_info=True)
            return {'table': table_name, 'metric': 'COMPLETENESS_PCT', 'status': 'ERROR', 'error': str(e)}

    def check_null_percentages(self, df: pd.DataFrame, table_name: str) -> Dict:
        """
        Check null percentage per column.

        Args:
            df: Dataframe to check
            table_name: Name of table being checked

        Returns:
            Dict: Null percentage metrics
        """
        try:
            logger.info(f"Checking null percentages for {table_name}")

            if len(df) == 0:
                null_pcts = {col: 100.0 for col in df.columns}
            else:
                null_pcts = (df.isnull().sum() / len(df) * 100).to_dict()

            # Calculate average null percentage
            avg_null_pct = sum(null_pcts.values()) / len(null_pcts) if null_pcts else 0

            metric_status = 'PASS' if avg_null_pct <= Config.MAX_NULL_PCT else 'FAIL'

            metric = {
                'table': table_name,
                'metric': 'NULL_PERCENTAGE',
                'value': round(avg_null_pct, 2),
                'threshold': Config.MAX_NULL_PCT,
                'status': metric_status,
                'record_count': len(df),
                'column_details': null_pcts,
            }

            logger.info(f"Average null percentage: {avg_null_pct:.2f}% (max allowed: {Config.MAX_NULL_PCT}%)")
            if null_pcts:
                logger.debug(f"Null percentages by column: {null_pcts}")

            self._update_status(metric_status)

            return metric

        except Exception as e:
            logger.error(f"Null percentage check failed: {str(e)}", exc_info=True)
            return {'table': table_name, 'metric': 'NULL_PERCENTAGE', 'status': 'ERROR', 'error': str(e)}

    def check_duplicates(self, df: pd.DataFrame, table_name: str, key_columns: List[str]) -> Dict:
        """
        Check for duplicate records.

        Args:
            df: Dataframe to check
            table_name: Name of table being checked
            key_columns: Columns to check for duplicates

        Returns:
            Dict: Duplicate metrics
        """
        try:
            logger.info(f"Checking duplicates for {table_name} using columns: {key_columns}")

            if not all(col in df.columns for col in key_columns):
                logger.warning(f"Some key columns not found in dataframe")
                return {
                    'table': table_name,
                    'metric': 'DUPLICATES',
                    'status': 'SKIPPED',
                    'reason': 'Key columns not found'
                }

            duplicate_count = df.duplicated(subset=key_columns, keep=False).sum()
            duplicate_pct = (duplicate_count / len(df) * 100) if len(df) > 0 else 0

            metric_status = 'PASS' if duplicate_count == 0 else 'WARNING'

            metric = {
                'table': table_name,
                'metric': 'DUPLICATES',
                'value': duplicate_count,
                'percentage': round(duplicate_pct, 2),
                'status': metric_status,
                'record_count': len(df),
                'key_columns': key_columns,
            }

            logger.info(f"Duplicate records found: {duplicate_count} ({duplicate_pct:.2f}%)")

            self._update_status(metric_status)

            return metric

        except Exception as e:
            logger.error(f"Duplicate check failed: {str(e)}", exc_info=True)
            return {'table': table_name, 'metric': 'DUPLICATES', 'status': 'ERROR', 'error': str(e)}

    def check_schema_validation(self, df: pd.DataFrame, table_name: str, expected_schema: Dict) -> Dict:
        """
        Validate dataframe schema against expected schema.

        Args:
            df: Dataframe to check
            table_name: Name of table being checked
            expected_schema: Dict of column_name -> expected_type

        Returns:
            Dict: Schema validation metrics
        """
        try:
            logger.info(f"Validating schema for {table_name}")

            schema_errors = []

            for column, expected_type in expected_schema.items():
                if column not in df.columns:
                    schema_errors.append(f"Missing column: {column}")
                else:
                    # Simple type checking
                    actual_type = str(df[column].dtype)
                    if expected_type not in actual_type:
                        schema_errors.append(f"Column {column}: expected {expected_type}, got {actual_type}")

            metric_status = 'PASS' if not schema_errors else 'FAIL'

            metric = {
                'table': table_name,
                'metric': 'SCHEMA_VALIDATION',
                'status': metric_status,
                'record_count': len(df),
                'errors': schema_errors,
            }

            logger.info(f"Schema validation: {len(schema_errors)} errors")
            if schema_errors:
                logger.warning(f"Schema errors: {schema_errors}")

            self._update_status(metric_status)

            return metric

        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}", exc_info=True)
            return {'table': table_name, 'metric': 'SCHEMA_VALIDATION', 'status': 'ERROR', 'error': str(e)}

    def check_referential_integrity(self, table_name: str, fk_checks: List[Dict]) -> Dict:
        """
        Check referential integrity (foreign key constraints).

        Args:
            table_name: Name of table being checked
            fk_checks: List of FK validation queries

        Returns:
            Dict: Referential integrity metrics
        """
        try:
            logger.info(f"Checking referential integrity for {table_name}")

            referential_errors = []

            with get_db_session() as session:
                for fk_check in fk_checks:
                    # Simple validation: check if foreign key values exist in referenced table
                    validation_query = text(fk_check['query'])
                    result = session.execute(validation_query)
                    orphaned_count = result.scalar() or 0

                    if orphaned_count > 0:
                        referential_errors.append({
                            'name': fk_check['name'],
                            'orphaned_records': orphaned_count
                        })

            metric_status = 'PASS' if not referential_errors else 'WARNING'

            metric = {
                'table': table_name,
                'metric': 'REFERENTIAL_INTEGRITY',
                'status': metric_status,
                'errors': referential_errors,
            }

            logger.info(f"Referential integrity: {len(referential_errors)} errors")

            self._update_status(metric_status)

            return metric

        except Exception as e:
            logger.error(f"Referential integrity check failed: {str(e)}", exc_info=True)
            return {'table': table_name, 'metric': 'REFERENTIAL_INTEGRITY', 'status': 'ERROR', 'error': str(e)}

    def store_quality_metrics(self) -> bool:
        """
        Store quality metrics in audit table.

        Returns:
            bool: True if metrics stored successfully
        """
        try:
            logger.info(f"Storing quality metrics for batch {self.batch_id}")

            with get_db_session() as session:
                for metric in self.quality_metrics:
                    insert_query = text(f"""
                        INSERT INTO audit.data_quality_metrics
                        (etl_batch_id, table_name, metric_name, metric_value, metric_percentage, 
                         record_count, quality_status, metric_description)
                        VALUES
                        (:batch_id, :table, :metric, :value, :percentage, :count, :status, :description)
                    """)

                    session.execute(insert_query, {
                        'batch_id': self.batch_id,
                        'table': metric.get('table', 'UNKNOWN'),
                        'metric': metric.get('metric', 'UNKNOWN'),
                        'value': metric.get('value'),
                        'percentage': metric.get('percentage', metric.get('value')),
                        'count': metric.get('record_count', 0),
                        'status': metric.get('status', 'UNKNOWN'),
                        'description': str(metric),
                    })

            logger.info(f"Stored {len(self.quality_metrics)} quality metrics")
            return True

        except Exception as e:
            logger.error(f"Failed to store quality metrics: {str(e)}", exc_info=True)
            return False

    def run_quality_checks(self, df: pd.DataFrame = None) -> Tuple[bool, List[Dict]]:
        """
        Run comprehensive quality checks.

        Args:
            df: Optional dataframe to check

        Returns:
            Tuple[bool, List[Dict]]: Overall quality status and metrics list
        """
        try:
            logger.info(f"Starting quality checks for batch {self.batch_id}")

            if df is not None:
                # Check caller-provided data
                self.quality_metrics.append(
                    self.check_completeness(df, 'raw_banking_data')
                )

                self.quality_metrics.append(
                    self.check_null_percentages(df, 'raw_banking_data')
                )

                self.quality_metrics.append(
                    self.check_duplicates(
                        df,
                        'raw_banking_data',
                        ['customer_id', 'transaction_id', 'transaction_date']
                    )
                )
            else:
                # Check loaded warehouse/staging tables when running from Airflow DAG.
                with get_db_session() as session:
                    fact_df = pd.read_sql(
                        text(f"""
                            SELECT transaction_id, customer_key, product_key, time_key, transaction_amount, transaction_date
                            FROM {Config.DW_SCHEMA_NAME}.fact_transactions
                            ORDER BY created_at DESC
                            LIMIT 50000
                        """),
                        session.connection()
                    )
                    cleaned_df = pd.read_sql(
                        text(f"""
                            SELECT customer_id, transaction_id, transaction_date, transaction_amount
                            FROM {Config.STAGING_SCHEMA_NAME}.cleaned_banking_data
                            ORDER BY created_at DESC
                            LIMIT 50000
                        """),
                        session.connection()
                    )

                self.quality_metrics.append(self.check_completeness(fact_df, 'fact_transactions'))
                self.quality_metrics.append(self.check_null_percentages(fact_df, 'fact_transactions'))
                self.quality_metrics.append(
                    self.check_duplicates(
                        fact_df,
                        'fact_transactions',
                        ['transaction_id']
                    )
                )

                self.quality_metrics.append(self.check_completeness(cleaned_df, 'cleaned_banking_data'))
                self.quality_metrics.append(self.check_null_percentages(cleaned_df, 'cleaned_banking_data'))
                self.quality_metrics.append(
                    self.check_duplicates(
                        cleaned_df,
                        'cleaned_banking_data',
                        ['customer_id', 'transaction_id', 'transaction_date']
                    )
                )

            # Store metrics in database
            self.store_quality_metrics()

            logger.info(f"Quality checks complete. Overall status: {self.quality_status}")

            return self.quality_status == 'PASS', self.quality_metrics

        except Exception as e:
            logger.error(f"Quality check execution failed: {str(e)}", exc_info=True)
            return False, []


def run_data_quality_checks(batch_id: str, df: pd.DataFrame = None) -> Tuple[bool, List[Dict]]:
    """
    Convenience function to run quality checks.

    Args:
        batch_id: ETL batch identifier
        df: Optional dataframe to check

    Returns:
        Tuple[bool, List[Dict]]: Quality status and metrics
    """
    checker = DataQualityChecker(batch_id)
    return checker.run_quality_checks(df)


if __name__ == '__main__':
    # Test quality checks
    checker = DataQualityChecker('TEST_BATCH_001')
    quality_pass, metrics = checker.run_quality_checks()
    print(f"Quality status: {'PASS' if quality_pass else 'FAIL'}")
    print(f"Metrics: {metrics}")
