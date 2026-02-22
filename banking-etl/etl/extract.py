"""
Banking ETL Pipeline - Data Extraction Module

This module extracts data from source files (Excel, CSV) into a staging area.
Handles multiple file formats and validates extracted data.

Author: Data Engineering Team
Date: 2024
"""

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

from etl.config import Config
from etl.database import get_db_session
from etl.logger import get_logger

logger = get_logger(__name__)


class DataExtractor:
    """
    Extracts banking data from source files.
    Handles Excel and CSV formats with validation.
    """

    def __init__(self, input_path: str = None):
        """
        Initialize the data extractor.

        Args:
            input_path: Path to the source data file
        """
        self.input_path = input_path or Config.DATA_INPUT_PATH
        self.batch_size = Config.BATCH_SIZE
        self.rows_extracted = 0
        self.file_hash = None

    def _compute_file_hash(self) -> str:
        """
        Compute MD5 hash of the source file for deduplication.

        Returns:
            str: File hash
        """
        hash_md5 = hashlib.md5()
        with open(self.input_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize source columns into the canonical staging schema.

        The source Excel uses human-friendly headers (spaces/title-case),
        while the warehouse expects snake_case operational columns.
        """
        df = df.copy()
        df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]

        column_map = {
            'Customer ID': 'customer_id',
            'TransactionID': 'transaction_id',
            'Transaction Date': 'transaction_date',
            'Transaction Amount': 'transaction_amount',
            'Transaction Type': 'transaction_type',
            'Account Type': 'account_type',
            'Email': 'customer_email',
            'Contact Number': 'customer_phone',
            'Age': 'customer_age',
            'Branch ID': 'branch_id',
            'City': 'branch_location',
            'Loan Status': 'account_status',
            'First Name': 'first_name',
            'Last Name': 'last_name',
            'Loan Type': 'loan_type',
            'Card Type': 'card_type',
        }
        df = df.rename(columns=column_map).copy()

        if {'first_name', 'last_name'}.issubset(df.columns):
            df['customer_name'] = (
                df['first_name'].fillna('').astype(str).str.strip() + ' ' +
                df['last_name'].fillna('').astype(str).str.strip()
            ).str.strip()
        else:
            df['customer_name'] = None

        account_type = df['account_type'] if 'account_type' in df.columns else pd.Series(index=df.index, dtype='object')
        loan_type = df['loan_type'] if 'loan_type' in df.columns else pd.Series(index=df.index, dtype='object')
        card_type = df['card_type'] if 'card_type' in df.columns else pd.Series(index=df.index, dtype='object')

        if 'product_type' not in df.columns:
            df['product_type'] = account_type.fillna(loan_type).fillna(card_type).fillna('UNCLASSIFIED')

        if 'account_status' not in df.columns:
            df['account_status'] = 'UNKNOWN'

        if 'customer_segment' not in df.columns:
            df['customer_segment'] = 'GENERAL'

        required_columns = [
            'customer_id',
            'transaction_id',
            'transaction_date',
            'product_type',
            'transaction_amount',
            'transaction_type',
            'account_type',
            'account_status',
            'customer_name',
            'customer_email',
            'customer_phone',
            'customer_age',
            'customer_segment',
            'branch_id',
            'branch_location',
        ]

        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        return df[required_columns]

    def _validate_input_file(self) -> bool:
        """
        Validate that the source file exists and is readable.

        Returns:
            bool: True if file is valid
        """
        if not os.path.exists(self.input_path):
            logger.error(f"Input file not found: {self.input_path}")
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

        if not os.access(self.input_path, os.R_OK):
            logger.error(f"Input file is not readable: {self.input_path}")
            raise PermissionError(f"Input file is not readable: {self.input_path}")

        file_size_mb = os.path.getsize(self.input_path) / (1024 * 1024)
        logger.info(f"Input file found and readable. Size: {file_size_mb:.2f} MB")

        return True

    def extract_data(self) -> Tuple[pd.DataFrame, dict]:
        """
        Extract data from the source file.

        Returns:
            Tuple[pd.DataFrame, dict]: Extracted dataframe and metadata
        """
        try:
            logger.info(f"Starting data extraction from {self.input_path}")
            self._validate_input_file()

            # Compute file hash
            self.file_hash = self._compute_file_hash()
            logger.info(f"Source file hash: {self.file_hash}")

            # Determine file type and read accordingly
            file_ext = Path(self.input_path).suffix.lower()

            if file_ext in ['.xlsx', '.xls']:
                logger.info("Detected Excel file format")
                df = pd.read_excel(self.input_path)
            elif file_ext == '.csv':
                logger.info("Detected CSV file format")
                df = pd.read_csv(self.input_path)
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")

            # Normalize raw source headers into canonical staging fields.
            df = self._normalize_columns(df)

            if df.empty:
                raise ValueError("Extracted dataframe is empty")

            logger.info(f"Raw data shape: {df.shape}")
            logger.info(f"Columns: {df.columns.tolist()}")

            df = df.dropna(how='all')

            self.rows_extracted = len(df)
            logger.info(f"Rows extracted: {self.rows_extracted}")

            # ================================================================
            # Extract Metadata
            # ================================================================
            metadata = {
                'extraction_timestamp': datetime.utcnow().isoformat(),
                'source_file': os.path.basename(self.input_path),
                'source_file_hash': self.file_hash,
                'rows_extracted': self.rows_extracted,
                'columns': df.columns.tolist(),
                'schema': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'null_counts': df.isnull().sum().to_dict(),
                'file_size_bytes': os.path.getsize(self.input_path),
            }

            logger.info(f"Extraction metadata: {metadata}")

            return df, metadata

        except Exception as e:
            logger.error(f"Data extraction failed: {str(e)}", exc_info=True)
            raise

    def load_to_staging(self, df: pd.DataFrame, metadata: dict) -> Tuple[int, int]:
        """
        Load extracted data into staging table in PostgreSQL.

        Args:
            df: Source dataframe
            metadata: Extraction metadata

        Returns:
            Tuple[int, int]: Rows loaded and rows rejected
        """
        try:
            logger.info(f"Loading {len(df)} rows to staging table")

            rows_loaded = 0
            rows_rejected = 0

            # Connect to database
            with get_db_session() as session:
                # Load data in batches
                for batch_idx in range(0, len(df), self.batch_size):
                    batch_df = df.iloc[batch_idx:batch_idx + self.batch_size].copy()

                    try:
                        # Construct insert query
                        batch_df['source_file_name'] = metadata['source_file']
                        batch_df['source_file_hash'] = metadata['source_file_hash']
                        batch_df['is_processed'] = False
                        batch_df['raw_data'] = batch_df.apply(
                            lambda row: json.dumps(row.to_dict(), default=str),
                            axis=1
                        )

                        # Use pandas to_sql for bulk insert
                        batch_df.to_sql(
                            'raw_banking_data',
                            con=session.connection(),
                            schema=Config.STAGING_SCHEMA_NAME,
                            if_exists='append',
                            index=False,
                            method='multi',
                            chunksize=1000
                        )

                        rows_loaded += len(batch_df)
                        logger.debug(f"Batch {batch_idx // self.batch_size + 1} loaded: {len(batch_df)} rows")

                    except Exception as batch_error:
                        rows_rejected += len(batch_df)
                        logger.error(f"Batch loading failed: {str(batch_error)}")

            logger.info(f"Staging load complete: {rows_loaded} rows loaded, {rows_rejected} rows rejected")

            return rows_loaded, rows_rejected

        except Exception as e:
            logger.error(f"Failed to load to staging: {str(e)}", exc_info=True)
            raise


def extract_and_stage_data() -> Tuple[int, int, dict]:
    """
    Orchestrate extraction and staging of banking data.

    Returns:
        Tuple[int, int, dict]: Rows extracted, rows loaded, and metadata
    """
    extractor = DataExtractor()
    df, metadata = extractor.extract_data()
    rows_loaded, rows_rejected = extractor.load_to_staging(df, metadata)

    logger.info(f"Extraction and staging complete")
    return extractor.rows_extracted, rows_loaded, metadata


if __name__ == '__main__':
    # Test extraction
    try:
        total_rows, loaded_rows, meta = extract_and_stage_data()
        print(f"Extraction successful: {total_rows} rows extracted, {loaded_rows} rows loaded")
        print(f"Metadata: {meta}")
    except Exception as e:
        print(f"Extraction failed: {e}")
