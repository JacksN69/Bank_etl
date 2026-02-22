"""
Banking ETL Pipeline - Data Transformation Module

This module transforms raw staging data into the dimensional model.
Performs data cleaning, normalization, and dimension population.

Author: Data Engineering Team
Date: 2024
"""

from typing import Dict, Tuple

import pandas as pd
from sqlalchemy import text

from etl.config import Config
from etl.database import get_db_session
from etl.logger import get_logger

logger = get_logger(__name__)


class DataTransformer:
    """
    Transforms raw banking data into dimensional model format.
    Handles data cleaning, validation, and dimension population.
    """

    def __init__(self):
        """Initialize the data transformer."""
        self.config = Config
        self.rows_transformed = 0
        self.rows_rejected = 0
        self.batch_size = Config.BATCH_SIZE

    def load_staging_data(self) -> pd.DataFrame:
        """
        Load data from staging table.

        Returns:
            pd.DataFrame: Staging data
        """
        try:
            logger.info("Loading staging data from database")

            with get_db_session() as session:
                query = text(f"""
                    SELECT
                        id,
                        customer_id,
                        transaction_id,
                        transaction_date,
                        product_type,
                        transaction_amount,
                        transaction_type,
                        account_type,
                        account_status,
                        customer_name,
                        customer_email,
                        customer_phone,
                        customer_age,
                        customer_segment,
                        branch_id,
                        branch_location
                    FROM {Config.STAGING_SCHEMA_NAME}.raw_banking_data
                    WHERE COALESCE(is_processed, FALSE) = FALSE
                    ORDER BY id
                    LIMIT 200000
                """)

                df = pd.read_sql(query, session.connection())

                logger.info(f"Loaded {len(df)} rows from staging")
                return df

        except Exception as e:
            logger.error(f"Failed to load staging data: {str(e)}", exc_info=True)
            raise

    # ========================================================================
    # Data Cleaning Operations
    # ========================================================================

    def clean_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize date fields.

        Args:
            df: Source dataframe

        Returns:
            pd.DataFrame: Dataframe with cleaned dates
        """
        try:
            logger.info("Cleaning date fields")

            if 'transaction_date' in df.columns:
                # Try multiple date formats
                for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        df['transaction_date'] = pd.to_datetime(
                            df['transaction_date'],
                            format=date_format
                        )
                        break
                    except:
                        continue

                # If still not converted, use automatic parsing
                if df['transaction_date'].dtype != 'datetime64[ns]':
                    df['transaction_date'] = pd.to_datetime(
                        df['transaction_date'],
                        errors='coerce'
                    )

                # Remove rows with invalid dates
                initial_len = len(df)
                df = df.dropna(subset=['transaction_date'])
                logger.info(f"Removed {initial_len - len(df)} rows with invalid dates")

            return df

        except Exception as e:
            logger.error(f"Date cleaning failed: {str(e)}", exc_info=True)
            raise

    def clean_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate transaction amounts.

        Args:
            df: Source dataframe

        Returns:
            pd.DataFrame: Dataframe with cleaned amounts
        """
        try:
            logger.info("Cleaning transaction amounts")

            if 'transaction_amount' in df.columns:
                # Remove non-numeric characters
                df['transaction_amount'] = df['transaction_amount'].astype(str).str.replace(
                    r'[^\d.-]', '', regex=True
                )

                # Convert to numeric
                df['transaction_amount'] = pd.to_numeric(
                    df['transaction_amount'],
                    errors='coerce'
                )

                # Remove negative amounts (unless explicitly allowed)
                initial_len = len(df)
                df['transaction_amount'] = df['transaction_amount'].abs()

                # Remove zero or null amounts
                df = df[df['transaction_amount'].notna()]
                df = df[df['transaction_amount'] > 0]
                logger.info(f"Removed {initial_len - len(df)} rows with invalid amounts")

            return df

        except Exception as e:
            logger.error(f"Amount cleaning failed: {str(e)}", exc_info=True)
            raise

    def clean_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean object/text fields: trim, standardize case, remove nulls.

        Args:
            df: Source dataframe

        Returns:
            pd.DataFrame: Dataframe with cleaned text
        """
        try:
            logger.info("Cleaning text fields")

            text_columns = df.select_dtypes(include='object').columns

            for col in text_columns:
                if col in ['customer_name', 'branch_location', 'product_type']:
                    # Trim whitespace
                    df[col] = df[col].str.strip()
                    # Title case for names
                    df[col] = df[col].str.title()
                    # Remove duplicates after cleaning
                    df = df[df[col].notna()]

            return df

        except Exception as e:
            logger.error(f"Text field cleaning failed: {str(e)}", exc_info=True)
            raise

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate records.

        Args:
            df: Source dataframe

        Returns:
            pd.DataFrame: Dataframe with duplicates removed
        """
        try:
            logger.info("Removing duplicates")

            initial_len = len(df)

            # Primary key: customer_id + transaction_id + transaction_date
            duplicate_subset = ['customer_id', 'transaction_id', 'transaction_date']
            duplicates = df.duplicated(subset=duplicate_subset, keep='first')

            self.rows_rejected += duplicates.sum()
            df = df[~duplicates]

            logger.info(f"Removed {initial_len - len(df)} duplicate records")

            return df

        except Exception as e:
            logger.error(f"Duplicate removal failed: {str(e)}", exc_info=True)
            raise

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values through imputation or removal.

        Args:
            df: Source dataframe

        Returns:
            pd.DataFrame: Dataframe with missing values handled
        """
        try:
            logger.info("Handling missing values")

            # Log missing value counts
            missing_counts = df.isnull().sum()
            if missing_counts.sum() > 0:
                logger.info(f"Missing values per column:\n{missing_counts[missing_counts > 0]}")

            # Drop rows with missing critical fields
            critical_cols = ['customer_id', 'transaction_id', 'transaction_date', 'transaction_amount']
            df = df.dropna(subset=[col for col in critical_cols if col in df.columns])

            # Fill non-critical nulls with defaults
            fill_defaults = {
                'account_status': 'UNKNOWN',
                'product_type': 'UNCLASSIFIED',
                'customer_segment': 'GENERAL',
            }

            for col, default_value in fill_defaults.items():
                if col in df.columns:
                    df[col] = df[col].fillna(default_value)

            return df

        except Exception as e:
            logger.error(f"Missing value handling failed: {str(e)}", exc_info=True)
            raise

    # ========================================================================
    # Data Transformation
    # ========================================================================

    def populate_dimension_tables(self) -> Dict[str, int]:
        """
        Populate dimension tables from staging data.

        Returns:
            Dict[str, int]: Row counts per dimension
        """
        try:
            logger.info("Populating dimension tables")

            with get_db_session() as session:
                # Customers
                customer_query = f"""
                    INSERT INTO {Config.DW_SCHEMA_NAME}.dim_customers
                    (customer_id, customer_name, customer_email, customer_phone, customer_age, customer_segment, is_active)
                    SELECT DISTINCT
                        customer_id,
                        customer_name,
                        customer_email,
                        customer_phone,
                        CAST(customer_age AS INT),
                        customer_segment,
                        TRUE
                    FROM {Config.STAGING_SCHEMA_NAME}.cleaned_banking_data
                    WHERE customer_id IS NOT NULL
                    ON CONFLICT (customer_id) DO UPDATE SET
                        customer_name = EXCLUDED.customer_name,
                        customer_email = EXCLUDED.customer_email,
                        customer_phone = EXCLUDED.customer_phone,
                        customer_age = EXCLUDED.customer_age,
                        customer_segment = EXCLUDED.customer_segment,
                        updated_at = CURRENT_TIMESTAMP
                """

                session.execute(text(customer_query))
                logger.info("Customers dimension populated")

                # Products
                product_query = f"""
                    INSERT INTO {Config.DW_SCHEMA_NAME}.dim_products
                    (product_id, product_type, product_name, product_category, is_active)
                    SELECT DISTINCT
                        product_type,
                        product_type,
                        product_type,
                        'BANKING',
                        TRUE
                    FROM {Config.STAGING_SCHEMA_NAME}.cleaned_banking_data
                    WHERE product_type IS NOT NULL
                    ON CONFLICT (product_type) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        updated_at = CURRENT_TIMESTAMP
                """
                session.execute(text(product_query))
                logger.info("Products dimension populated")

                # Branches
                branch_query = f"""
                    INSERT INTO {Config.DW_SCHEMA_NAME}.dim_branches
                    (branch_id, branch_name, branch_location, is_active)
                    SELECT DISTINCT
                        branch_id,
                        branch_id,
                        branch_location,
                        TRUE
                    FROM {Config.STAGING_SCHEMA_NAME}.cleaned_banking_data
                    WHERE branch_id IS NOT NULL
                    ON CONFLICT (branch_id) DO UPDATE SET
                        branch_location = EXCLUDED.branch_location,
                        updated_at = CURRENT_TIMESTAMP
                """

                session.execute(text(branch_query))
                logger.info("Branches dimension populated")

                # Time
                time_query = f"""
                    INSERT INTO {Config.DW_SCHEMA_NAME}.dim_time
                    (date, year, quarter, month, day, day_of_week, day_name, month_name, week_of_year, is_weekend)
                    SELECT DISTINCT
                        CAST(transaction_date AS DATE) AS date,
                        EXTRACT(YEAR FROM CAST(transaction_date AS DATE))::INT,
                        EXTRACT(QUARTER FROM CAST(transaction_date AS DATE))::INT,
                        EXTRACT(MONTH FROM CAST(transaction_date AS DATE))::INT,
                        EXTRACT(DAY FROM CAST(transaction_date AS DATE))::INT,
                        EXTRACT(ISODOW FROM CAST(transaction_date AS DATE))::INT,
                        TO_CHAR(CAST(transaction_date AS DATE), 'FMDay'),
                        TO_CHAR(CAST(transaction_date AS DATE), 'FMMonth'),
                        EXTRACT(WEEK FROM CAST(transaction_date AS DATE))::INT,
                        CASE WHEN EXTRACT(ISODOW FROM CAST(transaction_date AS DATE)) IN (6, 7) THEN TRUE ELSE FALSE END
                    FROM {Config.STAGING_SCHEMA_NAME}.cleaned_banking_data
                    WHERE transaction_date IS NOT NULL
                    ON CONFLICT (date) DO NOTHING
                """
                session.execute(text(time_query))
                logger.info("Time dimension populated")

            return {'customers': 1, 'branches': 1, 'products': 1, 'time': 1}

        except Exception as e:
            logger.error(f"Dimension population failed: {str(e)}", exc_info=True)
            raise

    def transform_data(self) -> Tuple[int, int]:
        """
        Orchestrate all transformation steps.

        Returns:
            Tuple[int, int]: Rows transformed and rows rejected
        """
        try:
            logger.info("Starting data transformation")

            # Load staging data
            df = self.load_staging_data()

            if df.empty:
                logger.warning("No staging data to transform")
                return 0, 0

            source_ids = [int(x) for x in df['id'].tolist()]
            initial_rows = len(df)

            # Apply transformations
            df = self.clean_dates(df)
            df = self.clean_amounts(df)
            df = self.clean_text_fields(df)
            df = self.handle_missing_values(df)
            df = self.remove_duplicates(df)

            self.rows_transformed = len(df)
            self.rows_rejected = initial_rows - self.rows_transformed
            logger.info(f"Transformation complete: {self.rows_transformed}/{initial_rows} rows retained")

            # Persist cleaned rows for deterministic loading
            self.persist_cleaned_data(df)
            self.mark_source_rows_processed(source_ids)

            # Populate dimensions
            self.populate_dimension_tables()

            return self.rows_transformed, self.rows_rejected

        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            raise

    def persist_cleaned_data(self, df: pd.DataFrame) -> None:
        """Persist cleaned rows to staging.cleaned_banking_data."""
        try:
            logger.info("Persisting cleaned data to staging.cleaned_banking_data")
            with get_db_session() as session:
                insert_query = text(f"""
                    INSERT INTO {Config.STAGING_SCHEMA_NAME}.cleaned_banking_data
                    (
                        source_row_id,
                        customer_id,
                        transaction_id,
                        transaction_date,
                        product_type,
                        transaction_amount,
                        transaction_type,
                        account_type,
                        account_status,
                        customer_name,
                        customer_email,
                        customer_phone,
                        customer_age,
                        customer_segment,
                        branch_id,
                        branch_location,
                        is_loaded
                    )
                    VALUES
                    (
                        :source_row_id,
                        :customer_id,
                        :transaction_id,
                        :transaction_date,
                        :product_type,
                        :transaction_amount,
                        :transaction_type,
                        :account_type,
                        :account_status,
                        :customer_name,
                        :customer_email,
                        :customer_phone,
                        :customer_age,
                        :customer_segment,
                        :branch_id,
                        :branch_location,
                        FALSE
                    )
                    ON CONFLICT (source_row_id) DO NOTHING
                """)

                records = []
                for _, row in df.iterrows():
                    age_value = pd.to_numeric(row.get('customer_age'), errors='coerce')
                    records.append({
                        'source_row_id': int(row['id']),
                        'customer_id': row.get('customer_id'),
                        'transaction_id': row.get('transaction_id'),
                        'transaction_date': row.get('transaction_date'),
                        'product_type': row.get('product_type'),
                        'transaction_amount': float(row.get('transaction_amount')) if pd.notnull(row.get('transaction_amount')) else None,
                        'transaction_type': row.get('transaction_type'),
                        'account_type': row.get('account_type'),
                        'account_status': row.get('account_status'),
                        'customer_name': row.get('customer_name'),
                        'customer_email': row.get('customer_email'),
                        'customer_phone': row.get('customer_phone'),
                        'customer_age': int(age_value) if pd.notnull(age_value) else None,
                        'customer_segment': row.get('customer_segment'),
                        'branch_id': row.get('branch_id'),
                        'branch_location': row.get('branch_location'),
                    })

                if records:
                    session.execute(insert_query, records)
        except Exception as e:
            logger.error(f"Failed persisting cleaned data: {str(e)}", exc_info=True)
            raise

    def mark_source_rows_processed(self, source_ids: list[int]) -> None:
        """Mark raw source rows as processed."""
        if not source_ids:
            return
        try:
            with get_db_session() as session:
                update_query = text(f"""
                    UPDATE {Config.STAGING_SCHEMA_NAME}.raw_banking_data
                    SET is_processed = TRUE, processed_at = CURRENT_TIMESTAMP
                    WHERE id = ANY(:source_ids)
                """)
                session.execute(update_query, {'source_ids': source_ids})
        except Exception as e:
            logger.error(f"Failed marking source rows as processed: {str(e)}", exc_info=True)
            raise


def transform_banking_data() -> Tuple[int, int]:
    """
    Orchestrate banking data transformation.

    Returns:
        Tuple[int, int]: Rows transformed and rows rejected
    """
    transformer = DataTransformer()
    return transformer.transform_data()


if __name__ == '__main__':
    # Test transformation
    try:
        rows_transformed, rows_rejected = transform_banking_data()
        print(f"Transformation successful: {rows_transformed} rows transformed, {rows_rejected} rows rejected")
    except Exception as e:
        print(f"Transformation failed: {e}")
