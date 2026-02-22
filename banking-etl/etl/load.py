from datetime import datetime
from typing import Dict, Tuple

from sqlalchemy import text

from etl.config import Config
from etl.database import get_db_session
from etl.logger import get_logger

logger = get_logger(__name__)


class DataLoader:
    """
    Loads transformed data into the data warehouse.
    Handles dimension and fact table population.
    """

    def __init__(self):
        """Initialize the data loader."""
        self.config = Config
        self.batch_size = Config.BATCH_SIZE
        self.rows_loaded = 0
        self.rows_failed = 0

    def load_facts(self) -> Tuple[int, int]:
        """
        Load transactions into the fact table.

        Returns:
            Tuple[int, int]: Rows loaded and rows failed
        """
        try:
            logger.info("Loading fact_transactions")

            rows_loaded = 0
            rows_failed = 0

            with get_db_session() as session:
                load_query = text(f"""
                    INSERT INTO {Config.DW_SCHEMA_NAME}.fact_transactions
                    (
                        customer_key,
                        product_key,
                        time_key,
                        branch_key,
                        transaction_id,
                        account_id,
                        transaction_amount,
                        transaction_type,
                        account_type,
                        account_status,
                        transaction_date,
                        transaction_timestamp,
                        is_duplicate,
                        data_quality_score,
                        etl_batch_id
                    )
                    SELECT
                        COALESCE(c.customer_key, 1),
                        COALESCE(p.product_key, 1),
                        COALESCE(t.time_key, 1),
                        b.branch_key,
                        s.transaction_id,
                        s.customer_id as account_id,
                        CAST(s.transaction_amount AS NUMERIC(18, 2)),
                        s.transaction_type,
                        s.account_type,
                        s.account_status,
                        CAST(s.transaction_date AS DATE),
                        CURRENT_TIMESTAMP,
                        FALSE,
                        0.95,
                        :batch_id
                    FROM {Config.STAGING_SCHEMA_NAME}.cleaned_banking_data s
                    LEFT JOIN {Config.DW_SCHEMA_NAME}.dim_customers c
                        ON s.customer_id = c.customer_id
                    LEFT JOIN {Config.DW_SCHEMA_NAME}.dim_products p
                        ON s.product_type = p.product_type
                    LEFT JOIN {Config.DW_SCHEMA_NAME}.dim_time t
                        ON CAST(s.transaction_date AS DATE) = t.date
                    LEFT JOIN {Config.DW_SCHEMA_NAME}.dim_branches b
                        ON s.branch_id = b.branch_id
                    WHERE COALESCE(s.is_loaded, FALSE) = FALSE
                    ON CONFLICT (transaction_id) DO NOTHING
                """)

                batch_id = datetime.utcnow().isoformat()
                result = session.execute(load_query, {"batch_id": batch_id})
                rows_loaded = result.rowcount or 0

                mark_loaded_query = text(f"""
                    UPDATE {Config.STAGING_SCHEMA_NAME}.cleaned_banking_data s
                    SET is_loaded = TRUE, loaded_at = CURRENT_TIMESTAMP
                    WHERE COALESCE(s.is_loaded, FALSE) = FALSE
                      AND EXISTS (
                        SELECT 1
                        FROM {Config.DW_SCHEMA_NAME}.fact_transactions f
                        WHERE f.transaction_id = s.transaction_id
                      )
                """)
                session.execute(mark_loaded_query)

                logger.info(f"Fact table load complete: {rows_loaded} rows loaded")

            self.rows_loaded = rows_loaded
            return rows_loaded, rows_failed

        except Exception as e:
            logger.error(f"Fact table loading failed: {str(e)}", exc_info=True)
            raise

    def load_dimensions(self) -> Dict[str, int]:
        """
        Ensure dimension tables are populated.

        Returns:
            Dict[str, int]: Row counts per dimension
        """
        try:
            logger.info("Verifying dimension tables")

            dimension_stats = {
                'customers': 0,
                'products': 0,
                'branches': 0,
                'time': 0,
            }

            with get_db_session() as session:
                for dimension in dimension_stats.keys():
                    count_query = f"SELECT COUNT(*) FROM {Config.DW_SCHEMA_NAME}.dim_{dimension}"
                    result = session.execute(text(count_query))
                    dimension_stats[dimension] = result.scalar() or 0

            logger.info(f"Dimension statistics: {dimension_stats}")
            return dimension_stats

        except Exception as e:
            logger.error(f"Dimension verification failed: {str(e)}", exc_info=True)
            raise

    def load_data(self) -> Tuple[int, int]:
        """
        Orchestrate data loading process.

        Returns:
            Tuple[int, int]: Total rows loaded and rows failed
        """
        try:
            logger.info("Starting data load into warehouse")

            dimension_stats = self.load_dimensions()

            rows_loaded, rows_failed = self.load_facts()

            logger.info(f"Data load complete: {rows_loaded} rows loaded, {rows_failed} rows failed")

            return rows_loaded, rows_failed

        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}", exc_info=True)
            raise


def load_banking_data() -> Tuple[int, int]:
    """
    Orchestrate banking data loading.

    Returns:
        Tuple[int, int]: Rows loaded and rows failed
    """
    loader = DataLoader()
    return loader.load_data()


if __name__ == '__main__':
    try:
        rows_loaded, rows_failed = load_banking_data()
        print(f"Loading successful: {rows_loaded} rows loaded, {rows_failed} rows failed")
    except Exception as e:
        print(f"Loading failed: {e}")
