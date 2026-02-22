"""
Banking ETL Pipeline - Database Connection Module

This module handles all database connections and operations.
Uses SQLAlchemy for database abstraction and connection pooling.

Author: Data Engineering Team
Date: 2024
"""

from pathlib import Path

from sqlalchemy import create_engine, text, pool
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from typing import Generator, Optional
import logging
from etl.config import Config
from etl.logger import get_logger

logger = get_logger(__name__)


class DatabaseConnection:
    """
    Manages database connections with connection pooling and error handling.
    Implements singleton pattern to ensure single connection pool.
    """

    _instance = None
    _engine = None
    _session_factory = None

    def __new__(cls):
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    @staticmethod
    def _initialize():
        """Initialize database connection pool."""
        try:
            # Create engine with connection pooling
            DatabaseConnection._engine = create_engine(
                Config.DATABASE_URL,
                poolclass=pool.QueuePool,
                pool_size=10,               # Number of connections to maintain
                max_overflow=20,            # Additional connections beyond pool_size
                pool_pre_ping=True,         # Test connections before using them
                pool_recycle=3600,          # Recycle connections every hour
                echo=False,                 # Set to True for SQL debugging
                connect_args={'application_name': 'banking-etl-pipeline'}
            )

            # Create session factory
            DatabaseConnection._session_factory = sessionmaker(
                bind=DatabaseConnection._engine,
                expire_on_commit=False
            )

            logger.info(f"Database connection pool initialized: {Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}/{Config.POSTGRES_DB}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize database connection: {str(e)}")
            raise

    @classmethod
    def initialize_warehouse_schema(cls, schema_sql_path: Optional[Path] = None) -> None:
        """
        Initialize/repair warehouse schemas and tables from the SQL DDL file.
        Safe to run repeatedly because DDL is IF NOT EXISTS / idempotent.

        Args:
            schema_sql_path: Optional custom path to schema SQL file
        """
        candidate_paths = []
        if schema_sql_path:
            candidate_paths.append(Path(schema_sql_path))
        candidate_paths.extend([
            Path(__file__).resolve().parent.parent / 'sql' / 'schema_creation.sql',
            Path('/home/airflow/airflow/sql/schema_creation.sql'),
            Path.cwd() / 'sql' / 'schema_creation.sql',
        ])

        sql_path = next((path for path in candidate_paths if path.exists()), None)
        if not sql_path:
            searched = ', '.join(str(p) for p in candidate_paths)
            raise FileNotFoundError(f"Schema SQL not found. Searched: {searched}")

        sql_script = sql_path.read_text(encoding='utf-8')
        with cls.get_engine().begin() as conn:
            # Use driver-level SQL execution to allow multi-statement DDL script.
            conn.connection.cursor().execute(sql_script)
        logger.info(f"Warehouse schema initialization completed using: {sql_path}")

    @classmethod
    def schema_health_check(cls) -> bool:
        """
        Validate required schemas/tables for the ETL pipeline.

        Returns:
            bool: True when all required objects exist
        """
        required_objects = [
            ('staging', 'raw_banking_data'),
            ('staging', 'cleaned_banking_data'),
            ('banking_dw', 'fact_transactions'),
            ('audit', 'data_quality_metrics'),
            ('audit', 'etl_execution_log'),
        ]
        try:
            with cls.session_scope() as session:
                for schema_name, table_name in required_objects:
                    exists_query = text("""
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = :schema_name
                              AND table_name = :table_name
                        )
                    """)
                    exists = session.execute(
                        exists_query,
                        {'schema_name': schema_name, 'table_name': table_name}
                    ).scalar()
                    if not exists:
                        logger.error(f"Missing required table: {schema_name}.{table_name}")
                        return False
            return True
        except SQLAlchemyError as e:
            logger.error(f"Schema health check failed: {str(e)}")
            return False

    @classmethod
    def get_engine(cls):
        """Get the SQLAlchemy engine."""
        return cls()._engine

    @classmethod
    def get_session(cls) -> Session:
        """Get a new database session."""
        return cls()._session_factory()

    @classmethod
    @contextmanager
    def session_scope(cls) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.
        Handles automatic commit/rollback.

        Yields:
            Session: SQLAlchemy session
        """
        session = cls.get_session()
        try:
            yield session
            session.commit()
            logger.debug("Database session committed successfully")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error in session: {str(e)}")
            raise
        except Exception as e:
            session.rollback()
            logger.error(f"Unexpected error in database session: {str(e)}")
            raise
        finally:
            session.close()

    @classmethod
    def execute_query(cls, query: str) -> object:
        """
        Execute a raw SQL query safely.

        Args:
            query: SQL query string

        Returns:
            Query result
        """
        with cls.session_scope() as session:
            try:
                result = session.execute(text(query))
                logger.debug(f"Query executed successfully")
                return result
            except SQLAlchemyError as e:
                logger.error(f"Query execution failed: {str(e)}")
                raise

    @classmethod
    def test_connection(cls) -> bool:
        """
        Test database connectivity.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            with cls.session_scope() as session:
                session.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection test failed: {str(e)}")
            return False

    @classmethod
    def get_table_row_count(cls, schema: str, table: str) -> int:
        """
        Get the row count of a table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            int: Number of rows
        """
        try:
            with cls.session_scope() as session:
                result = session.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table}")
                )
                count = result.scalar()
                return count or 0
        except SQLAlchemyError as e:
            logger.error(f"Failed to get row count for {schema}.{table}: {str(e)}")
            return -1

    @classmethod
    def close_connection(cls):
        """Close all database connections."""
        if cls._engine:
            cls._engine.dispose()
            logger.info("Database connection pool closed")


# Convenience function for session context management
@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Convenience function to get a database session.

    Yields:
        Session: SQLAlchemy session
    """
    with DatabaseConnection.session_scope() as session:
        yield session


if __name__ == '__main__':
    # Test database connection
    db = DatabaseConnection()
    if db.test_connection():
        print("Connection successful!")
    else:
        print("Connection failed!")
