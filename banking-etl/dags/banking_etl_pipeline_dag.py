"""
Banking ETL Pipeline - Airflow DAG

This DAG orchestrates the complete ETL pipeline:
1. Extract data from source
2. Transform and clean data
3. Load into data warehouse
4. Run smoke checks
5. Run data quality checks
6. Log execution details

Schedule: Daily at 2 AM UTC
Retry Policy: 3 retries with 5-minute delays

Author: Data Engineering Team
Date: 2024
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
import sys
from pathlib import Path

# Add etl module to path
etl_path = Path(__file__).parent.parent
sys.path.insert(0, str(etl_path))

from etl.config import Config
from etl.logger import get_logger
from etl.database import DatabaseConnection
from etl.extract import extract_and_stage_data
from etl.transform import transform_banking_data
from etl.load import load_banking_data
from etl.quality_checks import run_data_quality_checks
from sqlalchemy import text

logger = get_logger(__name__)

# ============================================================================
# Airflow DAG Configuration
# ============================================================================


def task_failure_alert(context):
    """Emit a task-failure alert and optionally send email notification."""
    ti = context.get('task_instance')
    dag_id = context.get('dag').dag_id if context.get('dag') else 'unknown_dag'
    task_id = ti.task_id if ti else 'unknown_task'
    run_id = context.get('run_id', 'unknown_run')
    exception = context.get('exception')

    alert_message = (
        f"Airflow task failure detected | dag_id={dag_id} "
        f"task_id={task_id} run_id={run_id} error={exception}"
    )
    logger.error(alert_message)

    if not Config.ALERT_EMAIL:
        return

    try:
        from airflow.utils.email import send_email

        send_email(
            to=Config.ALERT_EMAIL,
            subject=f"[ALERT] Airflow task failed: {dag_id}.{task_id}",
            html_content=f"<pre>{alert_message}</pre>",
        )
    except Exception as email_error:
        logger.error("Failed to send task-failure email alert: %s", str(email_error))


default_args = {
    'owner': Config.AIRFLOW_OWNER,
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': Config.AIRFLOW_RETRY_ATTEMPTS,
    'retry_delay': timedelta(seconds=Config.AIRFLOW_RETRY_DELAY_SECONDS),
    'execution_timeout': timedelta(hours=2),
    'on_failure_callback': task_failure_alert,
}

dag = DAG(
    'banking_etl_pipeline',
    default_args=default_args,
    description='Banking ETL Pipeline - Extract, Transform, Load with Quality Checks',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    catchup=False,
    tags=['banking', 'etl', 'production'],
    max_active_runs=1,  # Prevent concurrent runs
)

# ============================================================================
# Task Functions
# ============================================================================

def validate_environment(**context):
    """Validate ETL environment before running pipeline."""
    logger.info("Validating ETL environment")

    try:
        # Test database connection
        db = DatabaseConnection()
        if not db.test_connection():
            raise Exception("Database connection failed")

        # Ensure ETL uses the warehouse database, not Airflow metadata DB.
        with DatabaseConnection.session_scope() as session:
            current_db = session.execute(text("SELECT current_database()")).scalar()
            if current_db != Config.POSTGRES_DB:
                raise RuntimeError(
                    f"ETL is connected to '{current_db}', expected '{Config.POSTGRES_DB}'. "
                    "Check POSTGRES_* and DATABASE_URL env configuration."
                )

        # Idempotent bootstrap of schemas/tables required by ETL.
        DatabaseConnection.initialize_warehouse_schema()
        if not DatabaseConnection.schema_health_check():
            raise RuntimeError("Schema health check failed after initialization")

        logger.info("Environment validation successful")
        context['task_instance'].xcom_push(key='environment_valid', value=True)

    except Exception as e:
        logger.error(f"Environment validation failed: {str(e)}")
        raise

def extract_task(**context):
    """Extract data from source and load to staging."""
    logger.info("Starting extraction task")

    try:
        total_rows, loaded_rows, metadata = extract_and_stage_data()

        context['task_instance'].xcom_push(key='extraction_rows', value=total_rows)
        context['task_instance'].xcom_push(key='staging_loaded_rows', value=loaded_rows)
        context['task_instance'].xcom_push(key='extraction_metadata', value=metadata)

        logger.info(f"Extraction complete: {total_rows} rows extracted, {loaded_rows} rows loaded")

    except Exception as e:
        logger.error(f"Extraction task failed: {str(e)}")
        raise

def transform_task(**context):
    """Transform data into dimensional model."""
    logger.info("Starting transformation task")

    try:
        rows_transformed, rows_rejected = transform_banking_data()

        context['task_instance'].xcom_push(key='transformation_rows', value=rows_transformed)
        context['task_instance'].xcom_push(key='rejected_rows', value=rows_rejected)

        logger.info(f"Transformation complete: {rows_transformed} rows transformed, {rows_rejected} rows rejected")

    except Exception as e:
        logger.error(f"Transformation task failed: {str(e)}")
        raise

def load_task(**context):
    """Load transformed data into warehouse."""
    logger.info("Starting load task")

    try:
        rows_loaded, rows_failed = load_banking_data()

        context['task_instance'].xcom_push(key='load_rows', value=rows_loaded)
        context['task_instance'].xcom_push(key='failed_rows', value=rows_failed)

        logger.info(f"Load complete: {rows_loaded} rows loaded, {rows_failed} rows failed")

    except Exception as e:
        logger.error(f"Load task failed: {str(e)}")
        raise

def quality_check_task(**context):
    """Run data quality checks on loaded data."""
    logger.info("Starting quality check task")

    try:
        batch_id = context['dag_run'].run_id
        quality_pass, metrics = run_data_quality_checks(batch_id, df=None)

        context['task_instance'].xcom_push(key='quality_pass', value=quality_pass)
        context['task_instance'].xcom_push(key='quality_metrics', value=metrics)

        logger.info(f"Quality checks complete: Status={'PASS' if quality_pass else 'FAIL'}")

        if not quality_pass:
            logger.warning("Quality checks did not pass - review metrics")

    except Exception as e:
        logger.error(f"Quality check task failed: {str(e)}")
        raise

def smoke_check_task(**context):
    """Run post-load smoke checks and store count metrics."""
    logger.info("Starting post-load smoke checks")

    try:
        batch_id = context['dag_run'].run_id
        metric_rows = []

        with DatabaseConnection.session_scope() as session:
            checks = [
                ("staging.raw_banking_data", "SMOKE_COUNT_RAW"),
                ("staging.cleaned_banking_data", "SMOKE_COUNT_CLEANED"),
                ("banking_dw.fact_transactions", "SMOKE_COUNT_FACT"),
            ]

            for table_name, metric_name in checks:
                count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                row_count = session.execute(count_query).scalar() or 0

                metric_rows.append({
                    'batch_id': batch_id,
                    'table_name': table_name,
                    'metric_name': metric_name,
                    'metric_value': float(row_count),
                    'metric_percentage': None,
                    'record_count': int(row_count),
                    'quality_status': 'PASS' if row_count >= 0 else 'FAIL',
                    'metric_description': f"Smoke count for {table_name}",
                })

            insert_query = text("""
                INSERT INTO audit.data_quality_metrics
                (etl_batch_id, table_name, metric_name, metric_value, metric_percentage,
                 record_count, quality_status, metric_description)
                VALUES
                (:batch_id, :table_name, :metric_name, :metric_value, :metric_percentage,
                 :record_count, :quality_status, :metric_description)
            """)
            session.execute(insert_query, metric_rows)

        context['task_instance'].xcom_push(
            key='smoke_counts',
            value={row['table_name']: row['record_count'] for row in metric_rows}
        )
        logger.info("Smoke checks complete: %s", {row['table_name']: row['record_count'] for row in metric_rows})

    except Exception as e:
        logger.error(f"Smoke check task failed: {str(e)}")
        raise

def logging_task(**context):
    """Log execution details to audit table."""
    logger.info("Starting execution logging task")

    try:
        # Get metrics from previous tasks
        extraction_rows = context['task_instance'].xcom_pull(
            task_ids='extract', key='extraction_rows'
        ) or 0
        transformation_rows = context['task_instance'].xcom_pull(
            task_ids='transform', key='transformation_rows'
        ) or 0
        rejected_rows = context['task_instance'].xcom_pull(
            task_ids='transform', key='rejected_rows'
        ) or 0
        load_rows = context['task_instance'].xcom_pull(
            task_ids='load', key='load_rows'
        ) or 0
        quality_pass = context['task_instance'].xcom_pull(
            task_ids='quality_check', key='quality_pass'
        ) or False

        batch_id = context['dag_run'].run_id
        execution_start = context['task_instance'].start_date
        if execution_start is None:
            execution_start = context.get('logical_date') or timezone.utcnow()
        execution_start = timezone.coerce_datetime(execution_start)
        if execution_start.tzinfo is None:
            execution_start = timezone.make_aware(execution_start, timezone=timezone.utc)

        execution_end = timezone.utcnow()
        execution_end = timezone.coerce_datetime(execution_end)
        if execution_end.tzinfo is None:
            execution_end = timezone.make_aware(execution_end, timezone=timezone.utc)
        duration_seconds = int((execution_end - execution_start).total_seconds())

        # Determine overall status
        overall_status = 'SUCCESS' if quality_pass else 'WARNING'

        # Log to audit table
        with DatabaseConnection.session_scope() as session:
            insert_query = text(f"""
                INSERT INTO audit.etl_execution_log
                (etl_batch_id, pipeline_name, task_name, execution_start, execution_end,
                 execution_status, rows_extracted, rows_transformed, rows_loaded, rows_rejected,
                 execution_duration_seconds)
                VALUES
                (:batch_id, :pipeline, :task, :start, :end, :status, :extracted,
                 :transformed, :loaded, :rejected, :duration)
            """)

            session.execute(insert_query, {
                'batch_id': batch_id,
                'pipeline': 'banking_etl_pipeline',
                'task': 'full_pipeline',
                'start': execution_start,
                'end': execution_end,
                'status': overall_status,
                'extracted': extraction_rows,
                'transformed': transformation_rows,
                'loaded': load_rows,
                'rejected': rejected_rows,
                'duration': duration_seconds,
            })

        logger.info(f"Execution logged - Status: {overall_status}, Duration: {duration_seconds}s")

    except Exception as e:
        logger.error(f"Logging task failed: {str(e)}")
        raise

# ============================================================================
# DAG Tasks
# ============================================================================

# Task 1: Validate environment
t_validate = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_environment,
    dag=dag,
    doc="""
    Validates the ETL environment including:
    - Database connectivity
    - Required schemas existence
    - Configuration validity
    """,
)

# Task 2: Extract data
t_extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
    doc="Extract banking data from source file and load to staging",
)

# Task 3: Transform data
t_transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
    doc="Transform and clean data, populate dimensions",
)

# Task 4: Load data
t_load = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
    doc="Load transformed data into fact and dimension tables",
)

# Task 5: Smoke check
t_smoke = PythonOperator(
    task_id='smoke_check',
    python_callable=smoke_check_task,
    dag=dag,
    doc="Run post-load smoke checks and persist table row-count metrics",
)

# Task 6: Quality checks
t_quality = PythonOperator(
    task_id='quality_check',
    python_callable=quality_check_task,
    dag=dag,
    doc="Run comprehensive data quality validation",
)

# Task 7: Log execution
t_logging = PythonOperator(
    task_id='logging',
    python_callable=logging_task,
    dag=dag,
    doc="Log execution details and metrics to audit table",
)

# ============================================================================
# DAG Task Dependencies
# ============================================================================

t_validate >> t_extract >> t_transform >> t_load >> t_smoke >> t_quality >> t_logging
