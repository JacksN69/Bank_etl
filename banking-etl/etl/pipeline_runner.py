"""
Standalone ETL runner for the dedicated ETL container service.
"""

from datetime import datetime

from etl.extract import extract_and_stage_data
from etl.load import load_banking_data
from etl.logger import get_logger
from etl.quality_checks import run_data_quality_checks
from etl.transform import transform_banking_data
from etl.utils import generate_batch_id

logger = get_logger(__name__)


def run_pipeline() -> None:
    """Run full ETL pipeline outside Airflow."""
    batch_id = generate_batch_id()
    logger.info("ETL runner started | batch_id=%s", batch_id)

    extracted, staged, _ = extract_and_stage_data()
    logger.info("Extract complete | extracted=%s staged=%s", extracted, staged)

    transformed, rejected = transform_banking_data()
    logger.info("Transform complete | transformed=%s rejected=%s", transformed, rejected)

    loaded, failed = load_banking_data()
    logger.info("Load complete | loaded=%s failed=%s", loaded, failed)

    quality_pass, metrics = run_data_quality_checks(batch_id, df=None)
    logger.info(
        "Quality complete | pass=%s metrics_count=%s finished_at=%s",
        quality_pass,
        len(metrics),
        datetime.utcnow().isoformat(),
    )


if __name__ == "__main__":
    run_pipeline()
