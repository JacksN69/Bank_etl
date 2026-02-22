"""
Banking ETL Pipeline - Utilities

This module provides utility functions for the ETL pipeline.
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)


def generate_batch_id() -> str:
    """
    Generate a unique batch ID for ETL runs.

    Returns:
        str: Batch ID
    """
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")[:-3]


def compute_hash(data: str) -> str:
    """
    Compute MD5 hash of data.

    Args:
        data: Data to hash

    Returns:
        str: Hash value
    """
    return hashlib.md5(data.encode()).hexdigest()


def safe_json_dumps(obj: Any) -> str:
    """
    Safely serialize object to JSON string.

    Args:
        obj: Object to serialize

    Returns:
        str: JSON string
    """
    try:
        return json.dumps(obj, default=str)
    except Exception as e:
        logger.warning(f"JSON serialization failed: {e}")
        return str(obj)


def calculate_metrics(total: int, processed: int, failed: int) -> Dict[str, float]:
    """
    Calculate ETL processing metrics.

    Args:
        total: Total records
        processed: Successfully processed
        failed: Failed records

    Returns:
        Dict[str, float]: Metrics dictionary
    """
    success_rate = (processed / total * 100) if total > 0 else 0
    failure_rate = (failed / total * 100) if total > 0 else 0

    return {
        'total': total,
        'processed': processed,
        'failed': failed,
        'success_rate': round(success_rate, 2),
        'failure_rate': round(failure_rate, 2),
    }


def format_duration(seconds: int) -> str:
    """
    Format duration in seconds to human-readable format.

    Args:
        seconds: Duration in seconds

    Returns:
        str: Formatted duration string
    """
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"
