# ============================================================================
# Banking ETL Pipeline - Comprehensive Documentation
# ============================================================================

# The Ultimate Production-Grade Banking ETL Pipeline

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Directory Structure](#directory-structure)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring & Logs](#monitoring--logs)
- [Data Quality Framework](#data-quality-framework)
- [Example Queries](#example-queries)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

---

## 🎯 Overview

This is a **production-grade ETL (Extract, Transform, Load) pipeline** designed specifically for banking data. It demonstrates enterprise-level data engineering practices including:

### Key Features

✅ **Modular Architecture** - Separate extract, transform, load modules  
✅ **Scalable** - Containerized with Docker, supports parallel processing  
✅ **Robust** - Comprehensive error handling, retry logic, and logging  
✅ **Data Quality** - Built-in validation framework with metrics tracking  
✅ **Dimensional Model** - Star schema design for analytics  
✅ **Orchestration** - Apache Airflow for scheduling and monitoring  
✅ **PostgreSQL** - Enterprise-grade data warehouse  
✅ **Production Ready** - Security, performance, and maintainability considered  

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Data Sources                             │
│         (Excel, CSV Banking Data Files)                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│           EXTRACTION LAYER (Docker Container)               │
│  • Read source files (Excel/CSV)                            │
│  • Validate file integrity                                  │
│  • Stage raw data                                           │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│          TRANSFORMATION LAYER (Docker Container)            │
│  • Clean and normalize data                                 │
│  • Handle missing values                                    │
│  • Remove duplicates                                        │
│  • Validate business rules                                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│            LOADING LAYER (Docker Container)                 │
│  • Populate dimension tables                                │
│  • Load fact table                                          │
│  • Manage SCD (Slowly Changing Dimensions)                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│         PostgreSQL Data Warehouse                           │
│  ┌────────────────────────────────────────────────────┐    │
│  │  STAGING SCHEMA                                     │    │
│  │  • raw_banking_data                                 │    │
│  └────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────┐    │
│  │  DIMENSIONAL MODEL (Star Schema)                    │    │
│  │  • dim_customers                                    │    │
│  │  • dim_products                                     │    │
│  │  • dim_branches                                     │    │
│  │  • dim_time                                         │    │
│  │  • fact_transactions                                │    │
│  └────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────┐    │
│  │  AUDIT SCHEMA                                       │    │
│  │  • data_quality_metrics                             │    │
│  │  • etl_execution_log                                │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   ┌─────────┐   ┌────────────┐   ┌──────────┐
   │ Airflow │   │  PgAdmin   │   │   BI/BI  │
   │ Dashboard│   │    UI      │   │  Tools   │
   └─────────┘   └────────────┘   └──────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- 8GB RAM minimum
- 20GB disk space for PostgreSQL
- Python 3.8+ (optional, for local development)

### Step 1: Setup Environment

```bash
# Clone or navigate to project
cd banking-etl

# Copy environment template
cp .env.example .env

# Edit .env with your settings
nano .env  # Or use your preferred editor
```

### Step 2: Create Data Volume

```bash
# Copy your source data file
mkdir -p data
cp /path/to/Comprehensive_Banking_Database.csv.xlsx data/
```

### Step 3: Start Services

```bash
# Build and start all containers
docker-compose up --build

# This will start:
# - PostgreSQL (port 5432)
# - PgAdmin (port 5050)
# - Airflow Web UI (port 8080)
# - Airflow Scheduler (background)
```

### Step 4: Verify Installation

```bash
# Check container status
docker-compose ps

# Expected output:
# NAME                  STATUS
# banking-etl-postgres  Up
# banking-etl-pgadmin   Up
# banking-etl-airflow-webserver  Up
# banking-etl-airflow-scheduler   Up
```

### Step 5: Initialize Database Schema

```bash
# After PostgreSQL is ready, initialize the schema
docker exec banking-etl-postgres psql -U airflow -d banking_warehouse -f /docker-entrypoint-initdb.d/schema_creation.sql

# Verify schema creation
docker exec banking-etl-postgres psql -U airflow -d banking_warehouse -c "\\dt banking_dw.*"
```

### Step 6: Trigger ETL Pipeline

```bash
# Navigate to Airflow UI
# URL: http://localhost:8080
# Username: airflow
# Password: airflow

# Manually trigger the banking_etl_pipeline DAG
# Or wait for scheduled run (daily at 2 AM UTC)
```

---

## 📁 Directory Structure

```
banking-etl/
├── dags/
│   └── banking_etl_pipeline_dag.py          # Airflow DAG definition
├── etl/
│   ├── __init__.py                          # Package initialization
│   ├── config.py                            # Configuration management
│   ├── logger.py                            # Logging setup
│   ├── database.py                          # Database connections
│   ├── extract.py                           # Data extraction
│   ├── transform.py                         # Data transformation
│   ├── load.py                              # Data loading
│   ├── quality_checks.py                    # Data quality framework
│   └── utils.py                             # Utility functions
├── sql/
│   ├── schema_creation.sql                  # DDL for data warehouse
│   └── sample_data.sql                      # Sample data inserts
├── config/
│   └── (future: additional configs)
├── logs/
│   ├── etl_pipeline.log                     # Main ETL logs
│   └── etl_errors.log                       # Error logs
├── tests/
│   └── (unit tests)
├── docker-compose.yml                       # Service orchestration
├── .env                                     # Environment variables (KEEP SECRET!)
├── .env.example                             # Environment template
├── requirements.txt                         # Python dependencies
├── entrypoint.sh                            # Docker entrypoint
└── README.md                                # This file
```

---

## ⚙️ Configuration

### Environment Variables (.env file)

All sensitive configuration is managed through environment variables:

**Database Configuration**
```bash
POSTGRES_USER=airflow                    # Database user
POSTGRES_PASSWORD=secure_password        # Database password
POSTGRES_DB=banking_warehouse            # Database name
POSTGRES_HOST=postgres                   # Database host (Docker service name)
POSTGRES_PORT=5432                       # Database port
```

**ETL Configuration**
```bash
DATA_INPUT_PATH=/data/banking_data.xlsx  # Source data path
DW_SCHEMA_NAME=banking_dw                # Data warehouse schema
STAGING_SCHEMA_NAME=staging              # Staging schema
LOG_LEVEL=INFO                           # Logging level
```

**Data Quality Thresholds**
```bash
MIN_COMPLETENESS_PCT=95                  # Minimum data completeness
MAX_NULL_PCT=5                           # Maximum null percentage allowed
DUPLICATE_CHECK_ENABLED=True             # Enable duplicate checking
```

### Configuration in Code

The `etl/config.py` module provides centralized configuration:

```python
from etl.config import Config

# Access any configuration
print(Config.DATABASE_URL)
print(Config.LOG_LEVEL)
print(Config.get_config_dict())
```

---

## 🔄 Running the Pipeline

### Option 1: Airflow Web Interface (Recommended)

1. **Open Airflow UI**: http://localhost:8080
2. **Login**: 
   - Username: `airflow`
   - Password: `airflow`
3. **Navigate to DAGs**: Find `banking_etl_pipeline`
4. **Trigger DAG**:
   - Click the play button (▶️)
   - Select "Trigger DAG"
5. **Monitor Execution**:
   - View task status in real-time
   - Check logs by clicking on individual tasks

### Option 2: Command Line

```bash
# List all DAGs
docker exec banking-etl-airflow-webserver airflow dags list

# Trigger DAG
docker exec banking-etl-airflow-webserver airflow dags trigger -e 2024-02-18 banking_etl_pipeline

# View DAG details
docker exec banking-etl-airflow-webserver airflow dags show banking_etl_pipeline

# Check task logs
docker exec banking-etl-airflow-webserver airflow tasks logs banking_etl_pipeline extract 2024-02-18
```

### Option 3: Python Script

```python
# etl_runner.py - Standalone ETL execution
import sys
sys.path.insert(0, '/path/to/banking-etl')

from etl.extract import extract_and_stage_data
from etl.transform import transform_banking_data
from etl.load import load_banking_data
from etl.quality_checks import run_data_quality_checks
from etl.logger import get_logger

logger = get_logger(__name__)

try:
    logger.info("Starting ETL pipeline")
    
    # Extract
    total, loaded, meta = extract_and_stage_data()
    logger.info(f"Extracted: {total}, Loaded: {loaded}")
    
    # Transform
    transformed, rejected = transform_banking_data()
    logger.info(f"Transformed: {transformed}, Rejected: {rejected}")
    
    # Load
    rows_loaded, rows_failed = load_banking_data()
    logger.info(f"Loaded: {rows_loaded}, Failed: {rows_failed}")
    
    # Quality
    quality_pass, metrics = run_data_quality_checks('MANUAL_RUN', None)
    logger.info(f"Quality: {'PASS' if quality_pass else 'FAIL'}")
    
    logger.info("ETL pipeline completed successfully")
    
except Exception as e:
    logger.error(f"ETL pipeline failed: {e}", exc_info=True)
    sys.exit(1)
```

---

## 📊 Monitoring & Logs

### Log Files

Logs are written to `logs/` directory:

- **`etl_pipeline.log`**: Main ETL log (rotated when > 10MB)
- **`etl_errors.log`**: Error-level logs only

### View Logs in Real-Time

```bash
# From host machine
tail -f banking-etl/logs/etl_pipeline.log

# From Docker container
docker exec banking-etl-postgres tail -f /var/log/postgres.log
docker exec banking-etl-airflow-webserver tail -f /home/airflow/airflow/logs/*/*.log
```

### Airflow UI Monitoring

**Airflow Dashboard** (http://localhost:8080)
- Graph View: Visualize task dependencies
- Tree View: Historical DAG execution
- Log View: Individual task logs
- Admin > Log: Aggregate logs

**PgAdmin Dashboard** (http://localhost:5050)
- Graphical PostgreSQL administration
- Query editor
- Database schema explorer
- Performance metrics

---

## 🛠️ Data Quality Framework

### Quality Checks Performed

The pipeline automatically validates data quality in each step:

#### 1. Completeness Check
- Measures percentage of non-null values
- Threshold: 95% by default
- Tracks by table and batch

#### 2. Null Percentage Check
- Calculates null % per column
- Column-level visibility
- Average across columns

#### 3. Duplicate Detection
- Identifies duplicate transactions
- Key columns: customer_id + transaction_id + transaction_date
- Counts duplicates per batch

#### 4. Schema Validation
- Verifies data types
- Column existence checks
- Type consistency

#### 5. Referential Integrity
- Validates foreign key relationships
- Reports orphaned records
- Dimension-fact relationships

### Quality Metrics Storage

All metrics are persisted in `audit.data_quality_metrics`:

```sql
SELECT
    etl_batch_id,
    table_name,
    metric_name,
    metric_value,
    metric_percentage,
    record_count,
    quality_status,
    checked_at
FROM audit.data_quality_metrics
WHERE checked_at >= NOW() - INTERVAL '7 days'
ORDER BY checked_at DESC;
```

### Viewing Quality Reports

```bash
# Connect to database
docker exec -it banking-etl-postgres psql -U airflow -d banking_warehouse

# Quality report for last run
SELECT
    etl_batch_id,
    metric_name,
    metric_percentage,
    quality_status
FROM audit.data_quality_metrics
WHERE etl_batch_id = (SELECT MAX(etl_batch_id) FROM audit.data_quality_metrics)
ORDER BY table_name, metric_name;
```

---

## 📈 Example Analytics Queries

### 1. Customer Transaction Summary

```sql
SELECT
    c.customer_name,
    c.customer_segment,
    COUNT(DISTINCT t.transaction_id) as transaction_count,
    SUM(t.transaction_amount) as total_amount,
    AVG(t.transaction_amount) as avg_amount,
    MAX(t.transaction_date) as last_transaction
FROM banking_dw.dim_customers c
LEFT JOIN banking_dw.fact_transactions t
    ON c.customer_key = t.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_key, c.customer_name, c.customer_segment
ORDER BY total_amount DESC
LIMIT 20;
```

### 2. Product Performance Analysis

```sql
SELECT
    p.product_type,
    p.product_name,
    COUNT(DISTINCT t.transaction_id) as transaction_count,
    SUM(t.transaction_amount) as total_volume,
    AVG(t.transaction_amount) as avg_transaction,
    COUNT(DISTINCT t.customer_key) as customer_count,
    ROUND(100.0 * COUNT(DISTINCT t.customer_key) / 
        (SELECT COUNT(DISTINCT customer_key) FROM banking_dw.fact_transactions), 2) 
        as customer_penetration_pct
FROM banking_dw.dim_products p
LEFT JOIN banking_dw.fact_transactions t
    ON p.product_key = t.product_key
WHERE p.is_active = TRUE
GROUP BY p.product_key, p.product_type, p.product_name
ORDER BY total_volume DESC;
```

### 3. Time-Based Trends (by Month)

```sql
SELECT
    DATE_TRUNC('month', t.transaction_date)::DATE as month,
    COUNT(DISTINCT t.transaction_id) as transactions,
    SUM(t.transaction_amount) as volume,
    AVG(t.transaction_amount) as avg_transaction,
    COUNT(DISTINCT t.customer_key) as active_customers
FROM banking_dw.fact_transactions t
GROUP BY DATE_TRUNC('month', t.transaction_date)
ORDER BY month DESC;
```

### 4. Data Quality Trend

```sql
SELECT
    DATE_TRUNC('day', checked_at)::DATE as check_date,
    quality_status,
    COUNT(*) as metric_count,
    ROUND(AVG(metric_percentage), 2) as avg_quality_pct
FROM audit.data_quality_metrics
WHERE checked_at >= NOW() - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', checked_at), quality_status
ORDER BY check_date DESC;
```

### 5. ETL Performance Report

```sql
SELECT
    execution_start::DATE as run_date,
    AVG(execution_duration_seconds) as avg_duration_sec,
    MAX(execution_duration_seconds) as max_duration_sec,
    COUNT(*) as total_runs,
    SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    ROUND(100.0 * SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) / 
        COUNT(*), 2) as success_rate_pct,
    SUM(rows_extracted) as total_extracted,
    SUM(rows_loaded) as total_loaded,
    SUM(rows_rejected) as total_rejected
FROM audit.etl_execution_log
WHERE execution_start >= NOW() - INTERVAL '30 days'
GROUP BY execution_start::DATE
ORDER BY run_date DESC;
```

---

## 🔧 Troubleshooting

### Common Issues & Solutions

#### 1. Docker Service Won't Start

```bash
# Check logs
docker-compose logs postgres
docker-compose logs airflow-webserver

# Rebuild containers
docker-compose down -v
docker-compose up --build
```

#### 2. Database Connection Error

```
Error: could not connect to server: Connection refused

# Solution: Ensure postgres container is healthy
docker-compose ps
# If status is 'unhealthy', check postgres logs
docker-compose logs postgres

# Verify connection
docker exec banking-etl-postgres pg_isready -U airflow
```

####  3. Schema Doesn't Exist

```
Error: schema "banking_dw" does not exist

# Solution: Initialize schema
docker exec banking-etl-postgres psql -U airflow -d banking_warehouse -f /docker-entrypoint-initdb.d/schema_creation.sql
```

#### 4. Airflow DAG Not Appearing

```bash
# DAGs take 30+ seconds to appear

# Manually refresh DAG parsing
docker exec banking-etl-airflow-scheduler airflow dags list

# Check for syntax errors in DAG
docker exec banking-etl-airflow-webserver airflow dags validate dags/banking_etl_pipeline_dag.py
```

#### 5. Out of Disk Space

```bash
# Clean up Docker resources
docker system prune -a --volumes

# Rebuild specific service
docker-compose up --build postgres
```

#### 6. Airflow UI Shows "No Data"

```bash
# Airflow metadata DB not initialized
docker-compose down
docker-compose up -d airflow-init
docker-compose up -d
```

---

## 📚 Best Practices

### ETL Development

1. **Modular Design**
   - Keep extract, transform, load isolated
   - Single responsibility principle
   - Easy to test and maintain

2. **Error Handling**
   - Try-except blocks on all operations
   - Log errors with full context
   - Graceful degradation where possible

3. **Logging**
   - Log at appropriate levels (INFO, WARN, ERROR)
   - Include timestamps and context
   - Use structured logging for parsing

4. **Testing**
   - Unit tests for each module
   - Integration tests for pipeline
   - Data quality validation before load

### Production Operations

1. **Monitoring**
   - Check Airflow logs daily
   - Review data quality metrics
   - Monitor database performance

2. **Maintenance**
   - Schedule regular backups
   - Review and optimize indexes
   - Archive old audit logs

3. **Security**
   - Never commit .env files
   - Use strong database passwords
   - Rotate credentials regularly
   - Limit database access

4. **Performance**
   - Use batch processing
   - Index fact table keys
   - Partition large tables
   - Monitor query performance

### Code Quality

1. **Style & Format**
   - Use Black formatter: `black etl/`
   - Run linters: `flake8 etl/ --max-line-length=100`
   - Type hints: Use typing module

2. **Documentation**
   - Docstrings on all functions
   - Comments for complex logic
   - Keep README updated
   - Document configuration

3. **Version Control**
   - Commit frequently with clear messages
   - Use .gitignore for sensitive files
   - Follow git workflow

---

## 🚢 Deployment Checklist

Before deploying to production:

- [ ] Update .env with production credentials
- [ ] Increase resource limits in docker-compose.yml
- [ ] Enable database backups
- [ ] Configure alert email addresses
- [ ] Test disaster recovery procedures
- [ ] Load test with representative data volume
- [ ] Security audit of credentials handling
- [ ] Database performance tuning
- [ ] Set up monitoring (PagerDuty, DataDog, etc.)
- [ ] Create runbooks for common failures


---

## 📝 Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2024-02-18 | Initial production release |

---

**Last Updated**: February 18, 2024  
Oubay
