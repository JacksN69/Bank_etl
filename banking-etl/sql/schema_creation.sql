-- ============================================================================
-- Banking ETL Warehouse Schema (PostgreSQL)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS banking_dw;
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================================
-- STAGING LAYER
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.raw_banking_data (
    id BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    transaction_id VARCHAR(100),
    transaction_date VARCHAR(50),
    product_type VARCHAR(100),
    transaction_amount VARCHAR(50),
    transaction_type VARCHAR(50),
    account_type VARCHAR(50),
    account_status VARCHAR(50),
    customer_name VARCHAR(200),
    customer_email VARCHAR(200),
    customer_phone VARCHAR(50),
    customer_age VARCHAR(20),
    customer_segment VARCHAR(50),
    branch_id VARCHAR(50),
    branch_location VARCHAR(200),
    source_file_name VARCHAR(255),
    source_file_hash VARCHAR(64),
    raw_data JSONB,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_raw_is_processed
    ON staging.raw_banking_data(is_processed);
CREATE INDEX IF NOT EXISTS idx_staging_raw_transaction_id
    ON staging.raw_banking_data(transaction_id);
CREATE INDEX IF NOT EXISTS idx_staging_raw_created_at
    ON staging.raw_banking_data(created_at);

CREATE TABLE IF NOT EXISTS staging.cleaned_banking_data (
    cleaned_id BIGSERIAL PRIMARY KEY,
    source_row_id BIGINT NOT NULL UNIQUE,
    customer_id VARCHAR(50),
    transaction_id VARCHAR(100),
    transaction_date TIMESTAMP,
    product_type VARCHAR(100),
    transaction_amount NUMERIC(18, 2),
    transaction_type VARCHAR(50),
    account_type VARCHAR(50),
    account_status VARCHAR(50),
    customer_name VARCHAR(200),
    customer_email VARCHAR(200),
    customer_phone VARCHAR(50),
    customer_age INT,
    customer_segment VARCHAR(50),
    branch_id VARCHAR(50),
    branch_location VARCHAR(200),
    is_loaded BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_cleaned_is_loaded
    ON staging.cleaned_banking_data(is_loaded);
CREATE INDEX IF NOT EXISTS idx_staging_cleaned_transaction_id
    ON staging.cleaned_banking_data(transaction_id);

-- ============================================================================
-- DIMENSIONS
-- ============================================================================

CREATE TABLE IF NOT EXISTS banking_dw.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    customer_name VARCHAR(200),
    customer_email VARCHAR(200),
    customer_phone VARCHAR(50),
    customer_age INT,
    customer_segment VARCHAR(50),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_customers_segment
    ON banking_dw.dim_customers(customer_segment);

CREATE TABLE IF NOT EXISTS banking_dw.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL UNIQUE,
    product_type VARCHAR(100) NOT NULL UNIQUE,
    product_name VARCHAR(200) NOT NULL,
    product_category VARCHAR(100),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS banking_dw.dim_branches (
    branch_key SERIAL PRIMARY KEY,
    branch_id VARCHAR(50) NOT NULL UNIQUE,
    branch_name VARCHAR(200),
    branch_location VARCHAR(200),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS banking_dw.dim_time (
    time_key SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_time_year_month
    ON banking_dw.dim_time(year, month);

-- Seed unknown rows for resilient fact FK loading.
INSERT INTO banking_dw.dim_customers (
    customer_key, customer_id, customer_name, customer_segment, is_active
)
VALUES (1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', TRUE)
ON CONFLICT (customer_id) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('banking_dw.dim_customers', 'customer_key'),
    GREATEST((SELECT COALESCE(MAX(customer_key), 1) FROM banking_dw.dim_customers), 1),
    TRUE
);

INSERT INTO banking_dw.dim_products (
    product_key, product_id, product_type, product_name, product_category, is_active
)
VALUES (1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', TRUE)
ON CONFLICT (product_type) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('banking_dw.dim_products', 'product_key'),
    GREATEST((SELECT COALESCE(MAX(product_key), 1) FROM banking_dw.dim_products), 1),
    TRUE
);

INSERT INTO banking_dw.dim_time (
    time_key, date, year, quarter, month, day, day_of_week, day_name, month_name, week_of_year, is_weekend
)
VALUES (1, DATE '1900-01-01', 1900, 1, 1, 1, 1, 'Monday', 'January', 1, FALSE)
ON CONFLICT (date) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('banking_dw.dim_time', 'time_key'),
    GREATEST((SELECT COALESCE(MAX(time_key), 1) FROM banking_dw.dim_time), 1),
    TRUE
);

-- ============================================================================
-- FACT
-- ============================================================================

CREATE TABLE IF NOT EXISTS banking_dw.fact_transactions (
    transaction_key BIGSERIAL PRIMARY KEY,
    customer_key INT NOT NULL REFERENCES banking_dw.dim_customers(customer_key),
    product_key INT NOT NULL REFERENCES banking_dw.dim_products(product_key),
    time_key INT NOT NULL REFERENCES banking_dw.dim_time(time_key),
    branch_key INT REFERENCES banking_dw.dim_branches(branch_key),
    transaction_id VARCHAR(100) NOT NULL UNIQUE,
    account_id VARCHAR(50),
    transaction_amount NUMERIC(18, 2) NOT NULL,
    transaction_type VARCHAR(50),
    account_type VARCHAR(50),
    account_status VARCHAR(50),
    transaction_date DATE NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    data_quality_score NUMERIC(5, 2),
    source_system VARCHAR(50),
    etl_batch_id VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_transactions_customer_key
    ON banking_dw.fact_transactions(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_product_key
    ON banking_dw.fact_transactions(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_time_key
    ON banking_dw.fact_transactions(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_date
    ON banking_dw.fact_transactions(transaction_date);

-- ============================================================================
-- AUDIT
-- ============================================================================

CREATE TABLE IF NOT EXISTS audit.data_quality_metrics (
    quality_key BIGSERIAL PRIMARY KEY,
    etl_batch_id VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(18, 4),
    metric_percentage NUMERIC(7, 2),
    record_count INT,
    quality_status VARCHAR(50),
    metric_description TEXT,
    checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_metrics_batch
    ON audit.data_quality_metrics(etl_batch_id);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_checked_at
    ON audit.data_quality_metrics(checked_at);

CREATE TABLE IF NOT EXISTS audit.etl_execution_log (
    execution_id BIGSERIAL PRIMARY KEY,
    etl_batch_id VARCHAR(100) NOT NULL UNIQUE,
    pipeline_name VARCHAR(100) NOT NULL,
    task_name VARCHAR(100),
    execution_start TIMESTAMP NOT NULL,
    execution_end TIMESTAMP,
    execution_status VARCHAR(50),
    rows_extracted INT NOT NULL DEFAULT 0,
    rows_transformed INT NOT NULL DEFAULT 0,
    rows_loaded INT NOT NULL DEFAULT 0,
    rows_rejected INT NOT NULL DEFAULT 0,
    error_message TEXT,
    execution_duration_seconds INT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_etl_execution_log_start
    ON audit.etl_execution_log(execution_start);

