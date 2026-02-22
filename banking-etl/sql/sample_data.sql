-- ============================================================================
-- Banking ETL Pipeline - Sample Data Insert Scripts
-- ============================================================================
-- This script inserts sample data into dimension and fact tables for testing.
-- ============================================================================

-- ============================================================================
-- populate Time Dimension (full year 2024)
-- ============================================================================
INSERT INTO banking_dw.dim_time (date, year, quarter, month, day, day_of_week, day_name, month_name, week_of_year, is_weekend, is_holiday)
SELECT
    DATE '2024-01-01' + INTERVAL '1 day' * (i - 1) as date,
    EXTRACT(YEAR FROM DATE '2024-01-01' + INTERVAL '1 day' * (i - 1))::INT as year,
    (EXTRACT(MONTH FROM DATE '2024-01-01' + INTERVAL '1 day' * (i - 1))::INT - 1) / 3 + 1 as quarter,
    EXTRACT(MONTH FROM DATE '2024-01-01' + INTERVAL '1 day' * (i - 1))::INT as month,
    EXTRACT(DAY FROM DATE '2024-01-01' + INTERVAL '1 day' * (i - 1))::INT as day,
    EXTRACT(DOW FROM DATE '2024-01-01' + INTERVAL '1 day' * (i - 1))::INT as day_of_week,
    TO_CHAR(DATE '2024-01-01' + INTERVAL '1 day' * (i - 1), 'FMDay') as day_name,
    TO_CHAR(DATE '2024-01-01' + INTERVAL '1 day' * (i - 1), 'FMMonth') as month_name,
    EXTRACT(WEEK FROM DATE '2024-01-01' + INTERVAL '1 day' * (i - 1))::INT as week_of_year,
    EXTRACT(DOW FROM DATE '2024-01-01' + INTERVAL '1 day' * (i - 1))::INT IN (0, 6) as is_weekend,
    FALSE
FROM GENERATE_SERIES(1, 366) as i
ON CONFLICT (date) DO NOTHING;

-- ============================================================================
-- Insert Sample Products
-- ============================================================================
INSERT INTO banking_dw.dim_products (product_id, product_type, product_name, product_category) VALUES
('PROD001', 'Savings Account', 'Basic Savings Account', 'Deposit'),
('PROD002', 'Checking Account', 'Premium Checking Account', 'Deposit'),
('PROD003', 'Credit Card', 'Gold Credit Card', 'Credit'),
('PROD004', 'Personal Loan', 'Personal Loan Product', 'Loan'),
('PROD005', 'Home Loan', 'Mortgage Product', 'Loan'),
('PROD006', 'Investment Account', 'Brokerage Account', 'Investment'),
('PROD007', 'Money Market', 'Money Market Fund', 'Investment'),
('PROD008', 'Auto Loan', 'Vehicle Finance', 'Loan')
ON CONFLICT (product_id) DO NOTHING;

-- ============================================================================
-- Insert Sample Branches
-- ============================================================================
INSERT INTO banking_dw.dim_branches (branch_id, branch_name, branch_location, branch_city, branch_state, branch_country) VALUES
('BRN001', 'Main Branch', 'Downtown', 'New York', 'NY', 'USA'),
('BRN002', 'Midtown Branch', 'Midtown', 'New York', 'NY', 'USA'),
('BRN003', 'Brooklyn Branch', 'Brooklyn', 'Brooklyn', 'NY', 'USA'),
('BRN004', 'Queens Branch', 'Flushing', 'Queens', 'NY', 'USA'),
('BRN005', 'Manhattan West', 'West Side', 'New York', 'NY', 'USA'),
('BRN006', 'Bronx Branch', 'Grand Concourse', 'Bronx', 'NY', 'USA')
ON CONFLICT (branch_id) DO NOTHING;
