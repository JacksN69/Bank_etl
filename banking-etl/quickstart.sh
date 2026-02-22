#!/bin/bash

set -e

PROJECT_NAME="banking-etl"
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=======================================${NC}"
echo -e "${BLUE}Banking ETL Pipeline - Quick Start${NC}"
echo -e "${BLUE}=======================================${NC}\n"

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_status "Checking for Docker..."
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    echo "  Download from: https://www.docker.com/products/docker-desktop"
    exit 1
fi
print_success "Docker found: $(docker --version)"

print_status "Checking for Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed"
    exit 1
fi
print_success "Docker Compose found: $(docker-compose --version)"

print_status "Checking .env file..."
if [ ! -f "$PROJECT_DIR/.env" ]; then
    print_warning ".env file not found. Creating from template..."
    if [ -f "$PROJECT_DIR/.env.example" ]; then
        cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
        print_success ".env file created from template"
        echo "  Edit .env with your configuration before running the pipeline"
    else
        print_error ".env.example template not found"
        exit 1
    fi
else
    print_success ".env file exists"
fi

print_status "Checking data directory..."
if [ ! -d "$PROJECT_DIR/data" ]; then
    mkdir -p "$PROJECT_DIR/data"
    print_success "Created data/ directory"
fi

echo -e "\n${BLUE}Available Commands:${NC}\n"
echo "  1) Start services (docker-compose up --build)"
echo "  2) Stop services (docker-compose down)"
echo "  3) View logs"
echo "  4) Check container status"
echo "  5) Initialize database schema"
echo "  6) Run ETL pipeline"
echo "  7) Clean up (remove volumes)"
echo "  8) Exit"

if [ $# -eq 1 ]; then
    CHOICE=$1
else
    echo -ne "\n${BLUE}Enter your choice (1-8):${NC} "
    read CHOICE
fi

case $CHOICE in
    1)
        print_status "Starting services..."
        cd "$PROJECT_DIR"
        docker-compose up --build
        ;;
    2)
        print_status "Stopping services..."
        cd "$PROJECT_DIR"
        docker-compose down
        print_success "Services stopped"
        ;;
    3)
        print_status "Docker Compose logs..."
        cd "$PROJECT_DIR"
        docker-compose logs -f
        ;;
    4)
        print_status "Container status..."
        cd "$PROJECT_DIR"
        docker-compose ps
        ;;
    5)
        print_status "Initializing database schema..."
        cd "$PROJECT_DIR"
        sleep 5
        docker exec banking-etl-postgres psql -U airflow -d banking_warehouse \
            -f /docker-entrypoint-initdb.d/schema_creation.sql 2>/dev/null || \
            docker exec banking-etl-postgres psql -U airflow -d banking_warehouse \
            -f /sql/schema_creation.sql 2>/dev/null || \
            print_warning "Could not initialize schema. Please run manually."
        print_success "Schema initialization attempted"
        ;;
    6)
        print_status "Running ETL pipeline..."
        cd "$PROJECT_DIR"
        docker exec banking-etl-airflow-webserver airflow dags trigger -e 2024-02-18 banking_etl_pipeline
        print_success "Pipeline triggered. View at http://localhost:8080"
        ;;
    7)
        print_status "Cleaning up Docker resources..."
        cd "$PROJECT_DIR"
        docker-compose down -v
        print_success "Cleanup complete"
        ;;
    8)
        print_status "Exiting..."
        exit 0
        ;;
    *)
        print_error "Invalid choice"
        exit 1
        ;;
esac

echo -e "\n${BLUE}=======================================${NC}"
echo -e "${BLUE}Quick Start Complete${NC}"
echo -e "${BLUE}=======================================${NC}\n"

echo -e "${GREEN}Useful URLs:${NC}"
echo "  Airflow UI: http://localhost:8080"
echo "  PgAdmin: http://localhost:5050"
echo "  PostgreSQL: localhost:5432"

echo -e "\n${GREEN}Default Credentials:${NC}"
echo "  Airflow: airflow / airflow"
echo "  PgAdmin: admin@banking-etl.local / admin_password_123"
echo "  PostgreSQL: airflow / airflow_secure_password_123"
