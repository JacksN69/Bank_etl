# ============================================================================
# Banking ETL Pipeline - Deployment Config & Makefile
# ============================================================================
# Convenient commands for common operations

.PHONY: help build up down logs clean init-db run-tests lint format

PROJECT_NAME := banking-etl
DOCKER_COMPOSE := docker-compose

help:
	@echo "$(PROJECT_NAME) - Banking ETL Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make build      - Build Docker images"
	@echo "  make up         - Start all services"
	@echo "  make down       - Stop all services"
	@echo "  make logs       - View Docker logs"
	@echo "  make ps         - Show container status"
	@echo "  make clean      - Remove containers and volumes"
	@echo "  make init-db    - Initialize database schema"
	@echo "  make trigger    - Trigger ETL DAG"
	@echo "  make run-tests  - Run unit tests"
	@echo "  make lint       - Run code linter (flake8)"
	@echo "  make format     - Format code (black)"

build:
	@echo "Building Docker images..."
	$(DOCKER_COMPOSE) build

up:
	@echo "Starting services..."
	$(DOCKER_COMPOSE) up -d
	@echo "Services started. Check http://localhost:8080 for Airflow"

down:
	@echo "Stopping services..."
	$(DOCKER_COMPOSE) down

logs:
	$(DOCKER_COMPOSE) logs -f

ps:
	$(DOCKER_COMPOSE) ps

clean:
	@echo "Removing containers and volumes..."
	$(DOCKER_COMPOSE) down -v
	@echo "Cleanup complete"

init-db:
	@echo "Initializing database schema..."
	docker exec $(PROJECT_NAME)-postgres psql -U airflow -d banking_warehouse -f /docker-entrypoint-initdb.d/schema_creation.sql

trigger:
	@echo "Triggering ETL pipeline..."
	docker exec $(PROJECT_NAME)-airflow-webserver airflow dags trigger -e 2024-02-18 banking_etl_pipeline

run-tests:
	@echo "Running unit tests..."
	python -m pytest tests/ -v

lint:
	@echo "Running code linter..."
	flake8 etl/ --max-line-length=100

format:
	@echo "Formatting code..."
	black etl/ dags/ tests/

.DEFAULT_GOAL := help
