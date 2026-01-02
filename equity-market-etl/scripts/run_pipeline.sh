#!/bin/bash

# Equity Market ETL Pipeline - Startup Script
# This script initializes the pipeline environment and starts all services

set -e  # Exit on error

echo "=========================================="
echo "Equity Market ETL Pipeline Setup"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    echo "Please start Docker Desktop and try again"
    exit 1
fi

echo -e "${GREEN}✓${NC} Docker is running"

# Check if data directories exist
if [ ! -d "data/raw/us_stocks_etfs" ]; then
    echo -e "${YELLOW}Warning: data/raw/us_stocks_etfs directory not found${NC}"
    echo "Please download stock price CSV files and place them in this directory"
fi

if [ ! -f "data/raw/company_info_and_logos/companies.csv" ]; then
    echo -e "${YELLOW}Warning: companies.csv not found${NC}"
    echo "Please download company reference data"
fi

# Create necessary directories if they don't exist
echo "Creating directory structure..."
mkdir -p data/{raw/{us_stocks_etfs,company_info_and_logos},bronze/{prices,companies},silver/{prices,companies},gold/{prices_enriched,analytics}}
mkdir -p airflow/{dags,logs,plugins}
echo -e "${GREEN}✓${NC} Directories created"

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Creating .env file with default values..."
    cat > .env << EOF
# Alert Configuration
ALERT_EMAILS=

# Spark Configuration
SPARK_SHUFFLE_PARTITIONS=8
SPARK_APP_NAME=equity-market-etl

# Airflow Configuration
AIRFLOW_UID=50000
EOF
    echo -e "${GREEN}✓${NC} .env file created"
else
    echo -e "${GREEN}✓${NC} .env file exists"
fi

# Stop existing containers if running
echo "Stopping existing containers..."
docker-compose down > /dev/null 2>&1 || true

# Build Docker images
echo "Building Docker images (this may take a few minutes)..."
docker-compose build --no-cache

# Start services
echo "Starting services..."
docker-compose up -d

# Wait for Postgres to be healthy
echo "Waiting for PostgreSQL to be ready..."
timeout=60
counter=0
until docker-compose exec -T postgres pg_isready -U airflow > /dev/null 2>&1; do
    sleep 2
    counter=$((counter + 2))
    if [ $counter -ge $timeout ]; then
        echo -e "${RED}Error: PostgreSQL did not become ready in time${NC}"
        exit 1
    fi
done
echo -e "${GREEN}✓${NC} PostgreSQL is ready"

# Wait for Airflow webserver to be healthy
echo "Waiting for Airflow webserver to be ready..."
timeout=120
counter=0
until curl -s http://localhost:8080/health > /dev/null 2>&1; do
    sleep 5
    counter=$((counter + 5))
    if [ $counter -ge $timeout ]; then
        echo -e "${YELLOW}Warning: Airflow webserver is taking longer than expected${NC}"
        echo "You can check logs with: docker-compose logs airflow-webserver"
        break
    fi
done
echo -e "${GREEN}✓${NC} Airflow webserver is ready"

echo ""
echo "=========================================="
echo -e "${GREEN}Setup Complete!${NC}"
echo "=========================================="
echo ""
echo "Access the services:"
echo "  • Airflow UI:   http://localhost:8080"
echo "    Username: admin"
echo "    Password: admin"
echo ""
echo "  • Spark UI:     http://localhost:8081"
echo "  • PostgreSQL:   localhost:5432"
echo ""
echo "Useful commands:"
echo "  • View logs:         docker-compose logs -f"
echo "  • Stop services:     docker-compose down"
echo "  • Restart services:  docker-compose restart"
echo "  • Shell access:      docker-compose exec airflow-webserver bash"
echo ""
echo "Next steps:"
echo "  1. Ensure data files are in data/raw/"
echo "  2. Open Airflow UI at http://localhost:8080"
echo "  3. Enable the 'equity_market_etl_pipeline' DAG"
echo "  4. Trigger a manual run or wait for scheduled execution"
echo ""
echo "=========================================="