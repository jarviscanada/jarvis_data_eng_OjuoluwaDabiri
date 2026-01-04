#!/bin/bash
# Quick test runner script

echo "Running pytest tests..."
docker-compose exec -T airflow-webserver bash -c "cd /opt/airflow && pytest tests/ -v"