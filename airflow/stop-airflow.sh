#!/bin/bash
# Script dừng Airflow
# Usage: ./stop-airflow.sh

echo "🛑 Stopping Airflow..."

sudo docker-compose -f docker-compose.airflow.yml down

echo "✅ Airflow stopped successfully!"
echo ""
echo "To also remove volumes (database), run:"
echo "sudo docker-compose -f docker-compose.airflow.yml down -v"
