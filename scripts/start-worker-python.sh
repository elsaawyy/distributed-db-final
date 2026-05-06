#!/bin/bash

# Start Python Worker Node
# Computer C - Run this script

echo "=========================================="
echo "Starting Distributed DB - Python Worker Node"
echo "=========================================="

# Set environment variables
export MYSQL_HOST=localhost
export MYSQL_PORT=3309
export MYSQL_USER=root
export MYSQL_PASS=
export MYSQL_DATABASE=worker_python_db
export WORKER_PY_PORT=8082

# Navigate to worker-python folder
cd ../worker-python

# Activate virtual environment (if exists)
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Install dependencies if needed
pip install -q flask pymysql cryptography

# Run worker
python app.py