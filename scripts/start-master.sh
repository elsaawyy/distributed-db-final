#!/bin/bash

# Start Master Node
# Computer A - Run this script

echo "=========================================="
echo "Starting Distributed DB - Master Node"
echo "=========================================="

# Set environment variables
export MYSQL_HOST=localhost
export MYSQL_PORT=3309
export MYSQL_USER=root
export MYSQL_PASS=
export MYSQL_DATABASE=master_db
export WORKER_GO_ADDR=http://localhost:8081
export WORKER_PY_ADDR=http://localhost:8082
export MASTER_PORT=8080

# Navigate to master folder
cd ../master

# Run master
go run .