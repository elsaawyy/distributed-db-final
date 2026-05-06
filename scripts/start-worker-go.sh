#!/bin/bash

# Start Go Worker Node
# Computer B - Run this script

echo "=========================================="
echo "Starting Distributed DB - Go Worker Node"
echo "=========================================="

# Set environment variables
export MYSQL_HOST=localhost
export MYSQL_PORT=3309
export MYSQL_USER=root
export MYSQL_PASS=
export MYSQL_DATABASE=worker_go_db
export WORKER_GO_PORT=8081

# Navigate to worker-go folder
cd ../worker-go

# Run worker
go run .