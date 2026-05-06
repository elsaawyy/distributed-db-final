"""
Worker-Python: A Python (Flask) implementation of a Distributed DB Worker node.

This worker:
- Receives replicated data from the Go Master node via POST /replicate
- Stores data in MySQL
- Serves read requests for fault-tolerant read access
- Provides a SPECIAL TASK: data transformation pipeline (/transform)
"""

import json
import logging
import os
from datetime import datetime

from flask import Flask, jsonify, request

from database.mysql import MySQLDB

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="[WORKER-PY] %(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("worker-python")

# Flask app
app = Flask(__name__)

# MySQL Configuration
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3309")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("MYSQL_PASS", "")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "worker_python_db")

# Initialize database connection
db = MySQLDB(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_DATABASE)

# Helper functions
def success(data=None, message="", status=200):
    return jsonify({"success": True, "message": message, "data": data}), status

def error(msg, status=400):
    return jsonify({"success": False, "error": msg}), status


# ============================================================
# Replication Endpoint (called by Master)
# ============================================================
@app.route("/replicate", methods=["POST"])
def replicate():
    """Receives replication payloads from the Master node."""
    payload = request.get_json(force=True, silent=True)
    if not payload:
        return error("Invalid JSON body")
    
    logger.info("Replicating operation: %s on db: %s", payload.get("operation"), payload.get("database"))
    
    err = db.apply_replication(payload)
    if err:
        logger.error("Replication error: %s", err)
        return error(err, 500)
    
    return success(message=f"Replicated: {payload.get('operation')}")


# ============================================================
# Read Endpoint (fault-tolerant reads)
# ============================================================
@app.route("/select", methods=["GET"])
def select():
    """Read endpoint for fault-tolerant reads when master is unavailable."""
    db_name = request.args.get("database", "")
    table_name = request.args.get("table", "")
    
    if not db_name or not table_name:
        return error("'database' and 'table' query params required")
    
    where_filter = {}
    for key, val in request.args.items():
        if key.startswith("where_"):
            where_filter[key[6:]] = val
    
    try:
        records = db.select(db_name, table_name, where_filter)
        return success(data=records or [])
    except Exception as e:
        return error(str(e), 500)


# ============================================================
# Special Task: Data Transformation Pipeline
# ============================================================
@app.route("/transform", methods=["POST"])
def transform():
    """
    SPECIAL TASK — Data Transformation Pipeline.
    
    This is the Python worker's unique capability: it applies a transformation
    pipeline to table data and returns the transformed result without modifying
    the stored data.
    """
    body = request.get_json(force=True, silent=True)
    if not body:
        return error("Invalid JSON body")
    
    db_name = body.get("database", "")
    table_name = body.get("table", "")
    transformations = body.get("transformations", [])
    
    if not db_name or not table_name:
        return error("'database' and 'table' are required")
    
    try:
        result = db.transform(db_name, table_name, transformations)
        logger.info("Transform on %s.%s: %d rows returned", db_name, table_name, len(result))
        
        return success(
            data={
                "database": db_name,
                "table": table_name,
                "transformations_applied": len(transformations),
                "row_count": len(result),
                "rows": result,
            },
            message="Transformation completed by Python Worker",
        )
    except Exception as e:
        return error(str(e), 500)


# ============================================================
# Health & Status
# ============================================================
@app.route("/health", methods=["GET"])
def health():
    if not db.health():
        return error("Database connection failed", 503)
    
    return success(
        data={
            "node": "worker-python",
            "time": datetime.utcnow().isoformat(),
            "databases": db.list_databases()
        },
        message="Python Worker is healthy",
    )


@app.route("/status", methods=["GET"])
def status():
    return success(
        data={
            "node": "worker-python",
            "databases": db.list_databases()
        }
    )


# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":
    # Connect to MySQL
    if not db.connect():
        logger.error("Failed to connect to MySQL. Exiting.")
        exit(1)
    
    # Initialize schema
    db.init_schema()
    
    port = int(os.environ.get("WORKER_PY_PORT", "8082"))
    logger.info("Python Worker starting on port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False)