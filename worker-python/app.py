"""
Worker-Python: A Python (Flask) implementation of a Distributed DB Worker node.

This worker:
- Receives replicated data from the Go Master node via POST /replicate
- Stores data in MySQL
- Serves read requests for fault-tolerant read access
- Provides a SPECIAL TASK: data transformation pipeline (/transform)
- Supports leader election for automatic failover
"""

import json
import logging
import os
import threading
import time
from datetime import datetime

import requests
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

# ============================================================
# Leader Election (Raft-Lite)
# ============================================================

class RaftNode:
    def __init__(self, my_addr: str, peers: list):
        self.state = "follower"  # follower, candidate, leader
        self.current_term = 0
        self.voted_for = None
        self.leader_addr = None
        self.my_addr = my_addr
        self.peers = peers
        self.lock = threading.Lock()
    
    def start(self):
        thread = threading.Thread(target=self._election_loop, daemon=True)
        thread.start()
    
    def _election_loop(self):
        while True:
            time.sleep(0.1)
            
            with self.lock:
                is_leader = self.state == "leader"
            
            if is_leader:
                continue
            
            if self._should_start_election():
                self._start_election()
    
    def _should_start_election(self) -> bool:
        if self.leader_addr:
            try:
                resp = requests.get(f"{self.leader_addr}/health", timeout=2)
                if resp.status_code == 200:
                    return False
            except:
                pass
        return True
    
    def _start_election(self):
        with self.lock:
            self.state = "candidate"
            self.current_term += 1
            self.voted_for = self.my_addr
            term = self.current_term
        
        logger.info(f"[ELECTION] Node {self.my_addr} starting election for term {term}")
        
        votes = 1
        for peer in self.peers:
            try:
                resp = requests.post(
                    f"{peer}/vote",
                    json={"term": term, "candidateId": self.my_addr},
                    timeout=1
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("data", {}).get("voteGranted"):
                        votes += 1
            except Exception as e:
                logger.debug(f"Failed to get vote from {peer}: {e}")
        
        if votes > len(self.peers) // 2:
            with self.lock:
                self.state = "leader"
                self.leader_addr = self.my_addr
            logger.info(f"[ELECTION] Node {self.my_addr} became LEADER for term {term} with {votes} votes")
            self._send_heartbeats()
        else:
            with self.lock:
                self.state = "follower"
            logger.info(f"[ELECTION] Node {self.my_addr} lost election for term {term}")
    
    def _send_heartbeats(self):
        def heartbeat_loop():
            while True:
                time.sleep(2)
                with self.lock:
                    if self.state != "leader":
                        break
                    term = self.current_term
                    my_addr = self.my_addr
                    peers = self.peers.copy()
                
                heartbeat = {"term": term, "leader": my_addr}
                for peer in peers:
                    try:
                        requests.post(f"{peer}/heartbeat", json=heartbeat, timeout=1)
                    except Exception:
                        pass
        
        thread = threading.Thread(target=heartbeat_loop, daemon=True)
        thread.start()
    
    def handle_vote(self, req: dict) -> dict:
        with self.lock:
            term = req.get("term", 0)
            candidate_id = req.get("candidateId", "")
            
            response = {"term": self.current_term, "voteGranted": False}
            
            if term > self.current_term:
                self.current_term = term
                self.state = "follower"
                self.voted_for = None
            
            if term == self.current_term and self.voted_for is None:
                response["voteGranted"] = True
                self.voted_for = candidate_id
                logger.info(f"[ELECTION] Node {self.my_addr} voted for {candidate_id}")
            
            return response
    
    def handle_heartbeat(self, heartbeat: dict):
        with self.lock:
            term = heartbeat.get("term", 0)
            leader = heartbeat.get("leader", "")
            
            if term >= self.current_term:
                self.current_term = term
                self.state = "follower"
                self.leader_addr = leader
                logger.debug(f"[HEARTBEAT] Node {self.my_addr} acknowledged leader {leader}")
    
    def is_leader(self) -> bool:
        with self.lock:
            return self.state == "leader"
    
    def get_leader_addr(self) -> str:
        with self.lock:
            return self.leader_addr


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
    
    # --- API Key Authentication ---
    expected_key = os.environ.get("API_KEY", "default-secret-change-me")
    provided_key = request.headers.get("X-API-Key")
    if not provided_key or provided_key != expected_key:
        logger.warning(f"Unauthorized replication attempt from {request.remote_addr}")
        return error("Unauthorized: invalid API key", 403)
    # -----------------------------

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
# Election Endpoints
# ============================================================
@app.route("/vote", methods=["POST"])
def handle_vote():
    """Handle vote requests from other nodes during leader election."""
    data = request.get_json(force=True, silent=True)
    if not data:
        return error("Invalid JSON body")
    
    resp = raft_node.handle_vote(data)
    return success(data=resp)


@app.route("/heartbeat", methods=["POST"])
def handle_heartbeat():
    """Handle heartbeat from leader node."""
    data = request.get_json(force=True, silent=True)
    if not data:
        return error("Invalid JSON body")
    
    raft_node.handle_heartbeat(data)
    return success(message="Heartbeat received")


@app.route("/leader", methods=["GET"])
def get_leader():
    """Get current leader address."""
    leader = raft_node.get_leader_addr()
    return success(data={"leader": leader, "isLeader": raft_node.is_leader()})


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
            "isLeader": raft_node.is_leader(),
            "leader": raft_node.get_leader_addr(),
            "databases": db.list_databases()
        },
        message="Python Worker is healthy",
    )


@app.route("/status", methods=["GET"])
def status():
    return success(
        data={
            "node": "worker-python",
            "isLeader": raft_node.is_leader(),
            "leader": raft_node.get_leader_addr(),
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
    
    # Initialize Raft node for leader election
    my_addr = os.environ.get("WORKER_PY_ADDR", "http://localhost:8082")
    peers = [
        os.environ.get("MASTER_ADDR", "http://localhost:8080"),
        os.environ.get("WORKER_GO_ADDR", "http://localhost:8081"),
    ]
    raft_node = RaftNode(my_addr, peers)
    raft_node.start()
    
    port = int(os.environ.get("WORKER_PY_PORT", "8082"))
    logger.info("Python Worker starting on port %d", port)
    logger.info("My address: %s", my_addr)
    logger.info("Peers: %s", peers)
    app.run(host="0.0.0.0", port=port, debug=False)