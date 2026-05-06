# Distributed Database System - Final Report

## 1. Architecture Overview

### 1.1 System Components

The distributed database system consists of three independent nodes:

| Node | Technology | Port | Role |
|------|------------|------|------|
| Master | Go | 8080 | Schema management, query coordination, replication orchestrator |
| Worker-Go | Go | 8081 | Replicated storage, read-fallback, analytics (Bonus Task 1) |
| Worker-Python | Python/Flask | 8082 | Replicated storage, read-fallback, data transformation (Bonus Task 2) |

### 1.2 Communication Protocol

All nodes communicate over **HTTP** using REST APIs. The master exposes endpoints for database operations, while workers expose:

- `/replicate` - Endpoint to receive replication data from master
- `/select` - Endpoint for fault-tolerant reads
- `/health` - Liveness check endpoint

### 1.3 Storage Layer

Each node uses **MySQL** (via XAMPP, port 3309) as its persistence layer. The schema includes:

- `databases` table: Tracks created databases
- `tables_metadata` table: Stores table schemas as JSON
- Dynamic tables: `{database}__{table}` for actual data storage

**Why MySQL?**
- ACID compliance and crash recovery
- Eliminates custom JSON serialization
- Supports concurrent access without complex locking
- Enables complex queries via SQL

---

## 2. Core Features Implementation

### 2.1 Database and Table Management

The master node supports:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/create-db` | POST | Create a new database |
| `/drop-db` | DELETE | Drop an existing database (master only) |
| `/create-table` | POST | Create a table with dynamic schema |

Tables can be created at runtime with arbitrary columns. Each column defines:

- Name (string identifier)
- Type (string, int, float, bool)
- Required flag (true/false)

### 2.2 CRUD Operations

All nodes support the following data operations:

| Operation | Method | Endpoint | Description |
|-----------|--------|----------|-------------|
| Insert | POST | `/insert` | Add a new record |
| Select | GET | `/select` | Query records with optional WHERE filters |
| Update | PUT | `/update` | Modify existing record by ID |
| Delete | DELETE | `/delete` | Remove record by ID |
| Search | GET | `/search` | Substring search on a column |

### 2.3 Synchronous Replication

The master implements **synchronous replication** with fan-out to both workers:
Client → POST /insert → Master
├── 1. Validate request
├── 2. Write to Master's MySQL
├── 3. Replicate to Worker-Go (parallel goroutine)
├── 4. Replicate to Worker-Python (parallel goroutine)
└── 5. Return success to Client


**Implementation Details:**

- Uses `sync.WaitGroup` for parallel replication
- 5-second HTTP timeout per worker
- Failed workers are marked `alive: false` and skipped in future replications
- Health check loop runs every 10 seconds to detect recovery
- Replication is best-effort (no rollback if a worker fails)

### 2.4 Fault Tolerance

The system provides **read fault tolerance**:

- If the master node fails, clients can query workers directly
- Workers maintain complete copies of all data via replication
- Master automatically reconnects to recovered workers
- Worker status is visible in the GUI (green/red indicators)

---

## 3. Special Tasks (Bonus Features)

### 3.1 Go Worker - Analytics Endpoint

**Endpoint:** `GET /analytics?database=X&table=Y`

The Go worker computes column-level statistics without transmitting all data:

- Total row count in the table
- Per-column: non-null count, unique value count, sample values (up to 5)

**Example Response:**

```json
{
  "success": true,
  "data": {
    "database": "company",
    "table": "employees",
    "total_rows": 10,
    "column_stats": {
      "name": {
        "non_null_count": 10,
        "unique_values": 8,
        "sample_values": ["Alice", "Bob", "Carol"]
      },
      "salary": {
        "non_null_count": 10,
        "unique_values": 9,
        "sample_values": ["95000", "72000", "105000"]
      }
    }
  }
}

Use Case: Quick data profiling without pulling all records to client.

3.2 Python Worker - Transformation Pipeline
Endpoint: POST /transform

The Python worker applies an ordered pipeline of transformations. This demonstrates the benefit of heterogeneous workers: Python's flexibility for ETL-style data processing.

Supported Transformations:

Transform	Description	Example
filter	Keep rows matching condition	{"operator": "gt", "value": "25"}
project	Keep only specified columns	["name", "salary"]
rename	Rename a column	{"from": "name", "to": "full_name"}
cast	Convert column type	{"to_type": "float"}
sort	Sort rows by column	{"order": "desc"}
limit	Return first N rows	{"count": 10}
Operators for filter: eq, ne, gt, lt, gte, lte, contains

Example Request:

json
{
  "database": "company",
  "table": "employees",
  "transformations": [
    {"type": "filter", "column": "department", "operator": "eq", "value": "Engineering"},
    {"type": "project", "columns": ["name", "salary"]},
    {"type": "sort", "column": "salary", "order": "desc"},
    {"type": "limit", "count": 5}
  ]
}
Use Case: ETL pipelines, reporting, data preparation without modifying source data.

4. Web GUI
The system includes a web-based GUI embedded in the master binary using Go's //go:embed directive.

Access: http://localhost:8080/ui/

Features:

Feature	Description
Worker Status	Real-time green/red indicators for both workers
Database Management	Create and delete databases
Table Management	Create tables with JSON schema input
Record Viewer	View, insert, update, delete records
Special Tasks	Direct buttons for analytics and transform
Query Executor	Run custom JSON queries
Auto-Refresh	Status updates every 10 seconds
GUI is accessible from any device on the same network by using the master's IP address (e.g., http://192.168.1.100:8080/ui/).

5. Design Choices
5.1 Why MySQL Instead of Custom Storage?
Aspect	Custom Storage	MySQL
ACID Compliance	Manual implementation	Built-in
Concurrency	Complex locking	Built-in
Crash Recovery	Manual JSON parsing	Transaction logs
Query Capability	In-memory filtering	Full SQL
Development Time	Longer	Faster
We chose MySQL because it provides production-grade reliability while allowing focus on distributed systems concepts.

5.2 Why HTTP Instead of Raw TCP?
Simplicity of implementation and debugging

Built-in support for JSON serialization

Easy integration with web GUI

Standard tooling (curl, browsers, Postman)

No custom protocol design needed

5.3 Why Separate Workers?
Benefit	Description
Read Scalability	Workers handle direct read requests
Fault Isolation	Worker failure doesn't affect master writes
Heterogeneous Capabilities	Different languages for different tasks
Educational Value	Demonstrates master-worker pattern
5.4 Why Different Languages for Workers?
Worker	Language	Special Capability
Worker-Go	Go	Concurrent analytics with goroutines
Worker-Python	Python	Flexible data transformation with dynamic typing
This demonstrates interoperability and matches the bonus requirement.

6. Challenges Faced
6.1 MySQL Configuration
Challenge: XAMPP MySQL uses port 3309 instead of default 3306, and required allowNativePasswords=true flag.

Solution: Configured environment variables (MYSQL_PORT=3309) and updated DSN strings with proper authentication parameters.

6.2 Cross-Node Communication
Challenge: Workers initially only listened on localhost, preventing remote connections from other computers.

Solution: Changed server binding from ":port" to "0.0.0.0:port" to accept connections from any network interface.

6.3 Synchronous Replication Performance
Challenge: Sequential replication to workers would double response latency.

Solution: Implemented parallel goroutines with sync.WaitGroup to replicate concurrently.

6.4 SQL Reserved Keywords
Challenge: databases is a reserved keyword in MariaDB/MySQL.

Solution: Escaped table names with backticks (`databases`) in all SQL queries.

6.5 Type Handling in MySQL Driver
Challenge: Go's sql.RawBytes type caused compilation errors.

Solution: Used interface{} with type assertion and []byte conversion for flexible value handling.

7. Testing
7.1 Test Cases Executed
Test Case	Expected Result	Status
All 3 nodes start and respond to /health	HTTP 200 with success	✅
Create database	Success message, DB appears in status	✅
Create table with schema	Table created, schema stored	✅
INSERT record	Record appears on master	✅
Verify replication to Go worker	Same data on worker	✅
Verify replication to Python worker	Same data on worker	✅
SELECT with WHERE filter	Only matching records	✅
UPDATE record	Changes appear on all nodes	✅
SEARCH substring match	Records containing search term	✅
DELETE record	Record removed from all nodes	✅
Go Worker /analytics	Statistics returned	✅
Python Worker /transform	Transformed data returned	✅
Fault-tolerant read from workers	Data accessible when master down	✅
Master /status	Shows databases and worker states	✅
GUI loads and displays correctly	Interactive web interface	✅
7.2 Running the Demo
bash
# Terminal 1 - Python Worker
cd worker-python
python app.py

# Terminal 2 - Go Worker
cd worker-go
go run .

# Terminal 3 - Master
cd master
go run .

# Terminal 4 - Run demo
cd scripts
./demo.sh
Expected Output: All 14 test steps pass with green (success) status.

8. Deployment on 3 Computers
The system is designed to run on 3 separate physical computers:

Computer	Role	IP Configuration	Command
Computer A	Master	WORKER_GO_ADDR=http://[ComputerB_IP]:8081	go run .
Computer B	Worker-Go	Listens on 0.0.0.0:8081	go run .
Computer C	Worker-Python	Listens on 0.0.0.0:8082	python app.py
Network Requirements:

All computers on same network (same Wi-Fi or Ethernet)

Firewall rules allow ports 8080, 8081, 8082

MySQL accessible on each computer via localhost

GUI Access: Any computer on network can access http://[ComputerA_IP]:8080/ui/

9. Future Improvements
Improvement	Description	Priority
Write-Ahead Log (WAL)	Allow recovery of workers that missed replication	High
Leader Election	Automatic master failover using consensus algorithm (Raft)	High
Authentication	Add API keys or JWT for secure access	Medium
More Worker Types	Node.js, Rust, or Java workers for additional special tasks	Medium
Connection Pooling	Improve database connection management	Low
Query Optimization	Add indexes and query planner for better performance	Low
Docker Support	Containerized deployment for easier setup	Low
10. Conclusion
We successfully implemented a distributed database system with:

3 independent nodes (Master + Go Worker + Python Worker)

MySQL persistence on all nodes for ACID compliance

Synchronous replication with parallel fan-out using goroutines

Fault-tolerant reads directly from workers

Two bonus special tasks:

Go Worker: Column analytics with statistics

Python Worker: 6-stage transformation pipeline

Web-based GUI with real-time status monitoring

The system meets all core requirements specified in the project brief. It demonstrates key distributed systems concepts including:

Network communication between independent nodes

Data replication for consistency

Fault tolerance through worker fallback

Heterogeneous worker architectures

Real-time monitoring and control

Total Code: ~2,500 lines across Go, Python, HTML/CSS/JS, and SQL

Appendix A: API Reference
Master Endpoints
Method	Endpoint	Description
POST	/create-db	Create a new database
DELETE	/drop-db	Drop a database (master only)
POST	/create-table	Create a table with schema
POST	/insert	Insert a record
GET	/select	Select records with WHERE filters
PUT	/update	Update a record by ID
DELETE	/delete	Delete a record by ID
GET	/search	Substring search on column
GET	/health	Liveness check
GET	/status	System status with worker states
Worker-Go Endpoints
Method	Endpoint	Description
POST	/replicate	Receive replication from master
GET	/select	Direct read (fault-tolerant)
GET	/analytics	Column statistics (Bonus)
GET	/health	Liveness check
Worker-Python Endpoints
Method	Endpoint	Description
POST	/replicate	Receive replication from master
GET	/select	Direct read (fault-tolerant)
POST	/transform	Data transformation pipeline (Bonus)
GET	/health	Liveness check
Appendix B: Environment Variables
Master (Computer A)
Variable	Default	Description
MYSQL_HOST	localhost	MySQL host
MYSQL_PORT	3309	MySQL port
MYSQL_USER	root	MySQL user
MYSQL_PASS	(empty)	MySQL password
MYSQL_DATABASE	master_db	MySQL database name
WORKER_GO_ADDR	http://localhost:8081	Go worker address
WORKER_PY_ADDR	http://localhost:8082	Python worker address
MASTER_PORT	8080	HTTP port
Worker-Go (Computer B)
Variable	Default	Description
MYSQL_HOST	localhost	MySQL host
MYSQL_PORT	3309	MySQL port
MYSQL_USER	root	MySQL user
MYSQL_PASS	(empty)	MySQL password
MYSQL_DATABASE	worker_go_db	MySQL database name
WORKER_GO_PORT	8081	HTTP port
Worker-Python (Computer C)
Variable	Default	Description
MYSQL_HOST	localhost	MySQL host
MYSQL_PORT	3309	MySQL port
MYSQL_USER	root	MySQL user
MYSQL_PASS	(empty)	MySQL password
MYSQL_DATABASE	worker_python_db	MySQL database name
WORKER_PY_PORT	8082	HTTP port

Report Completed: May 6, 2026

Submitted by: [Mohamed Mosbah - Mazen Moharam - Hady Mohamed - Nada Ahmed - Naddin essam - sama ]

Course: Distributed Databases

Project Version: 1.0