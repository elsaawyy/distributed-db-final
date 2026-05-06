#!/bin/bash

# Full Demo Script
# Run this after all 3 nodes are running

MASTER="http://localhost:8080"
WORKER_GO="http://localhost:8081"
WORKER_PY="http://localhost:8082"

echo "=========================================="
echo "Distributed Database System - Demo"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Step 1: Health Check
echo -e "${BLUE}1. Health Check${NC}"
curl -s $MASTER/health | python -m json.tool
echo ""

# Step 2: Create Database
echo -e "${BLUE}2. Create Database 'company'${NC}"
curl -s -X POST $MASTER/create-db -H "Content-Type: application/json" -d '{"name":"company"}'
echo ""

# Step 3: Create Table
echo -e "${BLUE}3. Create Table 'employees'${NC}"
curl -s -X POST $MASTER/create-table \
  -H "Content-Type: application/json" \
  -d '{
    "database": "company",
    "table": "employees",
    "columns": [
      {"name": "name", "type": "string", "required": true},
      {"name": "department", "type": "string", "required": true},
      {"name": "salary", "type": "float", "required": true}
    ]
  }'
echo ""

# Step 4: Insert Records
echo -e "${BLUE}4. Insert Records${NC}"
curl -s -X POST $MASTER/insert -H "Content-Type: application/json" -d '{"database":"company","table":"employees","fields":{"name":"Alice","department":"Engineering","salary":95000}}'
echo ""
curl -s -X POST $MASTER/insert -H "Content-Type: application/json" -d '{"database":"company","table":"employees","fields":{"name":"Bob","department":"Marketing","salary":72000}}'
echo ""
curl -s -X POST $MASTER/insert -H "Content-Type: application/json" -d '{"database":"company","table":"employees","fields":{"name":"Carol","department":"Engineering","salary":105000}}'
echo ""

# Step 5: Select All
echo -e "${BLUE}5. Select All Employees${NC}"
curl -s "$MASTER/select?database=company&table=employees" | python -m json.tool
echo ""

# Step 6: Select with Filter
echo -e "${BLUE}6. Select Engineering Department${NC}"
curl -s "$MASTER/select?database=company&table=employees&where_department=Engineering" | python -m json.tool
echo ""

# Step 7: Update Record
echo -e "${BLUE}7. Update Alice's Salary${NC}"
curl -s -X PUT $MASTER/update -H "Content-Type: application/json" -d '{"database":"company","table":"employees","id":"1","fields":{"salary":110000}}'
echo ""

# Step 8: Search
echo -e "${BLUE}8. Search for 'Ali' in name${NC}"
curl -s "$MASTER/search?database=company&table=employees&column=name&value=Ali" | python -m json.tool
echo ""

# Step 9: Go Worker Analytics (Special Task)
echo -e "${BLUE}9. Go Worker - Analytics${NC}"
curl -s "$WORKER_GO/analytics?database=company&table=employees" | python -m json.tool
echo ""

# Step 10: Python Worker Transform (Special Task)
echo -e "${BLUE}10. Python Worker - Transform${NC}"
curl -s -X POST $WORKER_PY/transform \
  -H "Content-Type: application/json" \
  -d '{
    "database": "company",
    "table": "employees",
    "transformations": [
      {"type": "filter", "column": "department", "operator": "eq", "value": "Engineering"},
      {"type": "project", "columns": ["name", "salary"]}
    ]
  }' | python -m json.tool
echo ""

# Step 11: Fault-Tolerant Read from Go Worker
echo -e "${BLUE}11. Fault-Tolerant Read from Go Worker${NC}"
curl -s "$WORKER_GO/select?database=company&table=employees" | python -m json.tool
echo ""

# Step 12: Fault-Tolerant Read from Python Worker
echo -e "${BLUE}12. Fault-Tolerant Read from Python Worker${NC}"
curl -s "$WORKER_PY/select?database=company&table=employees" | python -m json.tool
echo ""

# Step 13: Delete Record
echo -e "${BLUE}13. Delete Bob's Record${NC}"
curl -s -X DELETE $MASTER/delete -H "Content-Type: application/json" -d '{"database":"company","table":"employees","id":"2"}'
echo ""

# Step 14: Status
echo -e "${BLUE}14. Master Status${NC}"
curl -s $MASTER/status | python -m json.tool
echo ""

echo -e "${GREEN}=========================================="
echo "Demo Complete!"
echo "==========================================${NC}"