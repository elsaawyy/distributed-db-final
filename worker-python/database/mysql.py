import pymysql
import json
import logging
from datetime import datetime

logger = logging.getLogger("worker-python")

class MySQLDB:
    def __init__(self, host, port, user, password, database):
        self.connection = None
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        
    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            logger.info(f"Connected to MySQL at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            return False
    
    def close(self):
        if self.connection:
            self.connection.close()
    
    def init_schema(self):
        cursor = self.connection.cursor()
        
        # Create databases metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `databases` (
                name VARCHAR(255) PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        # Create tables metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tables_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                database_name VARCHAR(255),
                table_name VARCHAR(255),
                schema_json JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_table (database_name, table_name),
                FOREIGN KEY (database_name) REFERENCES `databases`(name) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        self.connection.commit()
        logger.info("Database schema initialized")
    
    def apply_replication(self, payload):
        operation = payload.get("operation")
        db_name = payload.get("database", "")
        
        if operation == "create_db":
            return self._create_database(db_name)
        elif operation == "drop_db":
            return self._drop_database(db_name)
        elif operation == "create_table":
            schema = payload.get("schema")
            if not schema:
                return "missing schema"
            return self._create_table(db_name, payload.get("table", ""), schema)
        elif operation == "insert":
            record = payload.get("record")
            if not record:
                return "missing record"
            return self._insert_record(db_name, payload.get("table", ""), record)
        elif operation == "update":
            return self._update_record(db_name, payload.get("table", ""), payload.get("record_id", ""), payload.get("fields", {}))
        elif operation == "delete":
            return self._delete_record(db_name, payload.get("table", ""), payload.get("record_id", ""))
        else:
            return f"unknown operation: {operation}"
    
    def _create_database(self, name):
        cursor = self.connection.cursor()
        try:
            cursor.execute("INSERT INTO `databases` (name) VALUES (%s)", (name,))
            self.connection.commit()
            logger.info(f"Database '{name}' created via replication")
        except pymysql.err.IntegrityError:
            logger.info(f"Database '{name}' already exists")
        return None
    
    def _drop_database(self, name):
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM `databases` WHERE name = %s", (name,))
        self.connection.commit()
        logger.info(f"Database '{name}' dropped via replication")
        return None
    
    def _create_table(self, db_name, table_name, schema):
        cursor = self.connection.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM `databases` WHERE name = %s", (db_name,))
        if not cursor.fetchone():
            return f"database '{db_name}' does not exist"
        
        # Store schema in metadata
        schema_json = json.dumps(schema.get("columns", []))
        cursor.execute("""
            INSERT INTO tables_metadata (database_name, table_name, schema_json)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE schema_json = VALUES(schema_json)
        """, (db_name, table_name, schema_json))
        
        # Create actual table
        safe_table_name = f"{db_name}__{table_name}".replace("-", "_").replace(" ", "_")
        
        sql = f"CREATE TABLE IF NOT EXISTS `{safe_table_name}` ("
        sql += "`id` VARCHAR(255) PRIMARY KEY, "
        sql += "`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        sql += "`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        
        for col in schema.get("columns", []):
            col_name = col.get("name")
            col_type = col.get("type")
            required = col.get("required", False)
            
            sql_type = {
                "string": "TEXT",
                "int": "BIGINT",
                "float": "DOUBLE",
                "bool": "BOOLEAN"
            }.get(col_type, "TEXT")
            
            sql += f", `{col_name}` {sql_type}"
            if required:
                sql += " NOT NULL"
        
        sql += ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        
        try:
            cursor.execute(sql)
            self.connection.commit()
            logger.info(f"Table '{table_name}' created in database '{db_name}' via replication")
        except Exception as e:
            return str(e)
        
        return None
    
    def _insert_record(self, db_name, table_name, record):
        cursor = self.connection.cursor()
        safe_table_name = f"{db_name}__{table_name}".replace("-", "_").replace(" ", "_")
        
        columns = ["id"]
        values = [record.get("id")]
        placeholders = ["%s"]
        
        for k, v in record.get("fields", {}).items():
            columns.append(k)
            values.append(v)
            placeholders.append("%s")
        
        sql = f"INSERT INTO `{safe_table_name}` ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        try:
            cursor.execute(sql, values)
            self.connection.commit()
            logger.info(f"Record inserted into '{db_name}.{table_name}' via replication")
        except Exception as e:
            return str(e)
        
        return None
    
    def _update_record(self, db_name, table_name, record_id, fields):
        cursor = self.connection.cursor()
        safe_table_name = f"{db_name}__{table_name}".replace("-", "_").replace(" ", "_")
        
        set_clauses = []
        values = []
        for k, v in fields.items():
            set_clauses.append(f"`{k}` = %s")
            values.append(v)
        values.append(record_id)
        
        sql = f"UPDATE `{safe_table_name}` SET {', '.join(set_clauses)} WHERE id = %s"
        
        try:
            cursor.execute(sql, values)
            self.connection.commit()
            logger.info(f"Record '{record_id}' updated in '{db_name}.{table_name}' via replication")
        except Exception as e:
            return str(e)
        
        return None
    
    def _delete_record(self, db_name, table_name, record_id):
        cursor = self.connection.cursor()
        safe_table_name = f"{db_name}__{table_name}".replace("-", "_").replace(" ", "_")
        
        sql = f"DELETE FROM `{safe_table_name}` WHERE id = %s"
        
        try:
            cursor.execute(sql, (record_id,))
            self.connection.commit()
            logger.info(f"Record '{record_id}' deleted from '{db_name}.{table_name}' via replication")
        except Exception as e:
            return str(e)
        
        return None
    
    def select(self, db_name, table_name, where_filter):
        cursor = self.connection.cursor()
        safe_table_name = f"{db_name}__{table_name}".replace("-", "_").replace(" ", "_")
        
        sql = f"SELECT * FROM `{safe_table_name}`"
        values = []
        
        if where_filter:
            conditions = []
            for k, v in where_filter.items():
                conditions.append(f"`{k}` = %s")
                values.append(v)
            sql += " WHERE " + " AND ".join(conditions)
        
        cursor.execute(sql, values)
        rows = cursor.fetchall()
        
        records = []
        for row in rows:
            record = {
                "id": row.get("id"),
                "fields": {}
            }
            for k, v in row.items():
                if k not in ["id", "created_at", "updated_at"]:
                    record["fields"][k] = str(v) if v else None
            records.append(record)
        
        return records
    
    def transform(self, db_name, table_name, transformations):
        """Special task: Apply transformations to data"""
        cursor = self.connection.cursor()
        safe_table_name = f"{db_name}__{table_name}".replace("-", "_").replace(" ", "_")
        
        # Get all records
        cursor.execute(f"SELECT * FROM `{safe_table_name}`")
        rows = cursor.fetchall()
        
        # Convert to list of dicts
        data = []
        for row in rows:
            item = {"id": row.get("id")}
            for k, v in row.items():
                if k not in ["id", "created_at", "updated_at"]:
                    item[k] = v
            data.append(item)
        
        # Apply transformations
        for t in transformations:
            t_type = t.get("type")
            
            if t_type == "filter":
                column = t.get("column")
                operator = t.get("operator")
                value = t.get("value")
                data = [d for d in data if self._filter_match(d, column, operator, value)]
            
            elif t_type == "project":
                columns = t.get("columns", [])
                data = [{k: d[k] for k in columns if k in d} for d in data]
            
            elif t_type == "rename":
                old_name = t.get("from")
                new_name = t.get("to")
                for d in data:
                    if old_name in d:
                        d[new_name] = d.pop(old_name)
            
            elif t_type == "cast":
                column = t.get("column")
                to_type = t.get("to_type")
                for d in data:
                    if column in d and d[column] is not None:
                        try:
                            if to_type == "int":
                                d[column] = int(d[column])
                            elif to_type == "float":
                                d[column] = float(d[column])
                            elif to_type == "bool":
                                d[column] = bool(d[column])
                            elif to_type == "string":
                                d[column] = str(d[column])
                        except (ValueError, TypeError):
                            pass
            
            elif t_type == "sort":
                column = t.get("column")
                reverse = t.get("order") == "desc"
                data.sort(key=lambda x: (x.get(column) is None, x.get(column, "")), reverse=reverse)
            
            elif t_type == "limit":
                count = t.get("count", 10)
                data = data[:count]
        
        return data
    
    def _filter_match(self, item, column, operator, value):
        if column not in item:
            return False
        
        val = item[column]
        
        if val is None:
            return False
        
        try:
            if operator == "eq":
                return str(val) == str(value)
            elif operator == "ne":
                return str(val) != str(value)
            elif operator == "gt":
                return float(val) > float(value)
            elif operator == "lt":
                return float(val) < float(value)
            elif operator == "gte":
                return float(val) >= float(value)
            elif operator == "lte":
                return float(val) <= float(value)
            elif operator == "contains":
                return str(value).lower() in str(val).lower()
        except (ValueError, TypeError):
            return False
        
        return False
    
    def health(self):
        try:
            self.connection.ping()
            return True
        except:
            return False
    
    def list_databases(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM `databases` ORDER BY name")
        rows = cursor.fetchall()
        return [row["name"] for row in rows]