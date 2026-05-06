package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLDB struct {
	db *sql.DB
}

type Column struct {
	Name     string
	Type     string
	Required bool
}

type Record struct {
	ID        string                 `json:"id"`
	Fields    map[string]interface{} `json:"fields"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
}

type ReplicationPayload struct {
	Operation string                 `json:"operation"`
	Database  string                 `json:"database"`
	Table     string                 `json:"table,omitempty"`
	Schema    *TableSchema           `json:"schema,omitempty"`
	Record    *Record                `json:"record,omitempty"`
	RecordID  string                 `json:"record_id,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

type TableSchema struct {
	Name      string   `json:"name"`
	Columns   []Column `json:"columns"`
	CreatedAt string   `json:"created_at,omitempty"`
}

func NewMySQLDB(host, port, user, password, dbName string) (*MySQLDB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4&allowNativePasswords=true",
		user, password, host, port, dbName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	log.Println("Connected to MySQL successfully")
	return &MySQLDB{db: db}, nil
}

func (m *MySQLDB) Close() error {
	return m.db.Close()
}

func (m *MySQLDB) InitSchema() error {
	// Create databases metadata table
	_, err := m.db.Exec(`
		CREATE TABLE IF NOT EXISTS ` + "`databases`" + ` (
			name VARCHAR(255) PRIMARY KEY,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	if err != nil {
		return fmt.Errorf("failed to create databases table: %w", err)
	}

	// Create tables metadata table
	_, err = m.db.Exec(`
		CREATE TABLE IF NOT EXISTS tables_metadata (
			id INT AUTO_INCREMENT PRIMARY KEY,
			database_name VARCHAR(255),
			table_name VARCHAR(255),
			schema_json JSON,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE KEY unique_table (database_name, table_name),
			FOREIGN KEY (database_name) REFERENCES ` + "`databases`" + `(name) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	if err != nil {
		return fmt.Errorf("failed to create tables_metadata table: %w", err)
	}

	log.Println("Database schema initialized")
	return nil
}

func (m *MySQLDB) ApplyReplication(payload ReplicationPayload) error {
	switch payload.Operation {
	case "create_db":
		return m.createDatabase(payload.Database)
	case "drop_db":
		return m.dropDatabase(payload.Database)
	case "create_table":
		if payload.Schema == nil {
			return fmt.Errorf("missing schema in create_table replication")
		}
		columns := make([]Column, len(payload.Schema.Columns))
		for i, col := range payload.Schema.Columns {
			columns[i] = Column{
				Name:     col.Name,
				Type:     col.Type,
				Required: col.Required,
			}
		}
		return m.createTable(payload.Database, payload.Table, columns)
	case "insert":
		if payload.Record == nil {
			return fmt.Errorf("missing record in insert replication")
		}
		return m.insertRecord(payload.Database, payload.Table, payload.Record)
	case "update":
		return m.updateRecord(payload.Database, payload.Table, payload.RecordID, payload.Fields)
	case "delete":
		return m.deleteRecord(payload.Database, payload.Table, payload.RecordID)
	default:
		return fmt.Errorf("unknown operation: %s", payload.Operation)
	}
}

func (m *MySQLDB) createDatabase(name string) error {
	_, err := m.db.Exec("INSERT INTO `databases` (name) VALUES (?)", name)
	if err != nil && !strings.Contains(err.Error(), "Duplicate entry") {
		return err
	}
	log.Printf("Database '%s' created via replication", name)
	return nil
}

func (m *MySQLDB) dropDatabase(name string) error {
	_, err := m.db.Exec("DELETE FROM `databases` WHERE name = ?", name)
	if err != nil {
		return err
	}
	log.Printf("Database '%s' dropped via replication", name)
	return nil
}

func (m *MySQLDB) createTable(dbName, tableName string, columns []Column) error {
	// Check if database exists
	var exists bool
	err := m.db.QueryRow("SELECT EXISTS(SELECT 1 FROM `databases` WHERE name = ?)", dbName).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("database '%s' does not exist", dbName)
	}

	// Store schema in metadata
	schemaJSON, err := json.Marshal(columns)
	if err != nil {
		return err
	}

	_, err = m.db.Exec(`
		INSERT INTO tables_metadata (database_name, table_name, schema_json)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE schema_json = VALUES(schema_json)
	`, dbName, tableName, schemaJSON)
	if err != nil {
		return err
	}

	// Create actual table
	safeTableName := fmt.Sprintf("%s__%s", dbName, tableName)
	safeTableName = strings.ReplaceAll(safeTableName, "-", "_")
	safeTableName = strings.ReplaceAll(safeTableName, " ", "_")

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (", safeTableName)
	query += "`id` VARCHAR(255) PRIMARY KEY, "
	query += "`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
	query += "`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"

	for _, col := range columns {
		sqlType := map[string]string{
			"string": "TEXT",
			"int":    "BIGINT",
			"float":  "DOUBLE",
			"bool":   "BOOLEAN",
		}[col.Type]
		if sqlType == "" {
			sqlType = "TEXT"
		}

		query += fmt.Sprintf(", `%s` %s", col.Name, sqlType)
		if col.Required {
			query += " NOT NULL"
		}
	}
	query += ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	_, err = m.db.Exec(query)
	if err != nil {
		return err
	}

	log.Printf("Table '%s' created in database '%s' via replication", tableName, dbName)
	return nil
}

func (m *MySQLDB) insertRecord(dbName, tableName string, record *Record) error {
	safeTableName := fmt.Sprintf("%s__%s", dbName, tableName)
	safeTableName = strings.ReplaceAll(safeTableName, "-", "_")
	safeTableName = strings.ReplaceAll(safeTableName, " ", "_")

	columns := []string{"id"}
	values := []interface{}{record.ID}
	placeholders := []string{"?"}

	for k, v := range record.Fields {
		columns = append(columns, k)
		values = append(values, v)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		safeTableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := m.db.Exec(query, values...)
	if err != nil {
		return err
	}

	log.Printf("Record inserted into '%s.%s' via replication", dbName, tableName)
	return nil
}

func (m *MySQLDB) updateRecord(dbName, tableName, id string, fields map[string]interface{}) error {
	safeTableName := fmt.Sprintf("%s__%s", dbName, tableName)
	safeTableName = strings.ReplaceAll(safeTableName, "-", "_")
	safeTableName = strings.ReplaceAll(safeTableName, " ", "_")

	setClauses := make([]string, 0)
	args := make([]interface{}, 0)

	for k, v := range fields {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", k))
		args = append(args, v)
	}
	args = append(args, id)

	query := fmt.Sprintf("UPDATE `%s` SET %s WHERE id = ?",
		safeTableName,
		strings.Join(setClauses, ", "))

	_, err := m.db.Exec(query, args...)
	if err != nil {
		return err
	}

	log.Printf("Record '%s' updated in '%s.%s' via replication", id, dbName, tableName)
	return nil
}

func (m *MySQLDB) deleteRecord(dbName, tableName, id string) error {
	safeTableName := fmt.Sprintf("%s__%s", dbName, tableName)
	safeTableName = strings.ReplaceAll(safeTableName, "-", "_")
	safeTableName = strings.ReplaceAll(safeTableName, " ", "_")

	query := fmt.Sprintf("DELETE FROM `%s` WHERE id = ?", safeTableName)
	_, err := m.db.Exec(query, id)
	if err != nil {
		return err
	}

	log.Printf("Record '%s' deleted from '%s.%s' via replication", id, dbName, tableName)
	return nil
}

func (m *MySQLDB) Select(dbName, tableName string, where map[string]string) ([]*Record, error) {
	safeTableName := fmt.Sprintf("%s__%s", dbName, tableName)
	safeTableName = strings.ReplaceAll(safeTableName, "-", "_")
	safeTableName = strings.ReplaceAll(safeTableName, " ", "_")

	query := fmt.Sprintf("SELECT * FROM `%s`", safeTableName)
	var args []interface{}

	if len(where) > 0 {
		conditions := make([]string, 0)
		for k, v := range where {
			conditions = append(conditions, fmt.Sprintf("`%s` = ?", k))
			args = append(args, v)
		}
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	rows, err := m.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var records []*Record
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			var v interface{}
			values[i] = &v
		}

		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		record := &Record{
			Fields: make(map[string]interface{}),
		}

		for i, col := range columns {
			valPtr := values[i].(*interface{})
			val := *valPtr

			if val == nil {
				continue
			}

			switch col {
			case "id":
				if strVal, ok := val.(string); ok {
					record.ID = strVal
				} else if bytesVal, ok := val.([]byte); ok {
					record.ID = string(bytesVal)
				}
			case "created_at", "updated_at":
				// Skip timestamps
			default:
				if bytesVal, ok := val.([]byte); ok {
					record.Fields[col] = string(bytesVal)
				} else {
					record.Fields[col] = val
				}
			}
		}
		records = append(records, record)
	}

	return records, nil
}

func (m *MySQLDB) Analytics(dbName, tableName string) (map[string]interface{}, error) {
	safeTableName := fmt.Sprintf("%s__%s", dbName, tableName)
	safeTableName = strings.ReplaceAll(safeTableName, "-", "_")
	safeTableName = strings.ReplaceAll(safeTableName, " ", "_")

	// Get total row count
	var totalRows int
	err := m.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", safeTableName)).Scan(&totalRows)
	if err != nil {
		return nil, err
	}

	// Get column info
	rows, err := m.db.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 0", safeTableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Get column stats
	columnStats := make(map[string]interface{})
	for _, col := range columns {
		if col == "id" || col == "created_at" || col == "updated_at" {
			continue
		}

		// Get non-null count and unique values
		var nonNullCount int
		var uniqueCount int

		m.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE `%s` IS NOT NULL", safeTableName, col)).Scan(&nonNullCount)
		m.db.QueryRow(fmt.Sprintf("SELECT COUNT(DISTINCT `%s`) FROM `%s`", col, safeTableName)).Scan(&uniqueCount)

		// Get sample values
		sampleRows, err := m.db.Query(fmt.Sprintf("SELECT DISTINCT `%s` FROM `%s` WHERE `%s` IS NOT NULL LIMIT 5", col, safeTableName, col))
		if err != nil {
			sampleRows.Close()
			continue
		}

		var samples []string
		for sampleRows.Next() {
			var val sql.NullString
			if err := sampleRows.Scan(&val); err == nil && val.Valid {
				samples = append(samples, val.String)
			}
		}
		sampleRows.Close()

		columnStats[col] = map[string]interface{}{
			"non_null_count": nonNullCount,
			"unique_values":  uniqueCount,
			"sample_values":  samples,
		}
	}

	return map[string]interface{}{
		"database":     dbName,
		"table":        tableName,
		"total_rows":   totalRows,
		"column_stats": columnStats,
	}, nil
}

func (m *MySQLDB) ListDatabases() ([]string, error) {
	rows, err := m.db.Query("SELECT name FROM `databases` ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		databases = append(databases, name)
	}
	return databases, nil
}

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
