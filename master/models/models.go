package models

import "time"

type ColumnType string

const (
	TypeString ColumnType = "string"
	TypeInt    ColumnType = "int"
	TypeFloat  ColumnType = "float"
	TypeBool   ColumnType = "bool"
)

type Column struct {
	Name     string     `json:"name"`
	Type     ColumnType `json:"type"`
	Required bool       `json:"required"`
}

type TableSchema struct {
	Name      string    `json:"name"`
	Columns   []Column  `json:"columns"`
	CreatedAt time.Time `json:"created_at"`
}

type Record struct {
	ID        string                 `json:"id"`
	Fields    map[string]interface{} `json:"fields"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
}

type CreateDBRequest struct {
	Name string `json:"name"`
}

type CreateTableRequest struct {
	Database string   `json:"database"`
	Table    string   `json:"table"`
	Columns  []Column `json:"columns"`
}

type InsertRequest struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Fields   map[string]interface{} `json:"fields"`
}

type UpdateRequest struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	ID       string                 `json:"id"`
	Fields   map[string]interface{} `json:"fields"`
}

type DeleteRequest struct {
	Database string `json:"database"`
	Table    string `json:"table"`
	ID       string `json:"id"`
}

type DropDBRequest struct {
	Name string `json:"name"`
}

type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
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
