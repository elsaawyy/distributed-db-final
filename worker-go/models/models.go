package models

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type TableSchema struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

type Record struct {
	ID     string                 `json:"id"`
	Fields map[string]interface{} `json:"fields"`
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
