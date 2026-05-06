package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/elsaawyy/distributed-db/master/database"
	"github.com/elsaawyy/distributed-db/master/models"
	"github.com/elsaawyy/distributed-db/master/replication"
)

type Handler struct {
	db         *database.MySQLDB
	replicator *replication.Manager
}

func NewHandler(db *database.MySQLDB, replicator *replication.Manager) *Handler {
	return &Handler{db: db, replicator: replicator}
}

func respond(w http.ResponseWriter, status int, resp models.APIResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

func decodeBody(r *http.Request, dst interface{}) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, dst)
}

func (h *Handler) CreateDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	var req models.CreateDBRequest
	if err := decodeBody(r, &req); err != nil || req.Name == "" {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: "invalid request body; 'name' is required"})
		return
	}
	if err := h.db.CreateDatabase(req.Name); err != nil {
		respond(w, http.StatusConflict, models.APIResponse{Error: err.Error()})
		return
	}
	h.replicator.Replicate(models.ReplicationPayload{
		Operation: "create_db",
		Database:  req.Name,
	})
	respond(w, http.StatusCreated, models.APIResponse{Success: true, Message: "Database created: " + req.Name})
}

func (h *Handler) DropDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	var req models.DropDBRequest
	if err := decodeBody(r, &req); err != nil || req.Name == "" {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: "invalid request body; 'name' is required"})
		return
	}
	if err := h.db.DropDatabase(req.Name); err != nil {
		respond(w, http.StatusNotFound, models.APIResponse{Error: err.Error()})
		return
	}
	h.replicator.Replicate(models.ReplicationPayload{
		Operation: "drop_db",
		Database:  req.Name,
	})
	respond(w, http.StatusOK, models.APIResponse{Success: true, Message: "Database dropped: " + req.Name})
}

func (h *Handler) ListDatabases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	dbs, err := h.db.ListDatabases()
	if err != nil {
		respond(w, http.StatusInternalServerError, models.APIResponse{Error: err.Error()})
		return
	}
	respond(w, http.StatusOK, models.APIResponse{Success: true, Data: dbs})
}

func (h *Handler) CreateTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	var req models.CreateTableRequest
	if err := decodeBody(r, &req); err != nil || req.Database == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: "invalid request; 'database' and 'table' are required"})
		return
	}
	columns := make([]database.Column, len(req.Columns))
	for i, col := range req.Columns {
		columns[i] = database.Column{
			Name:     col.Name,
			Type:     string(col.Type),
			Required: col.Required,
		}
	}
	if err := h.db.CreateTable(req.Database, req.Table, columns); err != nil {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: err.Error()})
		return
	}
	schema := models.TableSchema{
		Name:    req.Table,
		Columns: req.Columns,
	}
	h.replicator.Replicate(models.ReplicationPayload{
		Operation: "create_table",
		Database:  req.Database,
		Table:     req.Table,
		Schema:    &schema,
	})
	respond(w, http.StatusCreated, models.APIResponse{
		Success: true,
		Message: "Table created: " + req.Table,
		Data:    schema,
	})
}

func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	dbName := r.URL.Query().Get("database")
	if dbName == "" {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: "'database' query param required"})
		return
	}
	tables, err := h.db.GetTables(dbName)
	if err != nil {
		respond(w, http.StatusInternalServerError, models.APIResponse{Error: err.Error()})
		return
	}
	respond(w, http.StatusOK, models.APIResponse{Success: true, Data: tables})
}

func (h *Handler) Insert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	var req models.InsertRequest
	if err := decodeBody(r, &req); err != nil || req.Database == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: "invalid request; 'database', 'table', and 'fields' are required"})
		return
	}
	record, err := h.db.Insert(req.Database, req.Table, req.Fields)
	if err != nil {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: err.Error()})
		return
	}
	h.replicator.Replicate(models.ReplicationPayload{
		Operation: "insert",
		Database:  req.Database,
		Table:     req.Table,
		Record: &models.Record{
			ID:     record.ID,
			Fields: record.Fields,
		},
	})
	respond(w, http.StatusCreated, models.APIResponse{Success: true, Message: "Record inserted", Data: record})
}

func (h *Handler) Select(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	q := r.URL.Query()
	dbName := q.Get("database")
	tableName := q.Get("table")
	if dbName == "" || tableName == "" {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: "'database' and 'table' query params required"})
		return
	}
	where := make(map[string]string)
	for key, vals := range q {
		if strings.HasPrefix(key, "where_") {
			where[key[6:]] = vals[0]
		}
	}
	records, err := h.db.Select(dbName, tableName, where)
	if err != nil {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: err.Error()})
		return
	}
	if records == nil {
		records = []*database.Record{}
	}
	// Convert to model records
	result := make([]*models.Record, len(records))
	for i, r := range records {
		result[i] = &models.Record{
			ID:     r.ID,
			Fields: r.Fields,
		}
	}
	respond(w, http.StatusOK, models.APIResponse{Success: true, Data: result})
}

func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	var req models.UpdateRequest
	if err := decodeBody(r, &req); err != nil || req.Database == "" || req.Table == "" || req.ID == "" {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: "invalid request; 'database', 'table', 'id', and 'fields' are required"})
		return
	}
	if err := h.db.Update(req.Database, req.Table, req.ID, req.Fields); err != nil {
		respond(w, http.StatusNotFound, models.APIResponse{Error: err.Error()})
		return
	}
	h.replicator.Replicate(models.ReplicationPayload{
		Operation: "update",
		Database:  req.Database,
		Table:     req.Table,
		RecordID:  req.ID,
		Fields:    req.Fields,
	})
	respond(w, http.StatusOK, models.APIResponse{Success: true, Message: "Record updated"})
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	var req models.DeleteRequest
	if err := decodeBody(r, &req); err != nil || req.Database == "" || req.Table == "" || req.ID == "" {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: "invalid request; 'database', 'table', and 'id' are required"})
		return
	}
	if err := h.db.Delete(req.Database, req.Table, req.ID); err != nil {
		respond(w, http.StatusNotFound, models.APIResponse{Error: err.Error()})
		return
	}
	h.replicator.Replicate(models.ReplicationPayload{
		Operation: "delete",
		Database:  req.Database,
		Table:     req.Table,
		RecordID:  req.ID,
	})
	respond(w, http.StatusOK, models.APIResponse{Success: true, Message: "Record deleted"})
}

func (h *Handler) Search(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	q := r.URL.Query()
	dbName := q.Get("database")
	tableName := q.Get("table")
	column := q.Get("column")
	value := q.Get("value")
	if dbName == "" || tableName == "" || column == "" {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: "'database', 'table', and 'column' are required"})
		return
	}
	records, err := h.db.Search(dbName, tableName, column, value)
	if err != nil {
		respond(w, http.StatusBadRequest, models.APIResponse{Error: err.Error()})
		return
	}
	if records == nil {
		records = []*database.Record{}
	}
	result := make([]*models.Record, len(records))
	for i, r := range records {
		result[i] = &models.Record{
			ID:     r.ID,
			Fields: r.Fields,
		}
	}
	respond(w, http.StatusOK, models.APIResponse{Success: true, Data: result})
}

func (h *Handler) ProxyToGoAnalytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	workers := h.replicator.WorkerStatuses()
	var goWorkerAddr string
	for _, w := range workers {
		if strings.Contains(w.Address, "8081") {
			goWorkerAddr = w.Address
			break
		}
	}
	if goWorkerAddr == "" {
		respond(w, http.StatusServiceUnavailable, models.APIResponse{Error: "Go worker not available"})
		return
	}
	url := goWorkerAddr + "/analytics?" + r.URL.RawQuery
	resp, err := http.Get(url)
	if err != nil {
		respond(w, http.StatusBadGateway, models.APIResponse{Error: err.Error()})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (h *Handler) ProxyToPythonTransform(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respond(w, http.StatusMethodNotAllowed, models.APIResponse{Error: "method not allowed"})
		return
	}
	workers := h.replicator.WorkerStatuses()
	var pyWorkerAddr string
	for _, w := range workers {
		if strings.Contains(w.Address, "8082") {
			pyWorkerAddr = w.Address
			break
		}
	}
	if pyWorkerAddr == "" {
		respond(w, http.StatusServiceUnavailable, models.APIResponse{Error: "Python worker not available"})
		return
	}
	bodyBytes, _ := io.ReadAll(r.Body)
	url := pyWorkerAddr + "/transform"
	resp, err := http.Post(url, "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		respond(w, http.StatusBadGateway, models.APIResponse{Error: err.Error()})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, models.APIResponse{
		Success: true,
		Message: "Master is healthy",
		Data:    map[string]string{"node": "master", "time": time.Now().Format(time.RFC3339)},
	})
}

func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	workers := h.replicator.WorkerStatuses()
	databases, err := h.db.ListDatabases()
	if err != nil {
		databases = []string{}
	}
	respond(w, http.StatusOK, models.APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"node":      "master",
			"databases": databases,
			"workers":   workers,
		},
	})
}
