package handlers

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/elsaawyy/distributed-db/worker-go/database"
)

type Handler struct {
	db *database.MySQLDB
}

func NewHandler(db *database.MySQLDB) *Handler {
	return &Handler{db: db}
}

type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func respond(w http.ResponseWriter, status int, resp APIResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

// Replicate handles POST /replicate - called by Master
func (h *Handler) Replicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respond(w, http.StatusMethodNotAllowed, APIResponse{Error: "method not allowed"})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		respond(w, http.StatusBadRequest, APIResponse{Error: "could not read body"})
		return
	}

	var payload database.ReplicationPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		respond(w, http.StatusBadRequest, APIResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	log.Printf("Replicating operation: %s on %s", payload.Operation, payload.Database)

	if err := h.db.ApplyReplication(payload); err != nil {
		respond(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
		return
	}

	respond(w, http.StatusOK, APIResponse{Success: true, Message: "Replicated: " + payload.Operation})
}

// Select handles GET /select - fault-tolerant reads
func (h *Handler) Select(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respond(w, http.StatusMethodNotAllowed, APIResponse{Error: "method not allowed"})
		return
	}

	q := r.URL.Query()
	dbName := q.Get("database")
	tableName := q.Get("table")

	if dbName == "" || tableName == "" {
		respond(w, http.StatusBadRequest, APIResponse{Error: "'database' and 'table' query params required"})
		return
	}

	where := make(map[string]string)
	for key, vals := range q {
		if len(key) > 6 && key[:6] == "where_" {
			where[key[6:]] = vals[0]
		}
	}

	records, err := h.db.Select(dbName, tableName, where)
	if err != nil {
		respond(w, http.StatusBadRequest, APIResponse{Error: err.Error()})
		return
	}

	if records == nil {
		records = []*database.Record{}
	}

	respond(w, http.StatusOK, APIResponse{Success: true, Data: records})
}

// Analytics handles GET /analytics - special task
func (h *Handler) Analytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respond(w, http.StatusMethodNotAllowed, APIResponse{Error: "method not allowed"})
		return
	}

	q := r.URL.Query()
	dbName := q.Get("database")
	tableName := q.Get("table")

	if dbName == "" || tableName == "" {
		respond(w, http.StatusBadRequest, APIResponse{Error: "'database' and 'table' required"})
		return
	}

	result, err := h.db.Analytics(dbName, tableName)
	if err != nil {
		respond(w, http.StatusBadRequest, APIResponse{Error: err.Error()})
		return
	}

	respond(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Analytics computed by Go Worker",
		Data:    result,
	})
}

// Health handles GET /health
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Go Worker is healthy",
		Data: map[string]interface{}{
			"node": "worker-go",
			"time": time.Now().Format(time.RFC3339),
			"databases": func() []string {
				dbs, _ := h.db.ListDatabases()
				return dbs
			}(),
		},
	})
}

// Status handles GET /status
func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	databases, err := h.db.ListDatabases()
	if err != nil {
		respond(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
		return
	}

	respond(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"node":      "worker-go",
			"databases": databases,
		},
	})
}
