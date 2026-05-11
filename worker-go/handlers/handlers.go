package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/elsaawyy/distributed-db/worker-go/database"
	"github.com/elsaawyy/distributed-db/worker-go/election"
)

type Handler struct {
	db       *database.MySQLDB
	raftNode *election.RaftNode
}

func NewHandler(db *database.MySQLDB, raftNode *election.RaftNode) *Handler {
	return &Handler{db: db, raftNode: raftNode}
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

func (h *Handler) Replicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respond(w, http.StatusMethodNotAllowed, APIResponse{Error: "method not allowed"})
		return
	}

	// --- API Key Authentication ---
	expectedKey := os.Getenv("API_KEY")
	if expectedKey == "" {
		expectedKey = "default-secret-change-me"
	}
	providedKey := r.Header.Get("X-API-Key")
	if providedKey != expectedKey {
		log.Printf("[SECURITY] Unauthorized replication attempt from %s", r.RemoteAddr)
		respond(w, http.StatusForbidden, APIResponse{Error: "unauthorized: invalid API key"})
		return
	}
	// -----------------------------

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

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Go Worker is healthy",
		Data: map[string]interface{}{
			"node":     "worker-go",
			"time":     time.Now().Format(time.RFC3339),
			"isLeader": h.raftNode.IsLeader(),
			"leader":   h.raftNode.GetLeaderAddr(),
			"databases": func() []string {
				dbs, _ := h.db.ListDatabases()
				return dbs
			}(),
		},
	})
}

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
			"isLeader":  h.raftNode.IsLeader(),
			"leader":    h.raftNode.GetLeaderAddr(),
			"databases": databases,
		},
	})
}

func (h *Handler) Insert(w http.ResponseWriter, r *http.Request) {
	// Only accept writes if I am the leader
	if !h.raftNode.IsLeader() {
		leader := h.raftNode.GetLeaderAddr()
		if leader != "" {
			http.Redirect(w, r, leader+r.URL.Path, http.StatusTemporaryRedirect)
			return
		}
		respond(w, http.StatusServiceUnavailable, APIResponse{Error: "No leader available. Master is down and no worker has been elected yet."})
		return
	}

	// I am leader, handle the write
	var req struct {
		Database string                 `json:"database"`
		Table    string                 `json:"table"`
		Fields   map[string]interface{} `json:"fields"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respond(w, http.StatusBadRequest, APIResponse{Error: "invalid request body"})
		return
	}

	if req.Database == "" || req.Table == "" {
		respond(w, http.StatusBadRequest, APIResponse{Error: "'database' and 'table' are required"})
		return
	}

	record, err := h.db.Insert(req.Database, req.Table, req.Fields)
	if err != nil {
		respond(w, http.StatusBadRequest, APIResponse{Error: err.Error()})
		return
	}

	// Replicate to other worker
	otherWorker := h.raftNode.GetOtherWorkerAddr()
	if otherWorker != "" {
		go func() {
			payload := database.ReplicationPayload{
				Operation: "insert",
				Database:  req.Database,
				Table:     req.Table,
				Record:    record,
			}
			data, _ := json.Marshal(payload)
			client := &http.Client{Timeout: 5 * time.Second}
			client.Post(otherWorker+"/replicate", "application/json", bytes.NewReader(data))
		}()
	}

	respond(w, http.StatusCreated, APIResponse{
		Success: true,
		Message: "Record inserted",
		Data:    record,
	})
}

// Election endpoints
func (h *Handler) HandleVote(w http.ResponseWriter, r *http.Request) {
	var req election.VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respond(w, http.StatusBadRequest, APIResponse{Error: err.Error()})
		return
	}

	resp := h.raftNode.HandleVote(req)
	respond(w, http.StatusOK, APIResponse{Success: true, Data: resp})
}

func (h *Handler) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var heartbeat election.Heartbeat
	if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
		respond(w, http.StatusBadRequest, APIResponse{Error: err.Error()})
		return
	}

	h.raftNode.HandleHeartbeat(heartbeat)
	respond(w, http.StatusOK, APIResponse{Success: true})
}

func (h *Handler) GetLeader(w http.ResponseWriter, r *http.Request) {
	leader := h.raftNode.GetLeaderAddr()
	respond(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"leader": leader},
	})
}
