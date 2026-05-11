package main

import (
	"log"
	"net/http"
	"os"

	"github.com/elsaawyy/distributed-db/worker-go/database"
	"github.com/elsaawyy/distributed-db/worker-go/election"
	"github.com/elsaawyy/distributed-db/worker-go/handlers"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("[WORKER-GO] ")

	mysqlHost := getEnv("MYSQL_HOST", "localhost")
	mysqlPort := getEnv("MYSQL_PORT", "3309")
	mysqlUser := getEnv("MYSQL_USER", "root")
	mysqlPass := getEnv("MYSQL_PASS", "")
	mysqlDB := getEnv("MYSQL_DATABASE", "worker_go_db")

	db, err := database.NewMySQLDB(mysqlHost, mysqlPort, mysqlUser, mysqlPass, mysqlDB)
	if err != nil {
		log.Fatal("Failed to connect to MySQL:", err)
	}
	defer db.Close()

	if err := db.InitSchema(); err != nil {
		log.Fatal("Failed to initialize schema:", err)
	}

	// Master monitoring - workers only elect among themselves if master dies
	myAddr := getEnv("WORKER_GO_ADDR", "http://localhost:8081")
	masterAddr := getEnv("MASTER_ADDR", "http://localhost:8080")
	peers := []string{
		getEnv("WORKER_PY_ADDR", "http://localhost:8082"),
	}
	raftNode := election.NewRaftNode(myAddr, peers, masterAddr)
	raftNode.Start()

	h := handlers.NewHandler(db, raftNode)

	mux := http.NewServeMux()

	mux.HandleFunc("/replicate", h.Replicate)
	mux.HandleFunc("/select", h.Select)
	mux.HandleFunc("/analytics", h.Analytics)
	mux.HandleFunc("/insert", h.Insert) // ← ADD THIS LINE
	mux.HandleFunc("/health", h.Health)
	mux.HandleFunc("/status", h.Status)

	// Election endpoints
	mux.HandleFunc("/vote", h.HandleVote)
	mux.HandleFunc("/heartbeat", h.HandleHeartbeat)
	mux.HandleFunc("/leader", h.GetLeader)

	port := getEnv("WORKER_GO_PORT", "8081")
	addr := "0.0.0.0:" + port

	log.Printf("Go Worker node starting on %s", addr)
	log.Printf("MySQL database: %s", mysqlDB)
	log.Printf("Monitoring master at: %s", masterAddr)

	if err := http.ListenAndServe(addr, loggingMiddleware(mux)); err != nil {
		log.Fatalf("Go Worker failed to start: %v", err)
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("→ %s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
