package main

import (
	"embed"
	"io/fs"
	"log"
	"net/http"
	"os"

	"github.com/elsaawyy/distributed-db/master/database"
	"github.com/elsaawyy/distributed-db/master/handlers"
	"github.com/elsaawyy/distributed-db/master/replication"
)

//go:embed web
var webFS embed.FS

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("[MASTER] ")

	mysqlHost := getEnv("MYSQL_HOST", "localhost")
	mysqlPort := getEnv("MYSQL_PORT", "3309")
	mysqlUser := getEnv("MYSQL_USER", "root")
	mysqlPass := getEnv("MYSQL_PASS", "")
	mysqlDB := getEnv("MYSQL_DATABASE", "master_db")

	db, err := database.NewMySQLDB(mysqlHost, mysqlPort, mysqlUser, mysqlPass, mysqlDB)
	if err != nil {
		log.Fatal("Failed to connect to MySQL:", err)
	}
	defer db.Close()

	if err := db.InitSchema(); err != nil {
		log.Fatal("Failed to initialize schema:", err)
	}

	workerAddrs := []string{
		getEnv("WORKER_GO_ADDR", "http://localhost:8081"),
		getEnv("WORKER_PY_ADDR", "http://localhost:8082"),
	}
	replicator := replication.NewManager(workerAddrs)

	h := handlers.NewHandler(db, replicator)

	mux := http.NewServeMux()

	// Database management
	mux.HandleFunc("/create-db", h.CreateDatabase)
	mux.HandleFunc("/drop-db", h.DropDatabase)
	mux.HandleFunc("/list-dbs", h.ListDatabases)

	// Table management
	mux.HandleFunc("/create-table", h.CreateTable)
	mux.HandleFunc("/list-tables", h.ListTables)

	// Data operations
	mux.HandleFunc("/insert", h.Insert)
	mux.HandleFunc("/select", h.Select)
	mux.HandleFunc("/update", h.Update)
	mux.HandleFunc("/delete", h.Delete)
	mux.HandleFunc("/search", h.Search)

	// Health & Status
	mux.HandleFunc("/health", h.Health)
	mux.HandleFunc("/status", h.Status)

	// Proxy routes for worker special tasks (GUI uses these)
	mux.HandleFunc("/proxy/analytics", h.ProxyToGoAnalytics)
	mux.HandleFunc("/proxy/transform", h.ProxyToPythonTransform)

	// GUI - serve embedded web files
	webContent, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatal("Failed to load web folder:", err)
	}
	mux.Handle("/ui/", http.StripPrefix("/ui/", http.FileServer(http.FS(webContent))))
	mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusMovedPermanently)
	})

	port := getEnv("MASTER_PORT", "8080")
	addr := "0.0.0.0:" + port

	log.Printf("Master node starting on %s", addr)
	log.Printf("Connected workers: %v", workerAddrs)
	log.Printf("GUI available at http://localhost:%s/ui/", port)

	if err := http.ListenAndServe(addr, loggingMiddleware(mux)); err != nil {
		log.Fatalf("Master failed to start: %v", err)
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("→ %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
