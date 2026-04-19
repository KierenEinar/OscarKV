package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	OscarKV "OscarKV"
)

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type apiError struct {
	Error string `json:"error"`
}

type apiValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "http listen address")
	dir := flag.String("dir", "./workdir-http", "OscarKV root directory")
	flag.Parse()

	db, err := OscarKV.Open(&OscarKV.Option{
		CommitBuffer:             64,
		RootDir:                  *dir,
		WriteBatchCountThreshold: 16,
		WriteBatchSizeThreshold:  1 << 16,
		MemoryLimitPerMemtable:   8 << 20,
		MaxImmutableMemtable:     16,
		MaxLevelPerMemtable:      32,
		StrictMode:               true,
		RandFactorPerMemtable:    0.5,
		SlowDownDurationMs:       0,
		PreferedLevel:            8,
	})
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	mux := http.NewServeMux()
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, apiError{Error: "method not allowed"})
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			writeJSON(w, http.StatusBadRequest, apiError{Error: "missing key"})
			return
		}
		val, err := db.Get([]byte(key))
		if err != nil {
			if errors.Is(err, OscarKV.ErrKeyNotFound) {
				writeJSON(w, http.StatusNotFound, apiError{Error: "not found"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, apiValue{Key: key, Value: string(val)})
	})

	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, apiError{Error: "method not allowed"})
			return
		}
		var req setRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, apiError{Error: "invalid json body"})
			return
		}
		if req.Key == "" {
			writeJSON(w, http.StatusBadRequest, apiError{Error: "missing key"})
			return
		}
		if err := db.Set([]byte(req.Key), []byte(req.Value)); err != nil {
			writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, apiValue{Key: req.Key, Value: req.Value})
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, apiError{Error: "method not allowed"})
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("OscarKV http server listening on http://%s", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

