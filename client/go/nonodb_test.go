package nonodb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestReadOps(t *testing.T) {
	if !isReadOp(map[string]any{"op": "find"}) {
		t.Fatal("find should be retryable")
	}
	if !isReadOp(map[string]any{"op": "batch"}) {
		t.Fatal("batch should be retryable")
	}
	if !isReadOp(map[string]any{"op": "sql"}) {
		t.Fatal("sql should be retryable")
	}
	if !isReadOp(map[string]any{"op": "search_all"}) {
		t.Fatal("search_all should be retryable")
	}
	if !isReadOp(map[string]any{"op": "explain_sql"}) {
		t.Fatal("explain_sql should be retryable")
	}
	if isReadOp(map[string]any{"op": "write_batch"}) {
		t.Fatal("write_batch must not be retried")
	}
	if isReadOp(map[string]any{"op": "insert"}) {
		t.Fatal("insert must not be retried")
	}
}

func TestDatabasePath(t *testing.T) {
	c := &Client{database: "app1"}
	if got := c.dataPath("/v1/exec"); got != "/app1/v1/exec" {
		t.Fatalf("dataPath = %q", got)
	}
	c.database = "default"
	if got := c.dataPath("/v1/exec"); got != "/v1/exec" {
		t.Fatalf("default dataPath = %q", got)
	}
}

func TestSQLWriteIsWrite(t *testing.T) {
	if !isWriteOp(map[string]any{"op": "sql", "query": "INSERT INTO c (x) VALUES (1)"}) {
		t.Fatal("SQL INSERT must be routed as a write")
	}
	if !isWriteOp(map[string]any{"op": "sql", "query": "/* c */ WITH x AS (SELECT 1) INSERT INTO c SELECT * FROM x"}) {
		t.Fatal("SQL CTE write must be routed as a write")
	}
	if isWriteOp(map[string]any{"op": "sql", "query": " -- c\n SELECT * FROM c"}) {
		t.Fatal("SQL SELECT must stay a read")
	}
}

func TestBatchWithWriteIsWrite(t *testing.T) {
	if !isWriteOp(map[string]any{"op": "batch", "ops": []map[string]any{
		{"op": "find"}, {"op": "insert"},
	}}) {
		t.Fatal("mixed batch must be routed as a write")
	}
	if isWriteOp(map[string]any{"op": "batch", "ops": []any{
		map[string]any{"op": "find"}, map[string]any{"op": "count"},
	}}) {
		t.Fatal("read-only batch must stay a read")
	}
}

func TestAggregateWriteClassification(t *testing.T) {
	if !isWriteOp(map[string]any{
		"op":       "aggregate",
		"pipeline": []any{map[string]any{"$out": "target"}},
	}) {
		t.Fatal("$out aggregate must be a write")
	}
	if !isWriteOp(map[string]any{
		"op":       "aggregate",
		"pipeline": []any{map[string]any{"$merge": map[string]any{"into": "target"}}},
	}) {
		t.Fatal("$merge aggregate must be a write")
	}
	if isWriteOp(map[string]any{
		"op":       "aggregate",
		"pipeline": []any{map[string]any{"$match": map[string]any{"x": 1}}},
	}) {
		t.Fatal("read aggregate must not be a write")
	}
}

func TestWriteRouting(t *testing.T) {
	var primaryWrites, replicaWrites atomic.Int64
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryWrites.Add(1)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"inserted_id":"doc-1"}}`))
	}))
	defer primary.Close()

	var replicaURL string
	replica := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":          true,
				"replication": map[string]any{"write_url": primary.URL},
			})
			return
		}
		replicaWrites.Add(1)
		w.Header().Set("content-type", "application/json")
		w.Header().Set("location", primary.URL)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"ok":false,"error":{"code":"read_only","message":"replica","write_url":"` + primary.URL + `"}}`))
	}))
	defer replica.Close()
	replicaURL = replica.URL

	c := New(WithBaseURL(replicaURL), WithAutoPrimary(true), WithToken("test"))
	if _, err := c.Exec(context.Background(), map[string]any{"op": "insert", "coll": "c", "doc": map[string]any{"x": 1}}); err != nil {
		t.Fatal(err)
	}
	if got := primaryWrites.Load(); got != 1 {
		t.Fatalf("primary writes = %d", got)
	}
	if got := replicaWrites.Load(); got != 0 {
		t.Fatalf("replica writes = %d", got)
	}
}

func TestSearchAllUsesReadEndpoint(t *testing.T) {
	var reads atomic.Int64
	replica := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
			return
		}
		reads.Add(1)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"results":[{"_id":"doc-1"}]}}`))
	}))
	defer replica.Close()

	c := New(
		WithBaseURL(replica.URL),
		WithWriteURL("http://127.0.0.1:9"),
		WithToken("test"),
	)
	results, err := c.Collection("c").SearchAll(context.Background(), SearchAllOptions{
		Mode:  "lexical",
		Query: "coffee",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0]["_id"] != "doc-1" {
		t.Fatalf("results = %#v", results)
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("read endpoint requests = %d", got)
	}
}
