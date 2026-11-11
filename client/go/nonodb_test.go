package nonodb

import "testing"

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
