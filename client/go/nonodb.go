// Package nonodb is a thin Go client for nonodb.
//
// It speaks HTTP/JSON to nonodb-daemon. By default it reads NONODB_SOCK and
// uses a Unix socket on Unix, or NONODB_URL and uses TCP on Windows. Set a base
// URL to use TCP/TLS explicitly.
package nonodb

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const maxResponseBytes = 8 << 20

// APIError is an error returned by the daemon.
type APIError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	StatusCode int    `json:"-"`
}

func (e *APIError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("%s: %s (HTTP %d)", e.Code, e.Message, e.StatusCode)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// BatchEntry is one result from Batch or WriteBatch.
type BatchEntry struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *APIError       `json:"error,omitempty"`
}

// UpdateOptions controls Update and UpdateResult.
type UpdateOptions struct {
	Multi  bool
	Upsert bool
}

// UpdateResult is returned by UpdateResult.
type UpdateResult struct {
	Matched    int64   `json:"matched"`
	Modified   int64   `json:"modified"`
	UpsertedID *string `json:"upserted_id"`
}

// SortField is one sort key in a FindQuery.
type SortField struct {
	Field     string
	Direction int // 1 for ascending, -1 for descending
}

// FindQuery describes a find operation.
type FindQuery struct {
	Filter     map[string]any
	Projection map[string]any
	Sort       []SortField
	Skip       int64
	Limit      *int64
}

// IndexOptions controls CreateIndex.
type IndexOptions struct {
	Unique  bool
	Include []string
}

// SearchOptions controls Search.
type SearchOptions struct {
	Filter map[string]any
	Skip   int64
	Limit  int64
}

// FuzzyOptions controls Fuzzy.
type FuzzyOptions struct {
	MaxDistance int
	Filter      map[string]any
	Limit       int64
}

type config struct {
	socketPath string
	baseURL    string
	token      string
	timeout    time.Duration
	verify     bool
}

// Option configures a Client.
type Option func(*config)

// WithSocketPath connects through a Unix domain socket.
func WithSocketPath(path string) Option {
	return func(c *config) { c.socketPath = path }
}

// WithBaseURL connects through HTTP over TCP/TLS. It takes precedence over a
// socket path.
func WithBaseURL(url string) Option {
	return func(c *config) { c.baseURL = strings.TrimRight(url, "/") }
}

// WithToken sets the bearer token.
func WithToken(token string) Option {
	return func(c *config) { c.token = token }
}

// WithTimeout sets the HTTP timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *config) { c.timeout = timeout }
}

// WithTLSVerify enables or disables certificate verification.
func WithTLSVerify(verify bool) Option {
	return func(c *config) { c.verify = verify }
}

// Client is a thin nonodb client. It is safe for concurrent use.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

// New creates a client. Defaults come from NONODB_SOCK, NONODB_URL,
// NONODB_TOKEN, NONODB_TIMEOUT, and NONODB_TLS_VERIFY.
func New(options ...Option) *Client {
	cfg := config{
		socketPath: os.Getenv("NONODB_SOCK"),
		baseURL:    strings.TrimRight(os.Getenv("NONODB_URL"), "/"),
		token:      os.Getenv("NONODB_TOKEN"),
		timeout:    envTimeout(),
		verify:     envVerify(),
	}
	for _, option := range options {
		option(&cfg)
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !cfg.verify}, //nolint:gosec
	}
	baseURL := cfg.baseURL
	if baseURL == "" {
		if cfg.socketPath == "" {
			if runtime.GOOS == "windows" {
				baseURL = "http://127.0.0.1:7878"
			} else {
				cfg.socketPath = "/run/nonodb.sock"
			}
		}
		if cfg.socketPath != "" {
			socketPath := cfg.socketPath
			transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", socketPath)
			}
			baseURL = "http://localhost"
		}
	}

	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   cfg.token,
		http: &http.Client{
			Transport: transport,
			Timeout:   cfg.timeout,
		},
	}
}

func envTimeout() time.Duration {
	value := os.Getenv("NONODB_TIMEOUT")
	if value == "" {
		return 10 * time.Second
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || seconds <= 0 {
		return 10 * time.Second
	}
	return time.Duration(seconds * float64(time.Second))
}

func envVerify() bool {
	switch strings.ToLower(os.Getenv("NONODB_TLS_VERIFY")) {
	case "0", "false", "no":
		return false
	default:
		return true
	}
}

// Health checks GET /healthz.
func (c *Client) Health(ctx context.Context) bool {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/healthz", nil)
	if err != nil {
		return false
	}
	response, err := c.http.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode == http.StatusOK
}

// Exec executes an arbitrary /v1/exec payload and returns its result.
func (c *Client) Exec(ctx context.Context, payload map[string]any) (any, error) {
	raw, err := c.execRaw(ctx, payload)
	if err != nil {
		return nil, err
	}
	return decodeAny(raw)
}

func (c *Client) execRaw(ctx context.Context, payload map[string]any) (json.RawMessage, error) {
	attempts := 1
	if isReadOp(payload) {
		attempts = 3
	}
	var lastError error
	for attempt := 0; attempt < attempts; attempt++ {
		raw, err := c.execOnce(ctx, payload)
		if err == nil {
			return raw, nil
		}
		var apiError *APIError
		if errors.As(err, &apiError) || attempt+1 >= attempts {
			return nil, err
		}
		lastError = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(10*(attempt+1)) * time.Millisecond):
		}
	}
	return nil, lastError
}

func (c *Client) execOnce(ctx context.Context, payload map[string]any) (json.RawMessage, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.baseURL+"/v1/exec",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	request.Header.Set("content-type", "application/json")
	if c.token != "" {
		request.Header.Set("authorization", "Bearer "+c.token)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
	if err != nil {
		return nil, err
	}

	var envelope struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
		Error  *APIError       `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("nonodb: invalid response (HTTP %d): %.200s", response.StatusCode, raw)
	}
	if !envelope.OK || response.StatusCode < 200 || response.StatusCode >= 300 {
		if envelope.Error == nil {
			envelope.Error = &APIError{
				Code:    "http_error",
				Message: http.StatusText(response.StatusCode),
			}
		}
		envelope.Error.StatusCode = response.StatusCode
		return nil, envelope.Error
	}
	return envelope.Result, nil
}

func isReadOp(payload map[string]any) bool {
	op, _ := payload["op"].(string)
	switch op {
	case "find", "find_one", "count", "list_indexes", "list_collections",
		"list_text_indexes", "list_fuzzy_indexes", "search", "fuzzy",
		"history", "get_version", "batch":
		return true
	default:
		return false
	}
}

func decodeAny(raw json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeInto(raw json.RawMessage, out any) error {
	return json.Unmarshal(raw, out)
}

// Collections lists collection names.
func (c *Client) Collections(ctx context.Context) ([]string, error) {
	raw, err := c.execRaw(ctx, map[string]any{"op": "list_collections"})
	if err != nil {
		return nil, err
	}
	var result struct {
		Collections []string `json:"collections"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return nil, err
	}
	return result.Collections, nil
}

// Batch executes read-only operations in one LMDB transaction.
func (c *Client) Batch(ctx context.Context, ops []map[string]any) ([]BatchEntry, error) {
	return c.batchWithOp(ctx, "batch", ops)
}

// WriteBatch executes writes atomically: all operations commit or none do.
func (c *Client) WriteBatch(ctx context.Context, ops []map[string]any) ([]BatchEntry, error) {
	return c.batchWithOp(ctx, "write_batch", ops)
}

func (c *Client) batchWithOp(
	ctx context.Context,
	op string,
	ops []map[string]any,
) ([]BatchEntry, error) {
	raw, err := c.execRaw(ctx, map[string]any{"op": op, "ops": ops})
	if err != nil {
		return nil, err
	}
	var result struct {
		Results []BatchEntry `json:"results"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return nil, err
	}
	return result.Results, nil
}

// Collection returns a handle for one collection.
func (c *Client) Collection(name string) *Collection {
	return &Collection{client: c, Name: name}
}

// Collection is a handle for one collection.
type Collection struct {
	client *Client
	Name   string
}

// Insert inserts a document and returns its _id.
func (c *Collection) Insert(ctx context.Context, doc any) (string, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "insert", "coll": c.Name, "doc": doc,
	})
	if err != nil {
		return "", err
	}
	var result struct {
		ID string `json:"inserted_id"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return "", err
	}
	return result.ID, nil
}

// InsertMany inserts documents and returns their _id values.
func (c *Collection) InsertMany(ctx context.Context, docs []any) ([]string, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "insert_many", "coll": c.Name, "docs": docs,
	})
	if err != nil {
		return nil, err
	}
	var result struct {
		IDs []string `json:"inserted_ids"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return nil, err
	}
	return result.IDs, nil
}

// Update updates documents and returns the number modified.
func (c *Collection) Update(
	ctx context.Context,
	filter, update map[string]any,
	options UpdateOptions,
) (int64, error) {
	result, err := c.UpdateResult(ctx, filter, update, options)
	if err != nil {
		return 0, err
	}
	return result.Modified, nil
}

// UpdateResult updates documents and returns match/upsert details.
func (c *Collection) UpdateResult(
	ctx context.Context,
	filter, update map[string]any,
	options UpdateOptions,
) (UpdateResult, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "update", "coll": c.Name, "filter": filter, "update": update,
		"multi": options.Multi, "upsert": options.Upsert,
	})
	if err != nil {
		return UpdateResult{}, err
	}
	var result UpdateResult
	if err := decodeInto(raw, &result); err != nil {
		return UpdateResult{}, err
	}
	return result, nil
}

// Delete deletes documents and returns the number deleted.
func (c *Collection) Delete(ctx context.Context, filter map[string]any, multi bool) (int64, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "delete", "coll": c.Name, "filter": filter, "multi": multi,
	})
	if err != nil {
		return 0, err
	}
	var result struct {
		Deleted int64 `json:"deleted"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return 0, err
	}
	return result.Deleted, nil
}

// WriteBatch atomically writes several operations in this collection.
func (c *Collection) WriteBatch(ctx context.Context, ops []map[string]any) ([]BatchEntry, error) {
	withColl := make([]map[string]any, len(ops))
	for index, op := range ops {
		copyOp := make(map[string]any, len(op)+1)
		for key, value := range op {
			copyOp[key] = value
		}
		if _, ok := copyOp["coll"]; !ok {
			copyOp["coll"] = c.Name
		}
		withColl[index] = copyOp
	}
	return c.client.WriteBatch(ctx, withColl)
}

// CreateIndex creates a single-field or compound index.
func (c *Collection) CreateIndex(
	ctx context.Context,
	fields []string,
	options IndexOptions,
) (bool, error) {
	include := options.Include
	if include == nil {
		include = []string{}
	}
	_, err := c.client.execRaw(ctx, map[string]any{
		"op": "create_index", "coll": c.Name, "fields": fields,
		"unique": options.Unique, "include": include,
	})
	return err == nil, err
}

// CreateFieldIndex creates a single-field index.
func (c *Collection) CreateFieldIndex(
	ctx context.Context,
	field string,
	unique bool,
) (bool, error) {
	return c.CreateIndex(ctx, []string{field}, IndexOptions{Unique: unique})
}

// DropIndex drops an index by its name (`field` or `a+b`).
func (c *Collection) DropIndex(ctx context.Context, name string) (bool, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "drop_index", "coll": c.Name, "field": name,
	})
	if err != nil {
		return false, err
	}
	var result struct {
		Dropped bool `json:"dropped"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return false, err
	}
	return result.Dropped, nil
}

// ListIndexes lists secondary index definitions.
func (c *Collection) ListIndexes(ctx context.Context) ([]map[string]any, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "list_indexes", "coll": c.Name,
	})
	if err != nil {
		return nil, err
	}
	var result struct {
		Indexes []map[string]any `json:"indexes"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return nil, err
	}
	return result.Indexes, nil
}

// GetMany fetches many documents by _id in one read batch.
func (c *Collection) GetMany(ctx context.Context, ids []string) ([]map[string]any, error) {
	ops := make([]map[string]any, len(ids))
	for index, id := range ids {
		ops[index] = map[string]any{
			"op": "find_one", "coll": c.Name, "filter": map[string]any{"_id": id},
		}
	}
	entries, err := c.client.Batch(ctx, ops)
	if err != nil {
		return nil, err
	}
	documents := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		if !entry.OK || len(entry.Result) == 0 || string(entry.Result) == "null" {
			continue
		}
		var document map[string]any
		if err := decodeInto(entry.Result, &document); err != nil {
			return nil, err
		}
		documents = append(documents, document)
	}
	return documents, nil
}

// History lists document revisions, newest first.
func (c *Collection) History(
	ctx context.Context,
	id string,
	limit int,
	includeDocs bool,
) ([]map[string]any, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "history", "coll": c.Name, "id": id,
		"limit": limit, "include_docs": includeDocs,
	})
	if err != nil {
		return nil, err
	}
	var result struct {
		Revisions []map[string]any `json:"revisions"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return nil, err
	}
	return result.Revisions, nil
}

// GetVersion reads one exact document revision. It returns nil when the
// revision does not exist.
func (c *Collection) GetVersion(
	ctx context.Context,
	id string,
	version uint64,
) (map[string]any, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "get_version", "coll": c.Name, "id": id, "version": version,
	})
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" {
		return nil, nil
	}
	var result map[string]any
	if err := decodeInto(raw, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// CreateTextIndex creates a full-text index.
func (c *Collection) CreateTextIndex(ctx context.Context, field string) (bool, error) {
	_, err := c.client.execRaw(ctx, map[string]any{
		"op": "create_text_index", "coll": c.Name, "field": field,
	})
	return err == nil, err
}

// DropTextIndex drops a full-text index.
func (c *Collection) DropTextIndex(ctx context.Context, field string) (bool, error) {
	return c.dropFieldIndex(ctx, "drop_text_index", field)
}

// ListTextIndexes lists full-text index fields.
func (c *Collection) ListTextIndexes(ctx context.Context) ([]string, error) {
	return c.listFieldIndexes(ctx, "list_text_indexes", "text_indexes")
}

// CreateFuzzyIndex creates a fuzzy index.
func (c *Collection) CreateFuzzyIndex(ctx context.Context, field string) (bool, error) {
	_, err := c.client.execRaw(ctx, map[string]any{
		"op": "create_fuzzy_index", "coll": c.Name, "field": field,
	})
	return err == nil, err
}

// DropFuzzyIndex drops a fuzzy index.
func (c *Collection) DropFuzzyIndex(ctx context.Context, field string) (bool, error) {
	return c.dropFieldIndex(ctx, "drop_fuzzy_index", field)
}

// ListFuzzyIndexes lists fuzzy index fields.
func (c *Collection) ListFuzzyIndexes(ctx context.Context) ([]string, error) {
	return c.listFieldIndexes(ctx, "list_fuzzy_indexes", "fuzzy_indexes")
}

func (c *Collection) dropFieldIndex(
	ctx context.Context,
	op, field string,
) (bool, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": op, "coll": c.Name, "field": field,
	})
	if err != nil {
		return false, err
	}
	var result struct {
		Dropped bool `json:"dropped"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return false, err
	}
	return result.Dropped, nil
}

func (c *Collection) listFieldIndexes(
	ctx context.Context,
	op, key string,
) ([]string, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{"op": op, "coll": c.Name})
	if err != nil {
		return nil, err
	}
	var result map[string][]string
	if err := decodeInto(raw, &result); err != nil {
		return nil, err
	}
	return result[key], nil
}

// Search runs BM25 full-text search.
func (c *Collection) Search(
	ctx context.Context,
	field, query string,
	options SearchOptions,
) ([]map[string]any, error) {
	if options.Limit == 0 {
		options.Limit = 10
	}
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "search", "coll": c.Name, "field": field, "query": query,
		"filter": nonNilMap(options.Filter), "skip": options.Skip, "limit": options.Limit,
	})
	if err != nil {
		return nil, err
	}
	return decodeResults(raw)
}

// Fuzzy runs fuzzy search.
func (c *Collection) Fuzzy(
	ctx context.Context,
	field, query string,
	options FuzzyOptions,
) ([]map[string]any, error) {
	if options.MaxDistance == 0 {
		options.MaxDistance = 2
	}
	if options.Limit == 0 {
		options.Limit = 10
	}
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "fuzzy", "coll": c.Name, "field": field, "query": query,
		"max_distance": options.MaxDistance, "filter": nonNilMap(options.Filter),
		"limit": options.Limit,
	})
	if err != nil {
		return nil, err
	}
	return decodeResults(raw)
}

// Find runs a find query.
func (c *Collection) Find(ctx context.Context, query FindQuery) ([]map[string]any, error) {
	payload := map[string]any{
		"op": "find", "coll": c.Name,
		"filter": nonNilMap(query.Filter), "skip": query.Skip,
	}
	if query.Projection != nil {
		payload["projection"] = query.Projection
	}
	if len(query.Sort) > 0 {
		sort := make([][2]any, len(query.Sort))
		for index, field := range query.Sort {
			sort[index] = [2]any{field.Field, field.Direction}
		}
		payload["sort"] = sort
	}
	if query.Limit != nil {
		payload["limit"] = *query.Limit
	}
	raw, err := c.client.execRaw(ctx, payload)
	if err != nil {
		return nil, err
	}
	return decodeDocuments(raw)
}

// FindOne returns one document or nil.
func (c *Collection) FindOne(
	ctx context.Context,
	filter, projection map[string]any,
) (map[string]any, error) {
	payload := map[string]any{
		"op": "find_one", "coll": c.Name, "filter": nonNilMap(filter),
	}
	if projection != nil {
		payload["projection"] = projection
	}
	raw, err := c.client.execRaw(ctx, payload)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var document map[string]any
	if err := decodeInto(raw, &document); err != nil {
		return nil, err
	}
	return document, nil
}

// Count counts documents matching a filter.
func (c *Collection) Count(ctx context.Context, filter map[string]any) (int64, error) {
	raw, err := c.client.execRaw(ctx, map[string]any{
		"op": "count", "coll": c.Name, "filter": nonNilMap(filter),
	})
	if err != nil {
		return 0, err
	}
	var result struct {
		Count int64 `json:"count"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return 0, err
	}
	return result.Count, nil
}

func decodeDocuments(raw json.RawMessage) ([]map[string]any, error) {
	var documents []map[string]any
	if err := decodeInto(raw, &documents); err != nil {
		return nil, err
	}
	return documents, nil
}

func decodeResults(raw json.RawMessage) ([]map[string]any, error) {
	var result struct {
		Results []map[string]any `json:"results"`
	}
	if err := decodeInto(raw, &result); err != nil {
		return nil, err
	}
	return result.Results, nil
}

func nonNilMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}
