package main

import (
	"context"
	"fmt"
	"log"
	"time"

	nonodb "github.com/essmos/nonodb/client/go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := nonodb.New()
	if !db.Health(ctx) {
		log.Fatal("daemon is not healthy")
	}

	name := fmt.Sprintf("go_client_test_%d", time.Now().UnixNano())
	users := db.Collection(name)

	id, err := users.Insert(ctx, map[string]any{
		"name": "Ada", "age": 36, "tags": []string{"db", "go"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("inserted", id)

	ids, err := users.InsertMany(ctx, []any{
		map[string]any{"name": "Grace", "age": 41},
		map[string]any{"name": "Linus", "age": 55},
	})
	if err != nil || len(ids) != 2 {
		log.Fatalf("insert_many: %v %v", ids, err)
	}

	limit := int64(2)
	adults, err := users.Find(ctx, nonodb.FindQuery{
		Filter: map[string]any{"age": map[string]any{"$gte": 40}},
		Sort:   []nonodb.SortField{{Field: "age", Direction: -1}},
		Limit:  &limit,
	})
	if err != nil || len(adults) != 2 || adults[0]["name"] != "Linus" {
		log.Fatalf("find: %v %v", adults, err)
	}

	update, err := users.UpdateResult(ctx,
		map[string]any{"name": "Grace"},
		map[string]any{"$set": map[string]any{"active": true}},
		nonodb.UpdateOptions{Upsert: true},
	)
	if err != nil || update.Modified != 1 {
		log.Fatalf("update: %+v %v", update, err)
	}

	if _, err := users.CreateIndex(ctx, []string{"age", "name"},
		nonodb.IndexOptions{Include: []string{"tags"}}); err != nil {
		log.Fatal(err)
	}
	if _, err := users.DropIndex(ctx, "age+name"); err != nil {
		log.Fatal(err)
	}

	if _, err := users.CreateTextIndex(ctx, "name"); err != nil {
		log.Fatal(err)
	}
	if _, err := users.CreateFuzzyIndex(ctx, "name"); err != nil {
		log.Fatal(err)
	}
	found, err := users.Search(ctx, "name", "grace", nonodb.SearchOptions{Limit: 5})
	if err != nil || len(found) != 1 {
		log.Fatalf("search: %v %v", found, err)
	}
	fuzzy, err := users.Fuzzy(ctx, "name", "grce", nonodb.FuzzyOptions{Limit: 5})
	if err != nil || len(fuzzy) < 1 {
		log.Fatalf("fuzzy: %v %v", fuzzy, err)
	}

	entries, err := users.WriteBatch(ctx, []map[string]any{
		{"op": "insert", "doc": map[string]any{"name": "Atomic"}},
		{"op": "delete", "filter": map[string]any{"name": "Atomic"}},
	})
	if err != nil || len(entries) != 2 {
		log.Fatalf("write_batch: %v %v", entries, err)
	}

	if _, err := users.GetMany(ctx, []string{id}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("go client smoke test: ok")
}
