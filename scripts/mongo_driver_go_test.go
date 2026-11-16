package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func check(condition bool, message string) {
	if !condition {
		panic(message)
	}
}

func main() {
	url := os.Getenv("NONODB_MONGO_URL")
	if url == "" {
		url = "mongodb://127.0.0.1:37017/?directConnection=true"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		panic(err)
	}
	databaseName := fmt.Sprintf("go_driver_%d", time.Now().UnixNano())
	db := client.Database(databaseName)
	defer func() {
		_ = db.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	}()

	if err := client.Ping(ctx, nil); err != nil {
		panic(err)
	}
	items := db.Collection("items")
	objectID := primitive.NewObjectIDFromTimestamp(time.Unix(0, 0))
	decimal, err := primitive.ParseDecimal128("1.50")
	if err != nil {
		panic(err)
	}
	_, err = items.InsertMany(ctx, []any{
		bson.M{
			"_id":    "a",
			"n":      1,
			"tag":    "x",
			"nested": bson.M{"v": true},
			"arr":    []int{1, 2, 3},
			"when":   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"dec":    decimal,
			"oid":    objectID,
		},
		bson.M{"_id": "b", "n": 2, "tag": "y", "arr": []int{2, 3, 4}},
		bson.M{"_id": "c", "n": 3, "tag": "x", "arr": []int{3, 4, 5}},
	})
	if err != nil {
		panic(err)
	}

	cursor, err := items.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		panic(err)
	}
	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		panic(err)
	}
	check(len(docs) == 3, "find all count")
	decimalText := docs[0]["dec"].(primitive.Decimal128).String()
	check(decimalText == "1.50", "Decimal128 round-trip: "+decimalText)
	check(docs[0]["oid"].(primitive.ObjectID) == objectID, "ObjectId round-trip")
	check(docs[0]["when"].(primitive.DateTime).Time().Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)), "Date round-trip")

	cursor, err = items.Find(
		ctx,
		bson.M{"n": bson.M{"$gte": 2}, "arr": bson.M{"$all": bson.A{3}}},
		options.Find().SetSort(bson.D{{Key: "n", Value: -1}}),
	)
	if err != nil {
		panic(err)
	}
	docs = nil
	if err := cursor.All(ctx, &docs); err != nil {
		panic(err)
	}
	check(len(docs) == 2 && docs[0]["_id"] == "c" && docs[1]["_id"] == "b", "filter and sort")

	result, err := items.UpdateOne(
		ctx,
		bson.M{"_id": "a"},
		bson.M{"$set": bson.M{"status": "new"}, "$inc": bson.M{"n": 10}},
	)
	if err != nil {
		panic(err)
	}
	check(result.MatchedCount == 1 && result.ModifiedCount == 1, "update result")
	var updated bson.M
	if err := items.FindOne(ctx, bson.M{"_id": "a"}).Decode(&updated); err != nil {
		panic(err)
	}
	check(updated["n"].(int32) == 11, "update value")

	cursor, err = items.Aggregate(ctx, []bson.M{
		{"$group": bson.M{"_id": "$tag", "total": bson.M{"$sum": "$n"}}},
		{"$sort": bson.M{"_id": 1}},
	}, options.Aggregate().SetBatchSize(1))
	if err != nil {
		panic(err)
	}
	docs = nil
	if err := cursor.All(ctx, &docs); err != nil {
		panic(err)
	}
	check(len(docs) == 2, "aggregate group count")
	check(docs[0]["_id"] == "x" && docs[0]["total"].(int32) == 14, "aggregate group value")
	check(docs[1]["_id"] == "y" && docs[1]["total"].(int32) == 2, "aggregate group value")

	cursor, err = items.Aggregate(ctx, []bson.M{
		{"$match": bson.M{"tag": "x"}},
		{"$project": bson.M{"_id": 1, "n": 1, "double": bson.M{"$multiply": bson.A{"$n", 2}}}},
	}, options.Aggregate().SetBatchSize(1))
	if err != nil {
		panic(err)
	}
	docs = nil
	if err := cursor.All(ctx, &docs); err != nil {
		panic(err)
	}
	check(len(docs) == 2, "streamed aggregate count")
	check(docs[0]["double"].(int32) == 22, "streamed aggregate value")

	tags, err := items.Distinct(ctx, "tag", bson.M{})
	if err != nil {
		panic(err)
	}
	values := make([]string, 0, len(tags))
	for _, tag := range tags {
		values = append(values, tag.(string))
	}
	sort.Strings(values)
	check(len(values) == 2 && values[0] == "x" && values[1] == "y", "distinct")

	count, err := items.CountDocuments(ctx, bson.M{"tag": "x"})
	if err != nil {
		panic(err)
	}
	check(count == 2, "count")

	name, err := items.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "tag", Value: 1}, {Key: "n", Value: -1}},
	})
	if err != nil {
		panic(err)
	}
	check(name != "", "create index")
	indexCursor, err := items.Indexes().List(ctx)
	if err != nil {
		panic(err)
	}
	var indexes []bson.M
	if err := indexCursor.All(ctx, &indexes); err != nil {
		panic(err)
	}
	found := false
	for _, index := range indexes {
		if key, ok := index["key"].(bson.M); ok && len(key) == 2 {
			if _, ok := key["tag"]; ok {
				if _, ok := key["n"]; ok {
					found = true
				}
			}
		}
	}
	check(found, "list indexes")

	deleted, err := items.DeleteMany(ctx, bson.M{"n": bson.M{"$gte": 11}})
	if err != nil {
		panic(err)
	}
	check(deleted.DeletedCount == 1, "deleteMany")

	upserts := db.Collection("upserts")
	upsertResult, err := upserts.UpdateOne(
		ctx,
		bson.M{"key": "generated"},
		bson.M{"$set": bson.M{"n": 1}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		panic(err)
	}
	upsertedID, ok := upsertResult.UpsertedID.(primitive.ObjectID)
	check(ok, "upsert ObjectId")
	var upsertedDoc bson.M
	if err := upserts.FindOne(ctx, bson.M{"key": "generated"}).Decode(&upsertedDoc); err != nil {
		panic(err)
	}
	check(upsertedDoc["_id"].(primitive.ObjectID) == upsertedID, "upsert document id")

	fmt.Println("go mongodb driver test: ok")
}
