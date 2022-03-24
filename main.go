package main

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	SOURCECONNECTIONSTRING = "mongodb://localhost:27018"
	SOURCEDB               = "prod"
	DESTCONNECTIONSTRING   = "mongodb://localhost:27017"
	DESTDB                 = "dev"
)

func GetMongoClient(connectionString string) (*mongo.Client, error) {
	//Perform connection creation operation only once.
	clientOptions := options.Client().ApplyURI(connectionString)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		panic(err)
	}
	return client, nil
}

func GetMongoCollection(client *mongo.Client, databaseName string, collectionName string) *mongo.Collection {
	return client.Database(databaseName).Collection(collectionName)
}

func GetRandomItemsFromCollection(collection *mongo.Collection, numberOfItemsToGet int) ([]interface{}, error) {
	pipeline := []bson.D{bson.D{{"$sample", bson.D{{"size", numberOfItemsToGet}}}}}
	cur, err := collection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		return nil, err
	}
	var results []interface{}
	for cur.Next(context.TODO()) {
		var document bson.D
		err := cur.Decode(&document)
		if err != nil {
			log.Println(err)
		}
		results = append(results, document)
	}
	return results, nil
}

func CreateCollection(client *mongo.Client, collectionName string) {
	db := client.Database(DESTDB)
	command := bson.D{{"create", collectionName}}
	var result bson.M
	if err := db.RunCommand(context.TODO(), command).Decode(&result); err != nil {
		log.Fatal(err)
	}
}

func CopyCollectionWithRandomData(sourceClient *mongo.Client, destClient *mongo.Client, collectionName string, numberOfItems int) {
	CreateCollection(destClient, collectionName)
	sourceCollection := GetMongoCollection(sourceClient, SOURCEDB, collectionName)
	destCollection := GetMongoCollection(destClient, DESTDB, collectionName)
	items, err := GetRandomItemsFromCollection(sourceCollection, numberOfItems)
	if err != nil {
		log.Println(err)
	}
	destCollection.InsertMany(context.TODO(), items)
}

func GetAllCollectionsNames(client *mongo.Client, db string) ([]string, error) {
	database := client.Database(db)

	// use a filter to only select capped collections
	return database.ListCollectionNames(
		context.TODO(),
		bson.D{{}})
}

func main() {
	sourceClient, err := GetMongoClient(SOURCECONNECTIONSTRING)
	if err != nil {
		panic(err)
	}
	destClient, destErr := GetMongoClient(DESTCONNECTIONSTRING)
	if destErr != nil {
		panic(destClient)
	}
	collections, _ := GetAllCollectionsNames(sourceClient, SOURCEDB)
	fmt.Println(collections)
	for _, collection := range collections {
		fmt.Println("Copie de la collection " + collection)
		CopyCollectionWithRandomData(sourceClient, destClient, collection, 10000)
		fmt.Println("Copie terminée.")
	}
}
