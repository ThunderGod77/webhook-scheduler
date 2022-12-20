package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"time"
)

func GetPartitionKey(tm time.Time) string {
	loc, _ := time.LoadLocation("Asia/Calcutta")
	//now := time.Now().In(loc)
	tm = tm.In(loc)
	pk := tm.Format("2006-01-02 T 15:04")
	return pk
}

func AddTask(tasks []map[string]types.AttributeValue) error {
	return nil
}

func handler(request events.CloudWatchEvent) error {
	pk := GetPartitionKey(request.Time)
	fmt.Println(pk)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Println(err)
		return err
	}
	svc := dynamodb.NewFromConfig(cfg)
	out, err := svc.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String("task-table"),
		KeyConditionExpression: aws.String("MinuteTime = :hashKey"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":hashKey": &types.AttributeValueMemberS{Value: pk},
		},
	})

	if err != nil {
		log.Println(err)
		return err
	}
	err = AddTask(out.Items)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil

}

func main() {
	lambda.Start(handler)
}
