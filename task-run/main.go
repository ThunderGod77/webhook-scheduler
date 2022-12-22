package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"log"
	"os"
	"time"
)

type SQSSendMessageAPI interface {
	GetQueueUrl(ctx context.Context,
		params *sqs.GetQueueUrlInput,
		optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)

	SendMessage(ctx context.Context,
		params *sqs.SendMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

func GetPartitionKey(tm time.Time) string {
	loc, _ := time.LoadLocation("Asia/Calcutta")
	//now := time.Now().In(loc)
	tm = tm.In(loc)
	pk := tm.Format("2006-01-02 T 15:04")
	return pk
}
func GetQueueURL(c context.Context, api SQSSendMessageAPI, input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	return api.GetQueueUrl(c, input)
}
func SendMsg(c context.Context, api SQSSendMessageAPI, input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	return api.SendMessage(c, input)
}

func AddTaskToQueue(tasks []map[string]types.AttributeValue) error {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-south-1"))
	if err != nil {
		panic("configuration error, " + err.Error())
	}
	client := sqs.NewFromConfig(cfg)
	queue := os.Getenv("Queue")
	if queue == "" {
		return errors.New("could not get queue name")
	}
	fmt.Println(queue == "beta-1-MySqsQueue-IHw1uAJJCu8p")
	//queue = "beta-1-MySqsQueue-IHw1uAJJCu8p"
	gQInput := &sqs.GetQueueUrlInput{
		QueueName: &queue,
	}

	result, err := GetQueueURL(context.TODO(), client, gQInput)
	if err != nil {

		return err
	}
	fmt.Println(*result.QueueUrl)
	queueURL := result.QueueUrl
	for _, task := range tasks {
		marshal, err := json.Marshal(task)
		if err != nil {
			return err
		}
		sMInput := &sqs.SendMessageInput{
			DelaySeconds: 10,
			MessageBody:  aws.String(string(marshal)),
			QueueUrl:     queueURL,
		}
		resp, err := SendMsg(context.TODO(), client, sMInput)
		if err != nil {
			return err
		}
		fmt.Println("Sent message with ID: " + *resp.MessageId)
	}

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
	err = AddTaskToQueue(out.Items)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil

}

func main() {
	lambda.Start(handler)
}
