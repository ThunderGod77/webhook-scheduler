package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"strconv"
	"time"
)

var (
	// DefaultHTTPGetAddress Default Address
	DefaultHTTPGetAddress = "https://checkip.amazonaws.com"

	// ErrNoIP No IP found in response
	ErrNoIP = errors.New("No IP in HTTP response")

	// ErrNon200Response non 200 status code in response
	ErrNon200Response = errors.New("Non 200 Response found")
)

type TaskReq struct {
	Email string `json:"email"`
	Body  string `json:"body"`
	Url   string `json:"url"`
	Time  string `json:"time"`
}

func GetPartitionKey(tm time.Time) string {
	//loc, _ := time.LoadLocation("Asia/Calcutta")
	//now := time.Now().In(loc)
	pk := tm.Format("2006-01-02 T 15:04")
	return pk
}

func GetSortKey(tm time.Time) (string, string) {
	sk := tm.String()
	taskId := uuid.New().String()
	return sk + "|" + taskId, taskId

}

func AddTask(tq *TaskReq) (string, error) {
	t, err := strconv.ParseInt(tq.Time, 10, 64)
	if err != nil {
		return "", err
	}
	loc, _ := time.LoadLocation("Asia/Calcutta")
	tm := time.Unix(t, 0).In(loc)
	pkKey := GetPartitionKey(tm)
	sortKey, taskId := GetSortKey(tm)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return "", err
	}
	svc := dynamodb.NewFromConfig(cfg)

	out, err := svc.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("task-table"),
		Item: map[string]types.AttributeValue{
			"MinuteTime":     &types.AttributeValueMemberS{Value: pkKey},
			"TaskIdentifier": &types.AttributeValueMemberS{Value: sortKey},
			"TaskId":         &types.AttributeValueMemberS{Value: taskId},
			"UserID":         &types.AttributeValueMemberS{Value: tq.Email},
			"Body":           &types.AttributeValueMemberS{Value: tq.Body},
			"URL":            &types.AttributeValueMemberS{Value: tq.Url},
		},
	})
	if err != nil {
		return "", err
	}
	fmt.Println(out.ConsumedCapacity)
	return taskId, nil

}

func handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	switch request.Path {
	case "/api/task":
		return events.APIGatewayProxyResponse{
			Body:       fmt.Sprintf("Hello, %v", string("This is the task path")),
			StatusCode: 200,
		}, nil

	default:
		fmt.Println(request.Path)
		fmt.Println(request.Resource)
		fmt.Println(request.Headers)
		return events.APIGatewayProxyResponse{
			Body:       fmt.Sprintf("Hello, %v", string("this id the default response")),
			StatusCode: 200,
		}, nil
	}

}

func main() {
	lambda.Start(handler)
}
