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
	"github.com/google/uuid"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	// DefaultHTTPGetAddress Default Address

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
	sk := tm.Format("2006-01-02 T 15:04")
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

func GetTask(taskId string) (map[string]types.AttributeValue, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	svc := dynamodb.NewFromConfig(cfg)
	primaryKey := strings.Split(taskId, "|")[0]
	out, err := svc.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String("task-table"),
		Key: map[string]types.AttributeValue{
			"MinuteTime":     &types.AttributeValueMemberS{Value: primaryKey},
			"TaskIdentifier": &types.AttributeValueMemberS{Value: taskId},
		},
	})
	if err != nil {
		return nil, err
	}
	return out.Item, nil
}

func handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	switch request.Path {
	case "/api/task":
		if request.HTTPMethod != "POST" {
			return events.APIGatewayProxyResponse{
				Body:       fmt.Sprintf("method not allowed"),
				StatusCode: 405,
			}, nil
		}
		var tq TaskReq
		err := json.Unmarshal([]byte(request.Body), &tq)
		if err != nil {
			log.Println(err)
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusBadRequest,
				Body:       fmt.Sprintf("Error parsing body of the request"),
			}, err
		}
		if tq.Url == "" || tq.Time == "" || tq.Body == "" || tq.Email == "" {

			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusBadRequest,
				Body:       fmt.Sprintf("you are missing the required fields for the request"),
			}, errors.New("you are missing the required fields")
		}
		taskId, err := AddTask(&tq)
		if err != nil {
			log.Println(err)
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusInternalServerError,
				Body:       fmt.Sprintf(err.Error()),
			}, err
		}

		fmt.Printf("Task created %s \n", taskId)
		return events.APIGatewayProxyResponse{
			Body:       fmt.Sprintf("Task has been succesfully registered with id %s", taskId),
			StatusCode: 200,
		}, nil
	case "/task/status":
		parameters := request.QueryStringParameters
		taskId, ok := parameters["id"]
		if !ok {
			return events.APIGatewayProxyResponse{
				Body:       fmt.Sprintf("task id is not present"),
				StatusCode: http.StatusBadRequest,
			}, nil
		}
		task, err := GetTask(taskId)
		if err != nil {
			return events.APIGatewayProxyResponse{
				Body:       err.Error(),
				StatusCode: http.StatusBadRequest,
			}, err
		}
		marshal, err := json.Marshal(task)
		if err != nil {

			return events.APIGatewayProxyResponse{
				Body:       err.Error(),
				StatusCode: http.StatusBadRequest,
			}, err
		}
		return events.APIGatewayProxyResponse{
			Body:       string(marshal),
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
