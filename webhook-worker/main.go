package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func HandleTask(message string) error {
	var reqFields map[string]string
	err := json.Unmarshal([]byte(message), &reqFields)
	if err != nil {
		return err
	}
	url, ok := reqFields["URL"]
	if !ok || url == "" {
		return errors.New("could not find the correct url")
	}
	body, ok := reqFields["Body"]
	//if !ok || body=="" {
	//	return errors.New("url body no present")
	//}
	pk, ok := reqFields["MinuteTime"]
	if !ok || pk == "" {
		return errors.New("could not find the correct partition key")
	}
	sk, ok := reqFields["TaskIdentifier"]
	if !ok || sk == "" {
		return errors.New("could not find the correct sort key")
	}
	responseBody := bytes.NewBuffer([]byte(body))
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("did not receive correct response")
	}
	return nil
}

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	for _, message := range sqsEvent.Records {

		fmt.Printf("The message %s for event source %s = %s \n", message.MessageId, message.EventSource, message.Body)
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
