package main

import (
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"time"
)

func GetPartitionKey() string {
	//loc, _ := time.LoadLocation("Asia/Calcutta")
	//now := time.Now().In(loc)
	loc, _ := time.LoadLocation("Asia/Calcutta")
	tm := time.Now().In(loc)
	pk := tm.Format("2006-01-02 T 15:04")
	return pk
}

func handler(request events.CloudWatchEvent) error {
	loc, _ := time.LoadLocation("Asia/Calcutta")
	fmt.Println(request.Time.In(loc))
	fmt.Println(GetPartitionKey())

	return nil

}

func main() {
	lambda.Start(handler)
}
