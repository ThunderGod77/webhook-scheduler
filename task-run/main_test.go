package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"testing"
)

func TestHandler(t *testing.T) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		t.Error(err.Error())
	}
	client := sqs.NewFromConfig(cfg)
	queue := "beta-1-MySqsQueue-IHw1uAJJCu8p"

	gQInput := &sqs.GetQueueUrlInput{
		QueueName: &queue,
	}
	result, err := GetQueueURL(context.TODO(), client, gQInput)
	if err != nil {
		t.Error(err)
	}
	t.Log(*result.QueueUrl)
}
