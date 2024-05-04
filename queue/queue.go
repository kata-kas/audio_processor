package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	messagebus "github.com/acme/audiopub/messagebus"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type SQSMessage struct {
	Type          string   `json:"type"`
	RvFid         string   `json:"rv_fid"`
	Certainty     int      `json:"certainty"`
	History       []string `json:"history"`
	Label         string   `json:"label"`
	Organization  string   `json:"organization"`
	DeviceID      string   `json:"device_id"`
	Categories    string   `json:"categories"`
	Timestamp     int      `json:"timestamp"`
	Transcription string   `json:"transcription"`
	RoomID        string   `json:"room_id"`
}

type Subscriber struct {
	sqsClient    *sqs.Client
	snsClient    *sns.Client
	queueURL     string
	topicARN     string
	subscription *string
	mb           *messagebus.MessageBus
}

func NewSubscriber(cfg aws.Config, mb *messagebus.MessageBus, topicARN string) (*Subscriber, error) {
	sqsClient := sqs.NewFromConfig(cfg)
	snsClient := sns.NewFromConfig(cfg)
	queueName := uuid.New().String()

	ctx := context.TODO()
	createQueueOutput, subscribeOutput, err := subscribeQueueToTopic(
		ctx, snsClient, topicARN, sqsClient, queueName)
	if err != nil {
		return nil, fmt.Errorf("error subscribing queue to topic: %w", err)
	}

	return &Subscriber{
		sqsClient:    sqsClient,
		snsClient:    snsClient,
		queueURL:     *createQueueOutput.QueueUrl,
		topicARN:     topicARN,
		subscription: subscribeOutput.SubscriptionArn,
		mb:           mb,
	}, nil
}

func (s *Subscriber) StartListening() {
	for {
		receiveMessage := &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(s.queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		}
		result, err := s.sqsClient.ReceiveMessage(context.TODO(), receiveMessage)
		if err != nil {
			log.Printf("error receiving message on queue %s: %v", s.queueURL, err)
			time.Sleep(time.Second)
			continue
		}

		for _, message := range result.Messages {
			var sqsMessage SQSMessage
			if err := json.Unmarshal([]byte(*message.Body), &sqsMessage); err != nil {
				log.Printf("error unmarshaling sqs message: %v", err)
				continue
			}

			deleteMessage := &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(s.queueURL),
				ReceiptHandle: message.ReceiptHandle,
			}
			if _, err := s.sqsClient.DeleteMessage(context.TODO(), deleteMessage); err != nil {
				log.Printf("error deleting message: %v", err)
				continue
			}

			s.mb.Publish(messagebus.Message{
				DeviceID: sqsMessage.DeviceID,
				Data: messagebus.AudioResponseData{
					Type:         sqsMessage.Type,
					RVFid:        sqsMessage.RvFid,
					Certainty:    sqsMessage.Certainty,
					History:      sqsMessage.History,
					Label:        sqsMessage.Label,
					Organization: sqsMessage.Organization,
					Categories:   sqsMessage.Categories,
				},
			})
		}
	}
}

func (s *Subscriber) DeleteQueueAndSubscription() error {
	_, err := s.sqsClient.DeleteQueue(context.TODO(), &sqs.DeleteQueueInput{
		QueueUrl: aws.String(s.queueURL),
	})
	if err != nil {
		return err
	}

	_, err = s.snsClient.Unsubscribe(context.TODO(), &sns.UnsubscribeInput{
		SubscriptionArn: s.subscription,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Deleted queue %q and subscription %q\n", s.queueURL, *s.subscription)

	return nil
}

func subscribeQueueToTopic(
	ctx context.Context,
	snsClient *sns.Client, topicARN string,
	sqsClient *sqs.Client, queueName string,
) (*sqs.CreateQueueOutput, *sns.SubscribeOutput, error) {
	createQueueOutput, err := sqsClient.CreateQueue(ctx,
		&sqs.CreateQueueInput{
			QueueName: aws.String(queueName),
			Attributes: map[string]string{
				"Policy": queuePolicySNSToSQS(topicARN),
			},
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating queue %q: %w", queueName, err)
	}
	timer := time.NewTimer(time.Second)
	select {
	case <-ctx.Done():
		return nil, nil, fmt.Errorf("waiting after creating SQS queue %q: %w",
			queueName, ctx.Err())
	case <-timer.C:
	}

	queueAttributes, err := sqsClient.GetQueueAttributes(ctx,
		&sqs.GetQueueAttributesInput{
			QueueUrl: createQueueOutput.QueueUrl,
			AttributeNames: []sqstypes.QueueAttributeName{
				sqstypes.QueueAttributeNameQueueArn,
			},
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("getting attributes for SQS queue %q: %w",
			queueName, err)
	}
	queueARNKey := string(sqstypes.QueueAttributeNameQueueArn)
	queueARN := queueAttributes.Attributes[queueARNKey]
	if queueARN == "" {
		return nil, nil, fmt.Errorf("SQS queue %q has empty ARN", queueName)
	}
	subscribeOutput, err := snsClient.Subscribe(ctx,
		&sns.SubscribeInput{
			Attributes: map[string]string{
				"RawMessageDelivery": "true",
			},
			Endpoint:              &queueARN,
			Protocol:              aws.String("sqs"),
			ReturnSubscriptionArn: true,
			TopicArn:              &topicARN,
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"subscribing SQS queue %q to SNS topic %q: %w",
			queueName, topicARN, err)
	}

	fmt.Printf("Subscribed queue %q to topic %q\n", queueName, topicARN)
	return createQueueOutput, subscribeOutput, nil
}

func queuePolicySNSToSQS(topicARN string) string {
	var buf strings.Builder
	err := json.NewEncoder(&buf).Encode(
		map[string]any{
			"Version": "2012-10-17",
			"Statement": map[string]any{
				"Sid":       "SNSTopicSendMessage",
				"Effect":    "Allow",
				"Principal": "*",
				"Action":    "sqs:SendMessage",
				"Resource":  "*",
				"Condition": map[string]any{
					"ArnEquals": map[string]any{
						"aws:SourceArn": topicARN,
					},
				},
			},
		},
	)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
