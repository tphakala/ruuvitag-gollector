// +build aws

package sqs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"hash/fnv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter"
	"github.com/niktheblak/ruuvitag-gollector/pkg/sensor"
)

type Config struct {
	QueueName       string
	QueueURL        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type sqsExporter struct {
	sess     *session.Session
	sqs      sqsiface.SQSAPI
	queueUrl string
}

func New(cfg Config) (exporter.Exporter, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(cfg.Region),
		Credentials: credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
	})
	if err != nil {
		return nil, err
	}
	sqs := awssqs.New(sess)
	var queueUrl string
	if cfg.QueueURL != "" {
		queueUrl = cfg.QueueURL
	} else {
		resp, err := sqs.GetQueueUrl(&awssqs.GetQueueUrlInput{
			QueueName: aws.String(cfg.QueueName),
		})
		if err != nil {
			return nil, err
		}
		queueUrl = *resp.QueueUrl
	}
	return &sqsExporter{
		sess:     sess,
		sqs:      sqs,
		queueUrl: queueUrl,
	}, nil
}

func (e *sqsExporter) Name() string {
	return "AWS SQS"
}

func (e *sqsExporter) Export(ctx context.Context, data ...sensor.Data) error {
	if len(data) == 0 {
		return exporter.ErrNoMeasurements
	}
	var entries []*awssqs.SendMessageBatchRequestEntry
	for _, m := range data {
		body, err := json.Marshal(m)
		if err != nil {
			return err
		}
		entries = append(entries, &awssqs.SendMessageBatchRequestEntry{
			Id: aws.String(getID(body)),
			MessageAttributes: map[string]*awssqs.MessageAttributeValue{
				"mac": {
					DataType:    aws.String("String"),
					StringValue: aws.String(m.Addr),
				},
				"name": {
					DataType:    aws.String("String"),
					StringValue: aws.String(m.Name),
				},
			},
			MessageBody: aws.String(string(body)),
		})
	}
	input := &awssqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(e.queueUrl),
	}
	_, err := e.sqs.SendMessageBatchWithContext(ctx, input)
	return err
}

func (e *sqsExporter) Close() error {
	return nil
}

func getID(data []byte) string {
	h := fnv.New64()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
