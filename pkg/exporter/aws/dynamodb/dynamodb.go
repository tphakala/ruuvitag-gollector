// +build aws

package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter"
	"github.com/niktheblak/ruuvitag-gollector/pkg/sensor"
)

type Config struct {
	Table           string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type dynamoDBExporter struct {
	sess  *session.Session
	db    dynamodbiface.DynamoDBAPI
	table string
}

func New(cfg Config) (exporter.Exporter, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(cfg.Region),
		Credentials: credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
	})
	if err != nil {
		return nil, err
	}
	db := dynamodb.New(sess)
	return &dynamoDBExporter{
		sess:  sess,
		db:    db,
		table: cfg.Table,
	}, nil
}

func (e *dynamoDBExporter) Name() string {
	return "AWS DynamoDB"
}

func (e *dynamoDBExporter) Export(ctx context.Context, data ...sensor.Data) error {
	if len(data) == 0 {
		return exporter.ErrNoMeasurements
	}
	var items []*dynamodb.WriteRequest
	for _, m := range data {
		item, err := dynamodbattribute.MarshalMap(m)
		if err != nil {
			return err
		}
		items = append(items, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{Item: item},
		})
	}
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			e.table: items,
		},
	}
	_, err := e.db.BatchWriteItemWithContext(ctx, input)
	if err != nil {
		return err
	}
	return nil
}

func (e *dynamoDBExporter) Close() error {
	return nil
}
