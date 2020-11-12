package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-ble/ble"
	"github.com/mitchellh/go-homedir"
	"github.com/niktheblak/gcloudzap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter/aws/dynamodb"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter/aws/sqs"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter/console"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter/gcp/pubsub"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter/http"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter/influxdb"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter/postgres"
)

var (
	logger      *zap.Logger
	peripherals map[string]string
	exporters   []exporter.Exporter
	cfgFile     string
	device      string
)

var rootCmd = &cobra.Command{
	Use:               "ruuvitag-gollector",
	Short:             "Collects measurements from RuuviTag sensors",
	SilenceUsage:      true,
	PersistentPreRunE: run,
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		if logger != nil {
			logger.Sync()
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ruuvitag-gollector.yaml)")

	rootCmd.PersistentFlags().StringToString("ruuvitags", nil, "RuuviTag addresses and names to use")
	rootCmd.PersistentFlags().String("device", "default", "HCL device to use")
	rootCmd.PersistentFlags().BoolP("console", "c", false, "Print measurements to console")
	rootCmd.PersistentFlags().String("loglevel", "info", "Log level")

	rootCmd.PersistentFlags().Bool("influxdb.enabled", false, "Store measurements to InfluxDB")
	rootCmd.PersistentFlags().String("influxdb.addr", "http://localhost:8086", "InfluxDB address with protocol, host and port")
	rootCmd.PersistentFlags().String("influxdb.database", "", "InfluxDB database to use")
	rootCmd.PersistentFlags().String("influxdb.measurement", "", "InfluxDB measurement name")
	rootCmd.PersistentFlags().String("influxdb.username", "", "InfluxDB username")
	rootCmd.PersistentFlags().String("influxdb.password", "", "InfluxDB password")

	rootCmd.PersistentFlags().Bool("gcp.stackdriver.enabled", false, "Send logs to Google Stackdriver")
	rootCmd.PersistentFlags().String("gcp.credentials", "", "Google Cloud application credentials file")
	rootCmd.MarkFlagFilename("gcp.credentials", "json")
	rootCmd.PersistentFlags().String("gcp.project", "", "Google Cloud Platform project")
	rootCmd.PersistentFlags().Bool("gcp.pubsub.enabled", false, "Send measurements to Google Pub/Sub")
	rootCmd.PersistentFlags().String("gcp.pubsub.topic", "", "Google Pub/Sub topic to use")

	rootCmd.PersistentFlags().String("aws.region", "us-east-2", "AWS region")
	rootCmd.PersistentFlags().String("aws.access_key_id", "", "AWS access key ID")
	rootCmd.PersistentFlags().String("aws.secret_access_key", "", "AWS secret access key")
	rootCmd.PersistentFlags().String("aws.session_token", "", "AWS session token")
	rootCmd.PersistentFlags().Bool("aws.dynamodb.enabled", false, "Store measurements to AWS DynamoDB")
	rootCmd.PersistentFlags().String("aws.dynamodb.table", "", "AWS DynamoDB table name")
	rootCmd.PersistentFlags().Bool("aws.sqs.enabled", false, "Send measurements to AWS SQS")
	rootCmd.PersistentFlags().String("aws.sqs.queue.name", "", "AWS SQS queue name")
	rootCmd.PersistentFlags().String("aws.sqs.queue.url", "", "AWS SQS queue URL")

	rootCmd.PersistentFlags().Bool("http.enabled", false, "Send measurements as JSON to a HTTP endpoint")
	rootCmd.PersistentFlags().String("http.url", "", "HTTP receiver URL")
	rootCmd.PersistentFlags().String("http.token", "", "HTTP receiver authorization token")

	if err := viper.BindPFlags(rootCmd.PersistentFlags()); err != nil {
		log.Fatal(err)
	}
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := homedir.Dir()
		if err == nil {
			viper.AddConfigPath(home)
		}
		viper.AddConfigPath(".")
		viper.SetConfigName("ruuvitag-gollector.yaml")
	}
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	viper.ReadInConfig()
}

func run(cmd *cobra.Command, args []string) error {
	creds := viper.GetString("gcp.credentials")
	if creds != "" {
		if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", creds); err != nil {
			return err
		}
	}
	if viper.GetBool("gcp.stackdriver.enabled") {
		project := viper.GetString("gcp.project")
		if project == "" {
			return fmt.Errorf("Google Cloud Platform project must be specified")
		}
		var err error
		logger, err = gcloudzap.NewProduction(project, "ruuvitag-gollector")
		if err != nil {
			return fmt.Errorf("failed to create Stackdriver logger: %w", err)
		}
	} else {
		logLevel := viper.GetString("loglevel")
		if logLevel == "" {
			logLevel = "info"
		}
		var zapLogLevel zap.AtomicLevel
		if err := zapLogLevel.UnmarshalText([]byte(logLevel)); err != nil {
			return err
		}
		cfg := zap.Config{
			Level:            zapLogLevel,
			Encoding:         "console",
			EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
			OutputPaths:      []string{"stdout"},
			ErrorOutputPaths: []string{"stderr"},
		}
		var err error
		logger, err = cfg.Build()
		if err != nil {
			return fmt.Errorf("failed to create logger: %w", err)
		}
	}
	ruuviTags := viper.GetStringMapString("ruuvitags")
	logger.Info("RuuviTags", zap.Any("ruuvitags", ruuviTags))
	peripherals = make(map[string]string)
	for addr, name := range ruuviTags {
		peripherals[ble.NewAddr(addr).String()] = name
	}
	if viper.GetBool("console") {
		exporters = append(exporters, console.Exporter{})
	}
	if viper.GetBool("influxdb.enabled") {
		addr := viper.GetString("influxdb.addr")
		if addr == "" {
			return fmt.Errorf("InfluxDB address must be specified")
		}
		influx, err := influxdb.New(influxdb.Config{
			Addr:        addr,
			Database:    viper.GetString("influxdb.database"),
			Measurement: viper.GetString("influxdb.measurement"),
			Username:    viper.GetString("influxdb.username"),
			Password:    viper.GetString("influxdb.password"),
		})
		if err != nil {
			return fmt.Errorf("failed to create InfluxDB exporter: %w", err)
		}
		exporters = append(exporters, influx)
	}
	if viper.GetBool("gcp.pubsub.enabled") {
		ctx := context.Background()
		project := viper.GetString("gcp.project")
		if project == "" {
			return fmt.Errorf("Google Cloud Platform project must be specified")
		}
		topic := viper.GetString("gcp.pubsub.topic")
		if topic == "" {
			return fmt.Errorf("Google Pub/Sub topic must be specified")
		}
		ps, err := pubsub.New(ctx, project, topic)
		if err != nil {
			return fmt.Errorf("failed to create Google Pub/Sub exporter: %w", err)
		}
		exporters = append(exporters, ps)
	}
	if viper.GetBool("aws.dynamodb.enabled") {
		table := viper.GetString("aws.dynamodb.table")
		if table == "" {
			return fmt.Errorf("DynamoDB table name must be specified")
		}
		exp, err := dynamodb.New(dynamodb.Config{
			Table:           table,
			Region:          viper.GetString("aws.region"),
			AccessKeyID:     viper.GetString("aws.access_key_id"),
			SecretAccessKey: viper.GetString("aws.secret_access_key"),
			SessionToken:    viper.GetString("aws.session_token"),
		})
		if err != nil {
			return fmt.Errorf("failed to create AWS DynamoDB exporter: %w", err)
		}
		exporters = append(exporters, exp)
	}
	if viper.GetBool("aws.sqs.enabled") {
		queueName := viper.GetString("aws.sqs.queue.name")
		queueURL := viper.GetString("aws.sqs.queue.url")
		if queueName == "" && queueURL == "" {
			return fmt.Errorf("AWS SQS queue name or queue URL must be specified")
		}
		exp, err := sqs.New(sqs.Config{
			QueueName:       queueName,
			QueueURL:        queueURL,
			Region:          viper.GetString("aws.region"),
			AccessKeyID:     viper.GetString("aws.access_key_id"),
			SecretAccessKey: viper.GetString("aws.secret_access_key"),
			SessionToken:    viper.GetString("aws.session_token"),
		})
		if err != nil {
			return fmt.Errorf("failed to create AWS SQS exporter: %w", err)
		}
		exporters = append(exporters, exp)
	}
	if viper.GetBool("postgres.enabled") {
		ctx := context.Background()
		connStr := viper.GetString("postgres.conn")
		table := viper.GetString("postgres.table")
		exp, err := postgres.New(ctx, connStr, table)
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL exporter: %w", err)
		}
		exporters = append(exporters, exp)
	}
	if viper.GetBool("http.enabled") {
		url := viper.GetString("http.url")
		token := viper.GetString("http.token")
		exp, err := http.New(url, token, 10*time.Second)
		if err != nil {
			return fmt.Errorf("failed to create HTTP exporter: %w", err)
		}
		exporters = append(exporters, exp)
	}
	device = viper.GetString("device")
	return nil
}
