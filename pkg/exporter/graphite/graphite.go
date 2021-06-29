package graphite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter"
	"github.com/niktheblak/ruuvitag-gollector/pkg/sensor"
)

type Config struct {
	URL         string
	OrgID       string
	Token       string
	Measurement string
	Interval    time.Duration
	Timeout     time.Duration
}

type graphiteExporter struct {
	client      *http.Client
	url         string
	apiKey      string
	measurement string
	interval    time.Duration
}

func New(cfg Config) exporter.Exporter {
	client := &http.Client{
		Timeout: cfg.Timeout,
	}
	interval := cfg.Interval
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}
	return &graphiteExporter{
		client:      client,
		url:         cfg.URL,
		apiKey:      fmt.Sprintf("%s:%s", cfg.OrgID, cfg.Token),
		measurement: cfg.Measurement,
		interval:    interval,
	}
}

func (e *graphiteExporter) Name() string {
	return fmt.Sprintf("Graphite (%s)", e.url)
}

func (e *graphiteExporter) Export(ctx context.Context, data sensor.Data) error {
	metrics, err := Convert(data, e.measurement, e.interval)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(metrics); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.apiKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", e.apiKey))
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func (e *graphiteExporter) Close() error {
	e.client.CloseIdleConnections()
	return nil
}
