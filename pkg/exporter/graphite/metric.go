package graphite

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/niktheblak/ruuvitag-gollector/pkg/sensor"
)

type GraphiteMetric struct {
	Timestamp int64    `json:"time"`
	Name      string   `json:"name"`
	Interval  int      `json:"interval"`
	Value     float64  `json:"value"`
	Tags      []string `json:"tags"`
}

func Convert(data sensor.Data, measurement string, interval time.Duration) ([]GraphiteMetric, error) {
	intervalSeconds := int(interval / time.Second)
	truncatedTS := data.Timestamp.Truncate(interval).Unix()
	tags := []string{fmt.Sprintf("name=%s", data.Name), fmt.Sprintf("mac=%s", data.Addr)}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	m := make(map[string]interface{})
	if err := json.Unmarshal(jsonData, &m); err != nil {
		return nil, err
	}
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var metrics []GraphiteMetric
	for _, k := range keys {
		v := m[k]
		if v == nil {
			continue
		}
		fv, ok := v.(float64)
		if !ok {
			continue
		}
		metrics = append(metrics, GraphiteMetric{
			Timestamp: truncatedTS,
			Name:      fmt.Sprintf("%s.%s", measurement, k),
			Interval:  intervalSeconds,
			Value:     fv,
			Tags:      tags,
		})

	}
	return metrics, nil
}
