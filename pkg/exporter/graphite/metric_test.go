package graphite

import (
	"testing"
	"time"

	"github.com/niktheblak/ruuvitag-gollector/pkg/sensor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvert(t *testing.T) {
	data := sensor.Data{
		Addr:            "CC:CA:7E:52:CC:34",
		Name:            "Backyard",
		Temperature:     21.5,
		Humidity:        60,
		Pressure:        1002,
		BatteryVoltage:  50,
		AccelerationX:   0,
		AccelerationY:   0,
		AccelerationZ:   0,
		MovementCounter: 1,
		Timestamp:       time.Date(2020, time.January, 1, 0, 0, 23, 331, time.UTC),
	}
	metrics, err := Convert(data, "ruuvitag", 10*time.Second)
	require.NoError(t, err)
	assert.Len(t, metrics, 9)
	t.Logf("Metrics: %+v", metrics)
	temp := metrics[8]
	assert.Equal(t, "ruuvitag.temperature", temp.Name)
	assert.Equal(t, []string{"name=Backyard", "mac=CC:CA:7E:52:CC:34"}, temp.Tags)
	assert.Equal(t, int64(1577836820), temp.Timestamp)
	assert.Equal(t, 21.5, temp.Value)
	ts := time.Unix(temp.Timestamp, 0).In(time.UTC)
	assert.Equal(t, time.Date(2020, time.January, 1, 0, 0, 20, 0, time.UTC), ts)
}
