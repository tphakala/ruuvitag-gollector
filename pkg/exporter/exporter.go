package exporter

import (
	"context"
	"errors"

	"github.com/niktheblak/ruuvitag-gollector/pkg/sensor"
)

var ErrNoMeasurements = errors.New("at least one measurement must be specified")

type Exporter interface {
	Name() string
	Export(ctx context.Context, data ...sensor.Data) error
	Close() error
}
