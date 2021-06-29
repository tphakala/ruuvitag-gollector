// +build !graphite

package cmd

import (
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter"
)

func addGraphiteExporter(exporters *[]exporter.Exporter) error {
	return ErrNotEnabled
}
