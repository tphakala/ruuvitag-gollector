// +build graphite

package cmd

import (
	"time"

	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter/graphite"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.PersistentFlags().Bool("graphite.enabled", false, "Store measurements to Graphite")
	rootCmd.PersistentFlags().String("graphite.url", "http://localhost:8080/metrics", "Graphite API URL with protocol, host and port")
	rootCmd.PersistentFlags().String("graphite.org", "", "Graphite organization ID")
	rootCmd.PersistentFlags().String("graphite.token", "", "Graphite token")
	rootCmd.PersistentFlags().String("graphite.measurement", "", "Graphite measurement name")
}

func addGraphiteExporter(exporters *[]exporter.Exporter) error {
	g := graphite.New(graphite.Config{
		URL:         viper.GetString("graphite.url"),
		OrgID:       viper.GetString("graphite.org"),
		Token:       viper.GetString("graphite.token"),
		Measurement: viper.GetString("graphite.measurement"),
		Interval:    viper.GetDuration("interval"),
		Timeout:     10 * time.Second,
	})
	*exporters = append(*exporters, g)
	return nil
}
