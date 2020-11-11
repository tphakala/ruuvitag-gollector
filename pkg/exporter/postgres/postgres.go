// +build postgresql

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/niktheblak/ruuvitag-gollector/pkg/exporter"
	"github.com/niktheblak/ruuvitag-gollector/pkg/sensor"

	_ "github.com/lib/pq"
)

const insertTemplate = `
INSERT INTO %s (
  mac,
  name,
  ts,
  temperature,
  humidity,
  pressure,
  acceleration_x,
  acceleration_y,
  acceleration_z,
  movement_counter,
  battery
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

type postgresExporter struct {
	db         *sql.DB
	insertStmt string
}

func New(connStr, table string) (exporter.Exporter, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return &postgresExporter{
		db:         db,
		insertStmt: fmt.Sprintf(insertTemplate, table),
	}, nil
}

func (p *postgresExporter) Name() string {
	return "Postgres"
}

func (p *postgresExporter) Export(ctx context.Context, data ...sensor.Data) error {
	if len(data) == 0 {
		return exporter.ErrNoMeasurements
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, m := range data {
		_, err := tx.ExecContext(ctx, p.insertStmt, m.Addr, m.Name, m.Timestamp, m.Temperature, m.Humidity, m.Pressure, m.AccelerationX, m.AccelerationY, m.AccelerationZ, m.MovementCounter, m.BatteryVoltage)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (p *postgresExporter) Close() error {
	return p.db.Close()
}
