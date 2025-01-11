package contract

import (
	"database/sql"
)

type Client interface {
	Connect() *sql.DB
	GetMigratedFiles(batch uint) ([]string, error)
	GetLastBatch() (MigrationModel, error)
	UpMigrations()
	DownMigrations()
	DropAllMigrations()
	MigrationTable() error
	GetMigrationFilesPath() string
	UpQuery(table string) string
	DownQuery(table string) string
	AddColUpQuery(table string) string
	AddColDownQuery(table string) string
}
