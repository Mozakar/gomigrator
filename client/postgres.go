package client

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Mozakar/gomigrator/contract"
	_ "github.com/lib/pq"
)

type PostgresDbConfig struct {
	Host         string
	Port         string
	User         string
	Passwd       string
	DBName       string
	SslMode      string
	ExtraOptions string
}
type PostgresConnector struct {
	db                 *sql.DB
	MigrationFilesPath string
	DbConfig           PostgresDbConfig
}

func (p *PostgresConnector) Connect() *sql.DB {
	// Get a database handle.
	var err error
	if strings.TrimSpace(p.DbConfig.SslMode) == "" {
		p.DbConfig.SslMode = "disable"
	}
	if strings.TrimSpace(p.DbConfig.ExtraOptions) == "" {
		p.DbConfig.ExtraOptions = " " + p.DbConfig.ExtraOptions
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=%s"+p.DbConfig.ExtraOptions,
		p.DbConfig.Host, p.DbConfig.Port, p.DbConfig.User, p.DbConfig.Passwd, p.DbConfig.DBName, p.DbConfig.SslMode)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
		return nil
	}

	pingErr := db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
		return nil
	}
	p.db = db
	return db
}

func (p *PostgresConnector) GetMigratedFiles(batch uint) ([]string, error) {
	var (
		migrations []string
		rows       *sql.Rows
		err        error
	)
	if batch > 0 {
		rows, err = p.db.Query("SELECT migration FROM migrations WHERE batch = $1 order by id desc", batch)
	} else {
		rows, err = p.db.Query("SELECT migration FROM migrations order by id desc")
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var migration string
		if err := rows.Scan(&migration); err != nil {
			return nil, err
		}
		migrations = append(migrations, migration)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return migrations, nil
}

func (p *PostgresConnector) GetLastBatch() (contract.MigrationModel, error) {
	var lastBatch contract.MigrationModel
	row := p.db.QueryRow("SELECT * FROM migrations order by batch desc limit 1")
	if err := row.Scan(&lastBatch.ID, &lastBatch.Migration, &lastBatch.Batch, &lastBatch.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return lastBatch, nil
		}
		return lastBatch, err
	}
	return lastBatch, nil
}

func (p *PostgresConnector) UpMigrations() {
	var (
		migFiles []string
		filePath = ".up.sql"
		path     = p.GetMigrationFilesPath()
	)
	lastBatch, err := p.GetLastBatch()
	if err != nil {
		log.Println(err)
		return
	}
	migratedMigrations, err := p.GetMigratedFiles(0)
	if err != nil {
		log.Println(err)
		return
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range entries {
		if strings.Contains(e.Name(), filePath) {
			migName := strings.ReplaceAll(e.Name(), filePath, "")
			isMigrated := false
			for _, m := range migratedMigrations {
				if m == migName {
					isMigrated = true
				}
			}
			if !isMigrated {
				migFiles = append(migFiles, migName)
			}
		}
	}

	for _, m := range migFiles {
		_, err := os.Stat(path + m + filePath)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			continue
		}
		query, err := os.ReadFile(path + m + filePath)
		if err != nil {
			log.Printf("migration error: %s\n", err.Error())
			continue
		}
		_, err = p.db.Exec(string(query))
		if err != nil {
			log.Printf("migration error: %s\n", err.Error())
			return
		}

		_, err = p.db.Exec("INSERT INTO migrations (migration, batch) VALUES ($1, $2)", m, lastBatch.Batch+1)
		if err != nil {
			fmt.Printf("insert %s: %v", m, err)
		}
		log.Println(m + " migrated successfully.")
	}
}

func (p *PostgresConnector) DownMigrations() {
	var (
		filePath = ".down.sql"
		path     = p.GetMigrationFilesPath()
	)
	lastBatch, err := p.GetLastBatch()
	if err != nil {
		log.Println(err)
		return
	}
	migratedMigrations, err := p.GetMigratedFiles(lastBatch.Batch)
	if err != nil {
		log.Println(err)
		return
	}
	for _, m := range migratedMigrations {
		_, err := os.Stat(path + m + filePath)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			continue
		}
		query, err := os.ReadFile(path + m + filePath)
		if err != nil {
			log.Printf("migration error: %s\n", err.Error())
			return
		}

		_, err = p.db.Exec(string(query))
		if err != nil {
			log.Printf("migration error: %s\n", err.Error())
			return
		}

		_, err = p.db.Query("DELETE FROM migrations WHERE migration = $1", m)
		if err != nil {
			fmt.Printf("deleting %s: %v", m, err)
		}
		log.Println(m + " rolled back successfully.")
	}
}

func (p *PostgresConnector) DropAllMigrations() {
	var (
		filePath = ".down.sql"
		path     = p.GetMigrationFilesPath()
	)
	migratedMigrations, err := p.GetMigratedFiles(0)
	if err != nil {
		log.Printf("migration error: %s\n", err.Error())
		return
	}

	for _, m := range migratedMigrations {
		_, err := os.Stat(path + m + filePath)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			continue
		}
		query, err := os.ReadFile(path + m + filePath)
		if err != nil {
			log.Printf("migration error: %s\n", err.Error())
			return
		}

		_, err = p.db.Exec(string(query))
		if err != nil {
			log.Printf("migration error: %s\n", err.Error())
			return
		}

		_, err = p.db.Query("DELETE FROM migrations WHERE migration =$1", m)
		if err != nil {
			fmt.Printf("deleting %s: %v", m, err)
			return
		}
		log.Println(m + " drop successfully.")
	}
}

func (p PostgresConnector) UpQuery(table string) string {
	return "CREATE TABLE IF NOT EXISTS " + table + " (\n id BIGSERIAL PRIMARY KEY,\n created_at timestamptz  DEFAULT NOW(),\n updated_at timestamptz NULL);"
}

func (p PostgresConnector) DownQuery(table string) string {
	return "DROP TABLE IF EXISTS " + table + ";"
}

func (p PostgresConnector) AddColUpQuery(table string) string {
	return "ALTER TABLE " + table + " ADD COLUMN IF NOT EXISTS columnName INTEGER;"
}

func (p PostgresConnector) AddColDownQuery(table string) string {
	return "ALTER TABLE " + table + "  DROP COLUMN IF EXISTS columnName;"
}

func (p PostgresConnector) MigrationTable() error {
	q := `CREATE TABLE IF NOT EXISTS migrations (
		id BIGSERIAL PRIMARY KEY,
		migration VARCHAR(230) NULL,
		batch int DEFAULT 0,
		created_at timestamptz  DEFAULT NOW());`

	_, err := p.db.Exec(q)
	if err != nil {
		return err
	}
	return nil
}

func (p PostgresConnector) GetMigrationFilesPath() string {
	return p.MigrationFilesPath
}
