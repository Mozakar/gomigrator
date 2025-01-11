package client

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Mozakar/gomigrator/contract"
	"github.com/go-sql-driver/mysql"
)

type MysqlConnector struct {
	db                 *sql.DB
	MigrationFilesPath string
	DbConfig           mysql.Config
}

func (m *MysqlConnector) Connect() *sql.DB {
	// Get a database handle.
	var err error
	db, err := sql.Open("mysql", m.DbConfig.FormatDSN())
	if err != nil {
		log.Fatal(err)
		return nil
	}

	pingErr := db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
		return nil
	}
	m.db = db
	return db
}

func (m *MysqlConnector) GetMigratedFiles(batch uint) ([]string, error) {
	var (
		migrations []string
		rows       *sql.Rows
		err        error
	)
	if batch > 0 {
		rows, err = m.db.Query("SELECT migration FROM migrations WHERE batch = ? order by id desc", batch)
	} else {
		rows, err = m.db.Query("SELECT migration FROM migrations order by id desc")
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

func (m *MysqlConnector) GetLastBatch() (contract.MigrationModel, error) {
	var lastBatch contract.MigrationModel
	row := m.db.QueryRow("SELECT * FROM migrations order by batch desc limit 1")
	if err := row.Scan(&lastBatch.ID, &lastBatch.Migration, &lastBatch.Batch, &lastBatch.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return lastBatch, nil
		}
		return lastBatch, err
	}
	return lastBatch, nil
}

func (my *MysqlConnector) UpMigrations() {
	var (
		migFiles []string
		filePath = ".up.sql"
		path     = my.GetMigrationFilesPath()
	)
	lastBatch, err := my.GetLastBatch()
	if err != nil {
		log.Println(err)
		return
	}
	migratedMigrations, err := my.GetMigratedFiles(0)
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
		_, err = my.db.Exec(string(query))
		if err != nil {
			log.Printf("migration error: %s\n", err.Error())
			return
		}

		_, err = my.db.Exec("INSERT INTO migrations (migration, batch) VALUES (?, ?)", m, lastBatch.Batch+1)
		if err != nil {
			fmt.Printf("insert %s: %v", m, err)
		}
		log.Println(m + " migrated successfully.")
	}
}

func (my *MysqlConnector) DownMigrations() {
	var (
		filePath = ".down.sql"
		path     = my.GetMigrationFilesPath()
	)
	lastBatch, err := my.GetLastBatch()
	if err != nil {
		log.Println(err)
		return
	}
	migratedMigrations, err := my.GetMigratedFiles(lastBatch.Batch)
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

		_, err = my.db.Exec(string(query))
		if err != nil {
			log.Printf("migration error: %s\n", err.Error())
			return
		}

		_, err = my.db.Query("DELETE FROM `migrations` WHERE migration = ?", m)
		if err != nil {
			fmt.Printf("deleting %s: %v", m, err)
		}
		log.Println(m + " rolled back successfully.")
	}
}

func (my *MysqlConnector) DropAllMigrations() {
	var (
		filePath = ".down.sql"
		path     = my.GetMigrationFilesPath()
	)
	migratedMigrations, err := my.GetMigratedFiles(0)
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

		_, err = my.db.Exec(string(query))
		if err != nil {
			log.Printf("migration error: %s\n", err.Error())
			return
		}

		_, err = my.db.Query("DELETE FROM `migrations` WHERE migration = ?", m)
		if err != nil {
			fmt.Printf("deleting %s: %v", m, err)
		}
		log.Println(m + " drop successfully.")
	}
}

func (m *MysqlConnector) MigrationTable() error {
	q := `CREATE TABLE IF NOT EXISTS migrations (
				id         	INT AUTO_INCREMENT NOT NULL,
				migration 	VARCHAR(230) NULL,
				batch 			int DEFAULT 0,
				created_at 	TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (id)
	);`
	_, err := m.db.Exec(q)
	if err != nil {
		return err
	}
	return nil
}

func (m MysqlConnector) UpQuery(table string) string {
	return "CREATE TABLE IF NOT EXISTS " + table + " (\n id INT AUTO_INCREMENT NOT NULL,\n created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,\n updated_at TIMESTAMP NULL,\n PRIMARY KEY (id));"
}

func (m MysqlConnector) DownQuery(table string) string {
	return "DROP TABLE IF EXISTS " + table + ";"
}

func (m MysqlConnector) AddColUpQuery(table string) string {
	return "ALTER TABLE " + table + " ADD COLUMN columnName INT;"
}

func (m MysqlConnector) AddColDownQuery(table string) string {
	return "ALTER TABLE " + table + " DROP COLUMN columnName;"
}

func (m *MysqlConnector) GetMigrationFilesPath() string {
	return m.MigrationFilesPath
}
