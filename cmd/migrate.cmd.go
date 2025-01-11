package cmd

import (
	"fmt"
	"log"

	"github.com/Mozakar/gomigrator/contract"
)

func RunMigrate(connector contract.Client, args []string) {
	var (
		down       = false
		downArg    = "down"
		downAll    = false
		downAllArg = "--all"
		fresh      = false
		freshArg   = "fresh"
	)

	for _, arg := range args {
		switch arg {
		case downArg:
			down = true
		case downAllArg:
			downAll = true
		case freshArg:
			fresh = true
		}
	}

	db := connector.Connect()

	defer db.Close()

	if err := connector.MigrationTable(); err != nil {
		log.Println(err)
		return
	}

	if down && downAll {
		fmt.Println("Rolling back all migrations...")
		connector.DropAllMigrations()
	} else if down {
		fmt.Println("Rolling back the latest migration...")
		connector.DownMigrations()
	} else if fresh {
		fmt.Println("Refreshing all migrations...")
		connector.DropAllMigrations()
		connector.UpMigrations()
	} else {
		fmt.Println("Running migrations up to the latest version...")
		connector.UpMigrations()
	}
}
