package gomigrator

import (
	"fmt"
	"os"
	"strings"

	"github.com/Mozakar/gomigrator/cmd"
	"github.com/Mozakar/gomigrator/contract"
)

func Gomigrator(c contract.Client) {
	args := os.Args[1:]
	if len(args) > 0 && strings.ToLower(args[0]) == "gomigrator" {
		if len(args) == 1 {
			printHelp()
			return
		}
		command := strings.ToLower(args[1])
		if command == "make:create" {
			cmd.CreateMigration(c, args, false)
		} else if command == "make:alter" {
			cmd.CreateMigration(c, args, true)
		} else if command == "down" || command == "up" || command == "fresh" {
			cmd.RunMigrate(c, args)
		} else {
			printHelp()
		}
	}
}

func printHelp() {
	fmt.Println(`gomigrator is a tool for managing database migrations in Go.

Usage:

        gomigrator <command> [arguments]

The commands are:

        up                  	migrate to the latest version
        down                	rollback the latest migration
        down --all          	rollback all migrations
        fresh               	rollback all migrations and migrate from scratch
        make:create -table  	create a migration for creating a new table
        make:alter -table     create a migration for modifying an existing table

Use "gomigrator help <command>" for more information about a command.`)
}
