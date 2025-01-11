package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Mozakar/gomigrator/contract"
)

func CreateMigration(c contract.Client, args []string, isAlter bool) error {
	if isAlter {
		fmt.Println("create a migration for modifying an existing table")
	} else {
		fmt.Println("create a migration for creating a new table...")
	}
	res := parseCommandFlag(args)
	if res.Table == "" {
		fmt.Println("error: table name (-table OR -t) is required")
		return errors.New("error: table name (-table OR -t) is required")
	}

	t := fmt.Sprintf("%v", time.Now().UnixMilli())
	action := "_create_"
	if isAlter {
		action = "_alter_"
	}
	upFileFullPath := c.GetMigrationFilesPath() + t + action + res.Table + "_table.up.sql"
	downFileFullPath := c.GetMigrationFilesPath() + t + action + res.Table + "_table.down.sql"
	up, err := os.Create(upFileFullPath)
	if err != nil {
		return err
	}
	defer up.Close()

	down, err := os.Create(downFileFullPath)
	if err != nil {
		return err
	}
	defer down.Close()
	if isAlter {
		_, err = up.WriteString(c.AddColUpQuery(res.Table))
		if err != nil {
			return err
		}
		_, err = down.WriteString(c.AddColDownQuery(res.Table))
		if err != nil {
			return err
		}
	} else {
		_, err = up.WriteString(c.UpQuery(res.Table))
		if err != nil {
			return err
		}
		_, err = down.WriteString(c.DownQuery(res.Table))
		if err != nil {
			return err
		}
	}

	fmt.Println(upFileFullPath)
	fmt.Println(downFileFullPath)

	return nil
}

type parsedFlag struct {
	Table  string
	Column string
}

func parseCommandFlag(args []string) parsedFlag {
	var res parsedFlag
	tableFlags := []string{
		"--table",
		"-table",
		"--t",
		"-t",
	}
	colFlags := []string{
		"--column",
		"-column",
		"--c",
		"-c",
	}
	for i, a := range args {
		a = strings.ToLower(a)
		for _, f := range tableFlags {
			if strings.Contains(a, f) {
				args[i] = "_"
				val := strings.Replace(a, f, "", -1)
				val = strings.Replace(val, "=", "", -1)
				val = strings.TrimSpace(val)
				res.Table = strings.Replace(val, " ", "_", -1)
				break
			}
		}
		for _, f := range colFlags {
			if strings.Contains(a, f) {
				args[i] = "_"
				val := strings.Replace(a, f, "", -1)
				val = strings.Replace(val, "=", "", -1)
				val = strings.TrimSpace(val)
				res.Column = strings.Replace(val, " ", "_", -1)
				break
			}
		}
	}
	return res
}
