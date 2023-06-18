package main

import (
	"os"
	"os/exec"
)

func syncLog(position Position) error {
	err := os.MkdirAll("log", 0744)
	if err != nil {
		return err
	}
	err = os.Chdir("log")
	if err != nil {
		return err
	}
	cmd := exec.Command("mysqlbinlog", "-R", "-h", mysql_host, "-u", mysql_user, "--password="+mysql_password, "--raw", "-t", position.Name)
	err = cmd.Run()
	if err != nil {
		return err
	}
	err = os.Chdir("..")
	if err != nil {
		return err
	}
	return nil
}
