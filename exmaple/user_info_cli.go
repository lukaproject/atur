package main

import (
	"github.com/lukaproject/atur/exmaple/cmd"
	"github.com/lukaproject/atur/exmaple/db"
)

func main() {
	db.Init()
	cmd.Execute()
}
