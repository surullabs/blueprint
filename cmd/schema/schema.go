// Copyright 2014, Surul Software Labs GmbH
// All rights reserved

// Command line
package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/surullabs/blueprint"
	"os"
)

type schemactl struct {
	flagSet *flag.FlagSet
	op      schemaOp
	command string
	dir     string
	env     string
}

func (c *schemactl) printUsage(u string) {
	var buffer string
	if u == "" {
		buffer = ""
	} else {
		buffer = "\n\n"
	}
	fmt.Fprintf(os.Stderr, "%s%sUsage: schema command [OPTIONS]\n\n", u, buffer)
	c.flagSet.PrintDefaults()
}

func newSchemaCtl() *schemactl {
	c := &schemactl{
		flagSet: flag.NewFlagSet("config", flag.ContinueOnError),
	}

	c.flagSet.Usage = func() {}

	c.flagSet.StringVar(
		&c.dir, "dir", ".", "Directory containing the schema to use")
	c.flagSet.StringVar(
		&c.env, "env", "development", "Schema environment to use")
	return c
}

type schemaOp func(*blueprint.Database) error

func (c *schemactl) build(db *blueprint.Database) (err error) {
	return db.Build()
}

func (c *schemactl) destroy(db *blueprint.Database) (err error) {
	return db.Destroy()
}

func (c *schemactl) chooseCommand() (err error) {
	if len(os.Args) < 2 {
		return errors.New("Please provide a command.")
	}
	switch os.Args[1] {
	case "build":
		c.op = c.build
	case "destroy":
		c.op = c.destroy
	default:
		return errors.New("Please provide a valid command.")
	}
	c.command = os.Args[1]
	return
}

func (c *schemactl) readConfig() (err error) {
	if err = c.chooseCommand(); err != nil {
		return
	}
	if err = c.flagSet.Parse(os.Args[2:]); err != nil {
		return errors.New("")
	}
	return
}

func (c *schemactl) run() (err error) {
	var db *blueprint.Database
	if db, err = blueprint.NewDatabase(c.dir, c.env); err != nil {
		return
	}
	if err = c.op(db); err != nil {
		return
	}
	return
}

func main() {
	c := newSchemaCtl()
	if err := c.readConfig(); err != nil {
		c.printUsage(err.Error())
		os.Exit(2)
	}
	if err := c.run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
}
