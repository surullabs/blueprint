// Copyright 2014, Surul Software Labs GmbH
// All rights reserved

// Provides schema migration functionality for Postgres
package schema

import (
	"bitbucket.org/surullabs/goose/lib/goose"
	"fmt"
	"github.com/lib/pq"
	surulio "github.com/surullabs/goutil/io"
	surultpl "github.com/surullabs/goutil/template"
	"path/filepath"
	"strings"
)

type Database struct {
	create migration
	schema migration
}

type migration struct {
	conf *goose.DBConf
	open map[string]string
}

var tempDir surulio.TempDirExecer = &surulio.SafeTempDirExecer{}
var globber func(string, string) (map[string]string, error) = surultpl.GlobBatch
var templater surultpl.BatchTemplater = surultpl.NewFileBatchTemplater(0600)

// This generates a schema creation script in 'dir' by using all *.sql files in
// the create migration as templates. The open string is parameterized and used
// as variables in the template
func (d *Database) Build() error {
	fmt.Printf("Creating %s\n", d.schema.open["dbname"])
	if err := d.templated(up); err != nil {
		return fmt.Errorf("Failed to create database: %v", err)
	}
	fmt.Printf("Applying database schema for %s\n", d.schema.open["dbname"])
	return up(d.schema.conf, d.schema.conf.MigrationsDir)
}

func (d *Database) templated(fn func(*goose.DBConf, string) error) error {
	return tempDir.Exec("schema_"+d.schema.open["dbname"], func(dir string) (err error) {
		var matches map[string]string
		if matches, err = globber(filepath.Join(d.create.conf.MigrationsDir, "*.sql"), dir); err != nil {
			return
		}
		if err = templater.Execute(matches, d.schema.open); err != nil {
			return
		}
		return fn(d.create.conf, dir)
	})
}

func up(conf *goose.DBConf, dir string) error {
	if target, err := goose.GetMostRecentDBVersion(dir); err != nil {
		return err
	} else {
		return goose.RunMigrations(conf, dir, target)
	}
}

func (d *Database) Destroy() error {
	return d.templated(func(c *goose.DBConf, dir string) error {
		return goose.RunMigrations(d.create.conf, dir, 0)
	})
}

// This expects a directory layout with two directories
//    create - Script to create the database
//    schema - The database schema to apply
func NewDatabase(dir, env string) (db *Database, err error) {
	db = &Database{}
	if db.create, err = load(filepath.Join(dir, "create"), env); err != nil {
		return
	}
	if db.schema, err = load(filepath.Join(dir, "schema"), env); err != nil {
		return
	}
	return
}

func load(dir, env string) (m migration, err error) {
	if m.conf, err = goose.NewDBConf(dir, env); err != nil {
		return
	}
	switch m.conf.Driver.Dialect.(type) {
	case *goose.PostgresDialect:
		openStr := m.conf.Driver.OpenStr
		if parsed, parseErr := pq.ParseURL(openStr); parseErr == nil && parsed != "" {
			openStr = parsed
		}
		m.open = make(map[string]string)
		// Note: This will cause problems in cases where there are quoted whitespace.
		parts := strings.Fields(openStr)
		for _, p := range parts {
			subParts := strings.Split(p, "=")
			m.open[subParts[0]] = subParts[1]
		}
	default:
		err = fmt.Errorf("Unsupported dialect %v", m.conf.Driver.Dialect)
		return
	}
	return
}
