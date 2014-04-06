// Copyright 2014, Surul Software Labs GmbH
// All rights reserved

// Provides schema migration functionality for Postgres
package blueprint

import (
	"bitbucket.org/surullabs/goose/lib/goose"
	"fmt"
	"github.com/lib/pq"
	"github.com/surullabs/fault"
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

var check = fault.NewChecker()

// This generates a schema creation script in 'dir' by using all *.sql files in
// the create migration as templates. The open string is parameterized and used
// as variables in the template
func (d *Database) Build() (err error) {
	defer check.Recover(&err)
	fmt.Printf("Creating %s\n", d.schema.open["dbname"])
	check.Error(d.templated(up))
	fmt.Printf("Applying database schema for %s\n", d.schema.open["dbname"])
	return up(d.schema.conf, d.schema.conf.MigrationsDir)
}

func (d *Database) templated(fn func(*goose.DBConf, string) error) error {
	return tempDir.Exec("schema_"+d.schema.open["dbname"], func(dir string) (err error) {
		matches := check.Return(globber(filepath.Join(d.create.conf.MigrationsDir, "*.sql"), dir)).(map[string]string)
		check.Error(templater.Execute(matches, d.schema.open))
		return fn(d.create.conf, dir)
	})
}

func up(conf *goose.DBConf, dir string) error {
	target := check.Return(goose.GetMostRecentDBVersion(dir)).(int64)
	return goose.RunMigrations(conf, dir, target)
}

func (d *Database) Destroy() (err error) {
	defer check.Recover(&err)
	return d.templated(func(c *goose.DBConf, dir string) error {
		return goose.RunMigrations(d.create.conf, dir, 0)
	})
}

// This expects a directory layout with two directories
//    create - Script to create the database
//    schema - The database schema to apply
func NewDatabase(dir, env string) (db *Database, err error) {
	defer check.Recover(&err)
	db = &Database{}
	db.create = check.Return(load(filepath.Join(dir, "create"), env)).(migration)
	db.schema = check.Return(load(filepath.Join(dir, "schema"), env)).(migration)
	return
}

func load(dir, env string) (m migration, err error) {
	m.conf = check.Return(goose.NewDBConf(dir, env)).(*goose.DBConf)
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
