// Copyright [2024] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package reconciliation

// sqlDriver is the small subset of SQLite behavior this package needs. It is
// implemented over cgo on linux and stubbed elsewhere so the rest of the
// package stays platform independent.
type sqlDriver interface {
	// Exec runs a statement that returns no rows and reports rows affected.
	Exec(query string, args ...interface{}) (int64, error)
	// Query runs a statement and returns column names and rows. Values are
	// nil, int64, or string.
	Query(query string, args ...interface{}) ([]string, [][]interface{}, error)
	// Begin starts a transaction. readOnly selects a deferred (read) or
	// immediate (write) transaction.
	Begin(readOnly bool) error
	Commit() error
	Rollback() error
	LastInsertID() (int64, error)
	Close() error
}

// openSQLiteDriver is supplied by the platform-specific files.
var openSQLiteDriver func(path string) (sqlDriver, error)
