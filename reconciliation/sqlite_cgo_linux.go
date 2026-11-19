// Copyright [2024] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//go:build linux && cgo

package reconciliation

/*
#cgo LDFLAGS: -l:libsqlite3.so.0
#include <stdlib.h>

typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;

extern int sqlite3_open_v2(const char *, sqlite3 **, int, const char *);
extern int sqlite3_close_v2(sqlite3 *);
extern const char *sqlite3_errmsg(sqlite3 *);
extern int sqlite3_extended_errcode(sqlite3 *);
extern int sqlite3_prepare_v2(sqlite3 *, const char *, int, sqlite3_stmt **, const char **);
extern int sqlite3_step(sqlite3_stmt *);
extern int sqlite3_finalize(sqlite3_stmt *);
extern int sqlite3_bind_null(sqlite3_stmt *, int);
extern int sqlite3_bind_int64(sqlite3_stmt *, int, long long);
extern int sqlite3_bind_text(sqlite3_stmt *, int, const char *, int, void (*)(void *));
extern int sqlite3_column_type(sqlite3_stmt *, int);
#define RECON_SQLITE_INTEGER 1
#define RECON_SQLITE_TEXT 3
#define RECON_SQLITE_NULL 5
extern long long sqlite3_column_int64(sqlite3_stmt *, int);
extern const unsigned char *sqlite3_column_text(sqlite3_stmt *, int);
extern int sqlite3_column_bytes(sqlite3_stmt *, int);
extern int sqlite3_column_count(sqlite3_stmt *);
extern const char *sqlite3_column_name(sqlite3_stmt *, int);
extern int sqlite3_changes(sqlite3 *);
extern long long sqlite3_last_insert_rowid(sqlite3 *);
extern int sqlite3_extended_result_codes(sqlite3 *, int);
extern int sqlite3_exec(sqlite3 *, const char *, void *, void *, char **);

static int recon_open(const char *path, sqlite3 **db) {
	return sqlite3_open_v2(path, db, 0x00000002|0x00000004|0x00000040, 0); // READWRITE|CREATE|FULLMUTEX
}

// recon_bind_text uses SQLITE_TRANSIENT so SQLite copies the C string before
// this frame returns and frees it.
static int recon_bind_text(sqlite3_stmt *stmt, int idx, const char *text, int n) {
	return sqlite3_bind_text(stmt, idx, text, n, (void (*)(void *))-1);
}
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

func init() {
	openSQLiteDriver = openCgoSQLite
}

// SQLite primary result codes used by this driver.
const (
	sqliteOK   = 0
	sqliteRow  = 100
	sqliteDone = 101
)

// sqliteFailed reports whether a C result code is an error.
func sqliteFailed(rc C.int) bool { return rc != 0 }

// sqliteError reports a SQLite failure, preserving the extended result code so
// callers can distinguish constraint, busy, and locked failures.
type sqliteError struct {
	code int
	msg  string
}

func (e *sqliteError) Error() string {
	return fmt.Sprintf("sqlite error %d: %s", e.code, e.msg)
}

type cgoSQLite struct {
	mu sync.Mutex
	db *C.sqlite3
}

func openCgoSQLite(path string) (sqlDriver, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var db *C.sqlite3
	rc := C.recon_open(cpath, &db) //nolint:gocritic // cgo call; gocritic misreports dupSubExpr here
	if sqliteFailed(rc) {
		msg := "cannot open database"
		if db != nil {
			msg = C.GoString(C.sqlite3_errmsg(db))
			C.sqlite3_close_v2(db)
		}
		return nil, fmt.Errorf("sqlite open %s: %s (code %d)", path, msg, int(rc))
	}
	C.sqlite3_extended_result_codes(db, 1)
	return &cgoSQLite{db: db}, nil
}

func (s *cgoSQLite) err() error {
	return &sqliteError{
		code: int(C.sqlite3_extended_errcode(s.db)),
		msg:  C.GoString(C.sqlite3_errmsg(s.db)),
	}
}

// prepare builds a statement and binds arguments. Caller must finalize.
func (s *cgoSQLite) prepare(query string, args []interface{}) (*C.sqlite3_stmt, error) {
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))
	var stmt *C.sqlite3_stmt
	rc := C.sqlite3_prepare_v2(s.db, cquery, -1, &stmt, nil) //nolint:gocritic // cgo call; gocritic misreports dupSubExpr here
	if sqliteFailed(rc) {
		return nil, s.err()
	}
	for i, arg := range args {
		idx := C.int(i + 1)
		var rc C.int
		switch v := arg.(type) {
		case nil:
			rc = C.sqlite3_bind_null(stmt, idx)
		case int64:
			rc = C.sqlite3_bind_int64(stmt, idx, C.longlong(v))
		case int:
			rc = C.sqlite3_bind_int64(stmt, idx, C.longlong(v))
		case string:
			cs := C.CString(v)
			rc = C.recon_bind_text(stmt, idx, cs, C.int(len(v)))
			C.free(unsafe.Pointer(cs))
		default:
			C.sqlite3_finalize(stmt)
			return nil, fmt.Errorf("unsupported bind type %T", arg)
		}
		if rc != sqliteOK {
			err := s.err()
			C.sqlite3_finalize(stmt)
			return nil, err
		}
	}
	return stmt, nil
}

func (s *cgoSQLite) Exec(query string, args ...interface{}) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt, err := s.prepare(query, args)
	if err != nil {
		return 0, err
	}
	defer C.sqlite3_finalize(stmt)
	if rc := C.sqlite3_step(stmt); rc != sqliteDone && rc != sqliteRow {
		return 0, s.err()
	}
	return int64(C.sqlite3_changes(s.db)), nil
}

func (s *cgoSQLite) Query(query string, args ...interface{}) ([]string, [][]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt, err := s.prepare(query, args)
	if err != nil {
		return nil, nil, err
	}
	defer C.sqlite3_finalize(stmt)

	ncol := int(C.sqlite3_column_count(stmt))
	cols := make([]string, ncol)
	for i := 0; i < ncol; i++ {
		cols[i] = C.GoString(C.sqlite3_column_name(stmt, C.int(i)))
	}
	var rows [][]interface{}
	for {
		rc := C.sqlite3_step(stmt)
		if rc == sqliteDone {
			break
		}
		if rc != sqliteRow {
			return nil, nil, s.err()
		}
		row := make([]interface{}, ncol)
		for i := 0; i < ncol; i++ {
			idx := C.int(i)
			switch C.sqlite3_column_type(stmt, idx) {
			case C.RECON_SQLITE_NULL:
				row[i] = nil
			case C.RECON_SQLITE_INTEGER:
				row[i] = int64(C.sqlite3_column_int64(stmt, idx))
			case C.RECON_SQLITE_TEXT:
				p := C.sqlite3_column_text(stmt, idx)
				n := C.sqlite3_column_bytes(stmt, idx)
				row[i] = C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
			default:
				row[i] = nil
			}
		}
		rows = append(rows, row)
	}
	return cols, rows, nil
}

func (s *cgoSQLite) execSimple(text string) error {
	cs := C.CString(text)
	defer C.free(unsafe.Pointer(cs))
	if rc := C.sqlite3_exec(s.db, cs, nil, nil, nil); rc != sqliteOK {
		return s.err()
	}
	return nil
}

func (s *cgoSQLite) Begin(readOnly bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if readOnly {
		return s.execSimple("BEGIN")
	}
	return s.execSimple("BEGIN IMMEDIATE")
}

func (s *cgoSQLite) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execSimple("COMMIT")
}

func (s *cgoSQLite) Rollback() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execSimple("ROLLBACK")
}

func (s *cgoSQLite) LastInsertID() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(C.sqlite3_last_insert_rowid(s.db)), nil
}

func (s *cgoSQLite) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	rc := C.sqlite3_close_v2(s.db)
	s.db = nil
	if rc != sqliteOK {
		return fmt.Errorf("sqlite close: code %d", int(rc))
	}
	return nil
}
