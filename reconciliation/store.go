// Copyright [2024] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package reconciliation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Options configures a Store. A Store is scoped to one fixed customer and
// currency partition because reconciliation never crosses either boundary.
type Options struct {
	// Customer is the partition customer identifier. Required.
	Customer string
	// Currency is the partition currency code. Required.
	Currency string
	// BusyTimeout is how long a write waits for a competing writer before
	// returning a busy error. Zero uses a one second default.
	BusyTimeout time.Duration
}

// Store is a durable reconciliation workspace backed by SQLite. It persists
// source documents, confirmed write-off details, current plan, and version
// counters. All methods are safe for concurrent use.
type Store struct {
	mu       sync.Mutex
	drv      sqlDriver
	customer string
	currency string
	closed   bool
}

const (
	metaSourceVersion = "source_version"
	metaPlanGen       = "plan_generation"
	metaCustomer      = "customer"
	metaCurrency      = "currency"
	metaSchema        = "schema_version"
	schemaVersion     = "1"
)

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS documents (
		kind TEXT NOT NULL,
		id TEXT NOT NULL,
		customer TEXT NOT NULL,
		currency TEXT NOT NULL,
		amount INTEGER NOT NULL CHECK (amount >= 0),
		version INTEGER NOT NULL CHECK (version >= 1),
		confirmed_amount INTEGER NOT NULL DEFAULT 0 CHECK (confirmed_amount >= 0),
		balance INTEGER NOT NULL CHECK (balance >= 0),
		status TEXT NOT NULL,
		PRIMARY KEY (kind, id)
	)`,
	`CREATE TABLE IF NOT EXISTS allowed_pairs (
		invoice_id TEXT NOT NULL,
		payment_id TEXT NOT NULL,
		customer TEXT NOT NULL,
		currency TEXT NOT NULL,
		unit_cost INTEGER NOT NULL CHECK (unit_cost >= 0),
		max_amount INTEGER NOT NULL DEFAULT 0 CHECK (max_amount >= 0),
		PRIMARY KEY (invoice_id, payment_id)
	)`,
	`CREATE TABLE IF NOT EXISTS allocations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		invoice_id TEXT NOT NULL,
		payment_id TEXT NOT NULL,
		amount INTEGER NOT NULL CHECK (amount > 0),
		unit_cost INTEGER NOT NULL CHECK (unit_cost >= 0),
		cost INTEGER NOT NULL CHECK (cost >= 0),
		plan_generation INTEGER NOT NULL,
		invoice_version INTEGER NOT NULL,
		payment_version INTEGER NOT NULL,
		status TEXT NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS allocations_invoice ON allocations (invoice_id)`,
	`CREATE INDEX IF NOT EXISTS allocations_payment ON allocations (payment_id)`,
	`CREATE TABLE IF NOT EXISTS plan_state (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		generation INTEGER NOT NULL,
		source_version INTEGER NOT NULL,
		total_allocated INTEGER NOT NULL,
		total_cost INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS plan_items (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		generation INTEGER NOT NULL,
		invoice_id TEXT NOT NULL,
		payment_id TEXT NOT NULL,
		amount INTEGER NOT NULL CHECK (amount > 0),
		unit_cost INTEGER NOT NULL CHECK (unit_cost >= 0),
		cost INTEGER NOT NULL CHECK (cost >= 0)
	)`,
	`CREATE INDEX IF NOT EXISTS plan_items_generation ON plan_items (generation)`,
	`CREATE TABLE IF NOT EXISTS confirmations (
		key TEXT PRIMARY KEY,
		digest TEXT NOT NULL,
		result_json TEXT NOT NULL,
		created_at TEXT NOT NULL
	)`,
}

// Open opens or creates the SQLite database at path for the given partition.
// Reopening an existing database resumes from persisted state: no confirmed
// detail, version counter, or plan is reset.
func Open(path string, opts Options) (*Store, error) {
	if strings.TrimSpace(opts.Customer) == "" {
		return nil, fmt.Errorf("%w: customer is required", ErrInvalid)
	}
	if strings.TrimSpace(opts.Currency) == "" {
		return nil, fmt.Errorf("%w: currency is required", ErrInvalid)
	}
	if openSQLiteDriver == nil {
		return nil, ErrCgoRequired
	}
	drv, err := openSQLiteDriver(path)
	if err != nil {
		return nil, err
	}
	s := &Store{drv: drv, customer: opts.Customer, currency: opts.Currency}
	if err := s.init(opts); err != nil {
		_ = drv.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init(opts Options) error {
	// busy_timeout and foreign keys are per connection, so set them once here.
	if _, err := s.drv.Exec(`PRAGMA busy_timeout = ` + busyTimeoutMillis(opts.BusyTimeout)); err != nil {
		return err
	}
	if _, err := s.drv.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		return err
	}
	if _, err := s.drv.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		return err
	}
	if _, err := s.drv.Exec(`PRAGMA synchronous = FULL`); err != nil {
		return err
	}

	if err := s.drv.Begin(false); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = s.drv.Rollback()
		}
	}()
	for _, stmt := range schemaStatements {
		if _, err := s.drv.Exec(stmt); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}
	storedCustomer, ok, err := s.metaGet(metaCustomer)
	if err != nil {
		return err
	}
	if !ok {
		if err := s.metaSet(metaCustomer, s.customer); err != nil {
			return err
		}
		if err := s.metaSet(metaCurrency, s.currency); err != nil {
			return err
		}
		if err := s.metaSet(metaSchema, schemaVersion); err != nil {
			return err
		}
		if err := s.metaSet(metaSourceVersion, "0"); err != nil {
			return err
		}
		if err := s.metaSet(metaPlanGen, "0"); err != nil {
			return err
		}
		if _, err := s.drv.Exec(`INSERT INTO plan_state (id, generation, source_version, total_allocated, total_cost) VALUES (1, 0, 0, 0, 0)`); err != nil {
			return err
		}
	} else {
		storedCurrency, _, err := s.metaGet(metaCurrency)
		if err != nil {
			return err
		}
		if storedCustomer != s.customer || storedCurrency != s.currency {
			return fmt.Errorf("%w: database belongs to %s/%s, not %s/%s", ErrConflict, storedCustomer, storedCurrency, s.customer, s.currency)
		}
	}
	if err := s.drv.Commit(); err != nil {
		return err
	}
	committed = true

	// Repair the invariant plan-current-as-of-source when a database written by
	// an older build, or interrupted outside SQLite's atomicity, has none.
	return s.ensureCurrentPlan()
}

func busyTimeoutMillis(d time.Duration) string {
	if d <= 0 {
		d = time.Second
	}
	ms := d / time.Millisecond
	if ms < 1 {
		ms = 1
	}
	return fmt.Sprintf("%d", int64(ms))
}

// Close releases the database handle. Outstanding Store operations fail with
// ErrClosed afterwards.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.drv.Close()
}

// Customer returns the partition customer.
func (s *Store) Customer() string { return s.customer }

// Currency returns the partition currency.
func (s *Store) Currency() string { return s.currency }

func (s *Store) checkOpen() error {
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *Store) metaGet(key string) (string, bool, error) {
	_, rows, err := s.drv.Query(`SELECT value FROM meta WHERE key = ?`, key)
	if err != nil {
		return "", false, err
	}
	if len(rows) == 0 {
		return "", false, nil
	}
	v, _ := rows[0][0].(string)
	return v, true, nil
}

func (s *Store) metaSet(key, value string) error {
	_, err := s.drv.Exec(`INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

func (s *Store) metaInt(key string) (int64, error) {
	v, ok, err := s.metaGet(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	var n int64
	if _, err := fmt.Sscan(v, &n); err != nil {
		return 0, fmt.Errorf("%w: corrupt meta %s=%q", ErrInvalid, key, v)
	}
	return n, nil
}

// transaction runs fn inside a write transaction. A failure rolls back every
// write, leaving the previous state intact.
func (s *Store) transaction(fn func() error) error {
	if err := s.drv.Begin(false); err != nil {
		return err
	}
	if err := fn(); err != nil {
		_ = s.drv.Rollback()
		return err
	}
	if err := s.drv.Commit(); err != nil {
		_ = s.drv.Rollback()
		return err
	}
	return nil
}

// PutInvoice inserts or corrects an invoice from the receivables system.
func (s *Store) PutInvoice(inv Invoice) error {
	return s.putDocument(KindInvoice, inv.ID, inv.Customer, inv.Currency, inv.Amount, inv.Version)
}

// PutPayment inserts or corrects a payment from the receipts system.
func (s *Store) PutPayment(p Payment) error {
	return s.putDocument(KindPayment, p.ID, p.Customer, p.Currency, p.Amount, p.Version)
}

func (s *Store) putDocument(kind, id, customer, currency string, amount, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if err := s.validateDocument(kind, id, customer, currency, amount, version); err != nil {
		return err
	}
	return s.transaction(func() error {
		changed, err := s.upsertDocument(kind, id, amount, version)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		if err := s.bumpSourceVersion(); err != nil {
			return err
		}
		return s.recomputePlan()
	})
}

func (s *Store) validateDocument(kind, id, customer, currency string, amount, version int64) error {
	if id == "" {
		return fmt.Errorf("%w: %s id is required", ErrInvalid, kind)
	}
	if customer != s.customer || currency != s.currency {
		return fmt.Errorf("%w: %s %s has partition %s/%s, store is %s/%s", ErrInvalid, kind, id, customer, currency, s.customer, s.currency)
	}
	if amount < 0 {
		return fmt.Errorf("%w: %s %s has negative amount %d", ErrInvalid, kind, id, amount)
	}
	if version < 0 {
		return fmt.Errorf("%w: %s %s has negative version %d", ErrInvalid, kind, id, version)
	}
	return nil
}

// ImportBatch is a set of source changes applied atomically.
type ImportBatch struct {
	Invoices     []Invoice
	Payments     []Payment
	AllowedPairs []AllowedPair
}

// Import applies a batch of source records in a single SQLite transaction. If
// any record is rejected, nothing is written and the previous state, source
// version, and plan generation are preserved. When the batch changes nothing,
// the source version is not advanced, so existing plans stay current.
func (s *Store) Import(batch ImportBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	// Validate everything before opening the transaction so the common error
	// path does no I/O.
	for i := range batch.Invoices {
		inv := batch.Invoices[i]
		if err := s.validateDocument(KindInvoice, strings.TrimSpace(inv.ID), inv.Customer, inv.Currency, inv.Amount, inv.Version); err != nil {
			return fmt.Errorf("invoice[%d]: %w", i, err)
		}
	}
	for i := range batch.Payments {
		p := batch.Payments[i]
		if err := s.validateDocument(KindPayment, strings.TrimSpace(p.ID), p.Customer, p.Currency, p.Amount, p.Version); err != nil {
			return fmt.Errorf("payment[%d]: %w", i, err)
		}
	}
	for i := range batch.AllowedPairs {
		if err := s.validatePair(batch.AllowedPairs[i]); err != nil {
			return fmt.Errorf("allowedPair[%d]: %w", i, err)
		}
	}

	return s.transaction(func() error {
		changed := false
		for _, inv := range batch.Invoices {
			c, err := s.upsertDocument(KindInvoice, strings.TrimSpace(inv.ID), inv.Amount, inv.Version)
			if err != nil {
				return err
			}
			changed = changed || c
		}
		for _, p := range batch.Payments {
			c, err := s.upsertDocument(KindPayment, strings.TrimSpace(p.ID), p.Amount, p.Version)
			if err != nil {
				return err
			}
			changed = changed || c
		}
		for _, pair := range batch.AllowedPairs {
			c, err := s.upsertPair(pair)
			if err != nil {
				return err
			}
			changed = changed || c
		}
		if !changed {
			return nil
		}
		if err := s.bumpSourceVersion(); err != nil {
			return err
		}
		return s.recomputePlan()
	})
}

// upsertDocument returns true when the stored document changed.
func (s *Store) upsertDocument(kind, id string, amount, version int64) (bool, error) {
	existing, found, err := s.getDocumentRow(kind, id)
	if err != nil {
		return false, err
	}
	if !found {
		if version == 0 {
			version = 1
		}
		status := statusFor(amount, 0)
		_, err := s.drv.Exec(`INSERT INTO documents (kind, id, customer, currency, amount, version, confirmed_amount, balance, status) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)`,
			kind, id, s.customer, s.currency, amount, version, amount, status)
		return true, err
	}
	if version == 0 {
		version = existing.version
	}
	if version < existing.version {
		return false, fmt.Errorf("%w: %s %s version %d is older than stored version %d", ErrStale, kind, id, version, existing.version)
	}
	if version == existing.version {
		if amount == existing.amount {
			return false, nil // identical replay
		}
		return false, fmt.Errorf("%w: %s %s version %d changed amount without a new version", ErrStale, kind, id, version)
	}
	if amount < existing.confirmedAmount {
		return false, fmt.Errorf("%w: cannot lower %s %s to %d below confirmed amount %d", ErrConfirmedDocument, kind, id, amount, existing.confirmedAmount)
	}
	status := statusFor(amount, existing.confirmedAmount)
	if _, err := s.drv.Exec(`UPDATE documents SET amount = ?, version = ?, balance = ?, status = ? WHERE kind = ? AND id = ?`,
		amount, version, amount-existing.confirmedAmount, status, kind, id); err != nil {
		return false, err
	}
	return true, nil
}

type documentRow struct {
	kind            string
	id              string
	amount          int64
	version         int64
	confirmedAmount int64
	balance         int64
	status          string
}

func (s *Store) getDocumentRow(kind, id string) (documentRow, bool, error) {
	_, rows, err := s.drv.Query(`SELECT kind, id, amount, version, confirmed_amount, balance, status FROM documents WHERE kind = ? AND id = ?`, kind, id)
	if err != nil {
		return documentRow{}, false, err
	}
	if len(rows) == 0 {
		return documentRow{}, false, nil
	}
	r := rows[0]
	return documentRow{
		kind:            asString(r[0]),
		id:              asString(r[1]),
		amount:          asInt(r[2]),
		version:         asInt(r[3]),
		confirmedAmount: asInt(r[4]),
		balance:         asInt(r[5]),
		status:          asString(r[6]),
	}, true, nil
}

// DeleteInvoice removes an invoice that has no confirmed write-off details.
func (s *Store) DeleteInvoice(id string) error { return s.deleteDocument(KindInvoice, id) }

// DeletePayment removes a payment that has no confirmed write-off details.
func (s *Store) DeletePayment(id string) error { return s.deleteDocument(KindPayment, id) }

func (s *Store) deleteDocument(kind, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.transaction(func() error {
		existing, found, err := s.getDocumentRow(kind, id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: %s %s", ErrNotFound, kind, id)
		}
		if existing.confirmedAmount > 0 {
			return fmt.Errorf("%w: cannot delete %s %s with confirmed amount %d", ErrConfirmedDocument, kind, id, existing.confirmedAmount)
		}
		if _, err := s.drv.Exec(`DELETE FROM documents WHERE kind = ? AND id = ?`, kind, id); err != nil {
			return err
		}
		// Removing a document invalidates the pairs that reference it.
		if kind == KindInvoice {
			if _, err := s.drv.Exec(`DELETE FROM allowed_pairs WHERE invoice_id = ?`, id); err != nil {
				return err
			}
		} else {
			if _, err := s.drv.Exec(`DELETE FROM allowed_pairs WHERE payment_id = ?`, id); err != nil {
				return err
			}
		}
		if err := s.bumpSourceVersion(); err != nil {
			return err
		}
		return s.recomputePlan()
	})
}

// PutAllowedPair inserts or updates an allowed invoice-to-payment edge.
func (s *Store) PutAllowedPair(pair AllowedPair) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := s.validatePair(pair); err != nil {
		return err
	}
	return s.transaction(func() error {
		changed, err := s.upsertPair(pair)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		if err := s.bumpSourceVersion(); err != nil {
			return err
		}
		return s.recomputePlan()
	})
}

func (s *Store) validatePair(pair AllowedPair) error {
	if strings.TrimSpace(pair.InvoiceID) == "" || strings.TrimSpace(pair.PaymentID) == "" {
		return fmt.Errorf("%w: allowed pair requires invoice and payment identifiers", ErrInvalid)
	}
	if pair.Customer != s.customer || pair.Currency != s.currency {
		return fmt.Errorf("%w: allowed pair has partition %s/%s, store is %s/%s", ErrInvalid, pair.Customer, pair.Currency, s.customer, s.currency)
	}
	if pair.UnitCost < 0 || pair.MaxAmount < 0 {
		return fmt.Errorf("%w: allowed pair has negative cost or cap", ErrInvalid)
	}
	return nil
}

// upsertPair inserts or updates an edge and reports whether anything changed.
func (s *Store) upsertPair(pair AllowedPair) (bool, error) {
	if _, found, err := s.getDocumentRow(KindInvoice, pair.InvoiceID); err != nil {
		return false, err
	} else if !found {
		return false, fmt.Errorf("%w: invoice %s", ErrNotFound, pair.InvoiceID)
	}
	if _, found, err := s.getDocumentRow(KindPayment, pair.PaymentID); err != nil {
		return false, err
	} else if !found {
		return false, fmt.Errorf("%w: payment %s", ErrNotFound, pair.PaymentID)
	}
	_, rows, err := s.drv.Query(`SELECT unit_cost, max_amount FROM allowed_pairs WHERE invoice_id = ? AND payment_id = ?`, pair.InvoiceID, pair.PaymentID)
	if err != nil {
		return false, err
	}
	if len(rows) == 1 && asInt(rows[0][0]) == pair.UnitCost && asInt(rows[0][1]) == pair.MaxAmount {
		return false, nil
	}
	_, err = s.drv.Exec(`INSERT INTO allowed_pairs (invoice_id, payment_id, customer, currency, unit_cost, max_amount)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(invoice_id, payment_id) DO UPDATE SET unit_cost = excluded.unit_cost, max_amount = excluded.max_amount`,
		pair.InvoiceID, pair.PaymentID, s.customer, s.currency, pair.UnitCost, pair.MaxAmount)
	if err != nil {
		return false, err
	}
	return true, nil
}

// DeleteAllowedPair removes an allowed edge. Already confirmed allocations are
// unaffected because they are immutable details; only future planning changes.
func (s *Store) DeleteAllowedPair(invoiceID, paymentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.transaction(func() error {
		n, err := s.drv.Exec(`DELETE FROM allowed_pairs WHERE invoice_id = ? AND payment_id = ?`, invoiceID, paymentID)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: allowed pair %s->%s", ErrNotFound, invoiceID, paymentID)
		}
		if err := s.bumpSourceVersion(); err != nil {
			return err
		}
		return s.recomputePlan()
	})
}

func (s *Store) bumpSourceVersion() error {
	// The increment happens inside the caller's transaction.
	_, err := s.drv.Exec(`UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = ?`, metaSourceVersion)
	return err
}

func statusFor(amount, confirmed int64) string {
	switch {
	case confirmed <= 0:
		return StatusOpen
	case confirmed >= amount:
		return StatusReconciled
	default:
		return StatusPartiallyReconciled
	}
}

// recomputePlan replaces the current unconfirmed plan with one computed from
// the balances net of confirmed amounts and the remaining allowed-edge
// capacity. Must run inside a write transaction.
func (s *Store) recomputePlan() error {
	sourceVersion, err := s.metaInt(metaSourceVersion)
	if err != nil {
		return err
	}
	generation, err := s.metaInt(metaPlanGen)
	if err != nil {
		return err
	}
	generation++

	invoiceBalances, err := s.balances(KindInvoice)
	if err != nil {
		return err
	}
	paymentBalances, err := s.balances(KindPayment)
	if err != nil {
		return err
	}
	edges, err := s.remainingEdges()
	if err != nil {
		return err
	}
	allocations, totalAllocated, totalCost, err := Optimize(invoiceBalances, paymentBalances, edges)
	if err != nil {
		return err
	}

	if _, err := s.drv.Exec(`DELETE FROM plan_items`); err != nil {
		return err
	}
	for _, a := range allocations {
		if _, err := s.drv.Exec(`INSERT INTO plan_items (generation, invoice_id, payment_id, amount, unit_cost, cost) VALUES (?, ?, ?, ?, ?, ?)`,
			generation, a.InvoiceID, a.PaymentID, a.Amount, a.UnitCost, a.Cost); err != nil {
			return err
		}
	}
	if _, err := s.drv.Exec(`INSERT INTO plan_state (id, generation, source_version, total_allocated, total_cost)
		VALUES (1, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET generation = excluded.generation, source_version = excluded.source_version,
			total_allocated = excluded.total_allocated, total_cost = excluded.total_cost`,
		generation, sourceVersion, totalAllocated, totalCost); err != nil {
		return err
	}
	return s.metaSet(metaPlanGen, fmt.Sprintf("%d", generation))
}

func (s *Store) balances(kind string) ([]Balance, error) {
	_, rows, err := s.drv.Query(`SELECT id, balance FROM documents WHERE kind = ? AND balance > 0 ORDER BY id`, kind)
	if err != nil {
		return nil, err
	}
	out := make([]Balance, 0, len(rows))
	for _, r := range rows {
		out = append(out, Balance{ID: asString(r[0]), Remaining: asInt(r[1])})
	}
	return out, nil
}

func (s *Store) remainingEdges() ([]Edge, error) {
	_, rows, err := s.drv.Query(`SELECT p.invoice_id, p.payment_id, p.unit_cost, p.max_amount,
			COALESCE((SELECT SUM(a.amount) FROM allocations a WHERE a.invoice_id = p.invoice_id AND a.payment_id = p.payment_id), 0)
		FROM allowed_pairs p ORDER BY p.invoice_id, p.payment_id`)
	if err != nil {
		return nil, err
	}
	out := make([]Edge, 0, len(rows))
	for _, r := range rows {
		maxAmount := asInt(r[3])
		used := asInt(r[4])
		if maxAmount > 0 {
			remaining := maxAmount - used
			if remaining <= 0 {
				continue
			}
			maxAmount = remaining
		}
		out = append(out, Edge{
			InvoiceID: asString(r[0]),
			PaymentID: asString(r[1]),
			UnitCost:  asInt(r[2]),
			MaxAmount: maxAmount,
		})
	}
	return out, nil
}

// ensureCurrentPlan computes a plan when persisted state has none, or when the
// plan predates the stored source version.
func (s *Store) ensureCurrentPlan() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.transaction(func() error {
		sourceVersion, err := s.metaInt(metaSourceVersion)
		if err != nil {
			return err
		}
		var planSource int64
		_, rows, err := s.drv.Query(`SELECT source_version FROM plan_state WHERE id = 1`)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			planSource = asInt(rows[0][0])
		}
		if planSource == sourceVersion {
			return nil
		}
		return s.recomputePlan()
	})
}

// Plan returns the current unconfirmed plan.
func (s *Store) Plan() (Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return Plan{}, err
	}
	return s.loadPlan()
}

func (s *Store) loadPlan() (Plan, error) {
	var plan Plan
	_, rows, err := s.drv.Query(`SELECT generation, source_version, total_allocated, total_cost FROM plan_state WHERE id = 1`)
	if err != nil {
		return plan, err
	}
	if len(rows) == 0 {
		return plan, nil
	}
	plan.Generation = asInt(rows[0][0])
	plan.SourceVersion = asInt(rows[0][1])
	plan.TotalAllocated = asInt(rows[0][2])
	plan.TotalCost = asInt(rows[0][3])
	items, err := s.planItems(plan.Generation)
	if err != nil {
		return plan, err
	}
	plan.Items = items
	return plan, nil
}

func (s *Store) planItems(generation int64) ([]PlanItem, error) {
	_, rows, err := s.drv.Query(`SELECT id, generation, invoice_id, payment_id, amount, unit_cost, cost
		FROM plan_items WHERE generation = ? ORDER BY invoice_id, payment_id`, generation)
	if err != nil {
		return nil, err
	}
	out := make([]PlanItem, 0, len(rows))
	for _, r := range rows {
		out = append(out, PlanItem{
			ID:         asInt(r[0]),
			Generation: asInt(r[1]),
			InvoiceID:  asString(r[2]),
			PaymentID:  asString(r[3]),
			Amount:     asInt(r[4]),
			UnitCost:   asInt(r[5]),
			Cost:       asInt(r[6]),
		})
	}
	return out, nil
}

func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asInt(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	default:
		return 0
	}
}

// Confirm binds a confirmation request to the current plan generation and
// source version, persists the selected write-off details, and recomputes the
// plan from the remaining balances. Progress and version counters are written
// in the same transaction, so a failure leaves the prior state untouched and a
// reopen continues from the committed state.
func (s *Store) Confirm(req ConfirmationRequest) (ConfirmationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return ConfirmationResult{}, err
	}
	digest, err := requestDigest(req)
	if err != nil {
		return ConfirmationResult{}, err
	}
	key := strings.TrimSpace(req.IdempotencyKey)
	if key == "" {
		key = "auto:" + digest
	}

	var result ConfirmationResult
	err = s.transaction(func() error {
		// Idempotent replay wins over staleness: a retry of an already applied
		// request returns the stored result even after state advanced.
		_, storedDigest, storedJSON, found, err := s.lookupConfirmation(key)
		if err != nil {
			return err
		}
		if found {
			if storedDigest != digest {
				return fmt.Errorf("%w: key %s", ErrIdempotencyConflict, key)
			}
			if err := json.Unmarshal([]byte(storedJSON), &result); err != nil {
				return fmt.Errorf("%w: corrupt stored confirmation: %v", ErrInvalid, err)
			}
			result.Idempotent = true
			return nil
		}

		sourceVersion, err := s.metaInt(metaSourceVersion)
		if err != nil {
			return err
		}
		plan, err := s.loadPlan()
		if err != nil {
			return err
		}
		if req.PlanGeneration != plan.Generation || req.SourceVersion != sourceVersion || plan.SourceVersion != sourceVersion {
			return fmt.Errorf("%w: request plan %d/source %d, current plan %d/source %d", ErrStale, req.PlanGeneration, req.SourceVersion, plan.Generation, sourceVersion)
		}

		selected, err := selectConfirmations(plan, req.Items)
		if err != nil {
			return err
		}

		// Validate against the plan, remaining balances, and remaining edge
		// capacity before writing anything.
		documents, err := s.loadDocuments()
		if err != nil {
			return err
		}
		if err := validateSelection(selected, documents); err != nil {
			return err
		}

		var confirmedTotal int64
		var allocations []Allocation
		for _, item := range selected {
			inv := documents[[2]string{KindInvoice, item.InvoiceID}]
			pay := documents[[2]string{KindPayment, item.PaymentID}]
			cost, err := safeMul(item.Amount, item.UnitCost)
			if err != nil {
				return err
			}
			if _, err := s.drv.Exec(`INSERT INTO allocations (invoice_id, payment_id, amount, unit_cost, cost, plan_generation, invoice_version, payment_version, status)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				item.InvoiceID, item.PaymentID, item.Amount, item.UnitCost, cost, plan.Generation, inv.version, pay.version, StatusConfirmed); err != nil {
				return err
			}
			id, err := s.drv.LastInsertID()
			if err != nil {
				return err
			}
			allocations = append(allocations, Allocation{
				ID:             id,
				InvoiceID:      item.InvoiceID,
				PaymentID:      item.PaymentID,
				Amount:         item.Amount,
				UnitCost:       item.UnitCost,
				Cost:           cost,
				PlanGeneration: plan.Generation,
				InvoiceVersion: inv.version,
				PaymentVersion: pay.version,
				Status:         StatusConfirmed,
			})
			if err := s.applyConfirmed(item.InvoiceID, KindInvoice, item.Amount); err != nil {
				return err
			}
			if err := s.applyConfirmed(item.PaymentID, KindPayment, item.Amount); err != nil {
				return err
			}
			if err := s.enforceEdgeCapacity(item); err != nil {
				return err
			}
			confirmedTotal, err = safeAdd(confirmedTotal, item.Amount)
			if err != nil {
				return err
			}
		}

		newGeneration, err := s.recomputePlanTx()
		if err != nil {
			return err
		}
		newPlan, err := s.loadPlan()
		if err != nil {
			return err
		}
		result = ConfirmationResult{
			Idempotent:        false,
			Allocations:       allocations,
			SourceVersion:     sourceVersion,
			PlanGeneration:    plan.Generation,
			NewPlanGeneration: newGeneration,
			TotalConfirmed:    confirmedTotal,
			TotalPlanned:      newPlan.TotalAllocated,
			TotalPlannedCost:  newPlan.TotalCost,
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if _, err := s.drv.Exec(`INSERT INTO confirmations (key, digest, result_json, created_at) VALUES (?, ?, ?, ?)`,
			key, digest, string(encoded), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return ConfirmationResult{}, err
	}
	return result, nil
}

// applyConfirmed adds one allocation amount to a document's confirmed amount
// and updates its balance and status in the same statement.
func (s *Store) applyConfirmed(id, kind string, amount int64) error {
	changed, err := s.drv.Exec(`UPDATE documents
		SET confirmed_amount = confirmed_amount + ?,
		    balance = amount - (confirmed_amount + ?),
		    status = CASE WHEN confirmed_amount + ? >= amount THEN ? ELSE ? END
		WHERE kind = ? AND id = ? AND confirmed_amount + ? <= amount`,
		amount, amount, amount, StatusReconciled, StatusPartiallyReconciled, kind, id, amount)
	if err != nil {
		return err
	}
	if changed != 1 {
		// The guard in the WHERE clause rejected the update because the
		// remaining balance was insufficient.
		return fmt.Errorf("%w: %s %s has insufficient remaining balance for %d", ErrOverAllocate, kind, id, amount)
	}
	return nil
}

func (s *Store) recomputePlanTx() (int64, error) {
	if err := s.recomputePlan(); err != nil {
		return 0, err
	}
	return s.metaInt(metaPlanGen)
}

func (s *Store) lookupConfirmation(key string) (string, string, string, bool, error) {
	_, rows, err := s.drv.Query(`SELECT key, digest, result_json FROM confirmations WHERE key = ?`, key)
	if err != nil {
		return "", "", "", false, err
	}
	if len(rows) == 0 {
		return "", "", "", false, nil
	}
	return asString(rows[0][0]), asString(rows[0][1]), asString(rows[0][2]), true, nil
}

// enforceEdgeCapacity checks that confirmed allocations on one pair never
// exceed its max amount.
func (s *Store) enforceEdgeCapacity(item confirmSelection) error {
	_, rows, err := s.drv.Query(`SELECT p.max_amount,
			COALESCE((SELECT SUM(a.amount) FROM allocations a WHERE a.invoice_id = p.invoice_id AND a.payment_id = p.payment_id), 0)
		FROM allowed_pairs p WHERE p.invoice_id = ? AND p.payment_id = ?`, item.InvoiceID, item.PaymentID)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("%w: allowed pair %s->%s disappeared", ErrStale, item.InvoiceID, item.PaymentID)
	}
	maxAmount := asInt(rows[0][0])
	used := asInt(rows[0][1])
	if maxAmount > 0 && used > maxAmount {
		return fmt.Errorf("%w: pair %s->%s used %d of cap %d", ErrOverAllocate, item.InvoiceID, item.PaymentID, used, maxAmount)
	}
	return nil
}

type documentState struct {
	amount          int64
	version         int64
	confirmedAmount int64
	balance         int64
}

func (s *Store) loadDocuments() (map[[2]string]documentState, error) {
	_, rows, err := s.drv.Query(`SELECT kind, id, amount, version, confirmed_amount, balance FROM documents`)
	if err != nil {
		return nil, err
	}
	out := make(map[[2]string]documentState, len(rows))
	for _, r := range rows {
		out[[2]string{asString(r[0]), asString(r[1])}] = documentState{
			amount:          asInt(r[2]),
			version:         asInt(r[3]),
			confirmedAmount: asInt(r[4]),
			balance:         asInt(r[5]),
		}
	}
	return out, nil
}

type confirmSelection struct {
	InvoiceID string
	PaymentID string
	Amount    int64
	UnitCost  int64
}

func selectConfirmations(plan Plan, items []ConfirmItem) ([]confirmSelection, error) {
	if len(items) == 0 {
		out := make([]confirmSelection, 0, len(plan.Items))
		for _, it := range plan.Items {
			out = append(out, confirmSelection{InvoiceID: it.InvoiceID, PaymentID: it.PaymentID, Amount: it.Amount, UnitCost: it.UnitCost})
		}
		return out, nil
	}
	byPair := make(map[[2]string]PlanItem, len(plan.Items))
	for _, it := range plan.Items {
		byPair[[2]string{it.InvoiceID, it.PaymentID}] = it
	}
	seen := make(map[[2]string]struct{}, len(items))
	out := make([]confirmSelection, 0, len(items))
	for _, req := range items {
		if req.Amount <= 0 {
			return nil, fmt.Errorf("%w: confirmation amount must be positive for %s->%s", ErrInvalid, req.InvoiceID, req.PaymentID)
		}
		key := [2]string{req.InvoiceID, req.PaymentID}
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("%w: duplicate confirmation item %s->%s", ErrInvalid, req.InvoiceID, req.PaymentID)
		}
		seen[key] = struct{}{}
		planItem, ok := byPair[key]
		if !ok {
			return nil, fmt.Errorf("%w: %s->%s is not in plan generation %d", ErrStale, req.InvoiceID, req.PaymentID, plan.Generation)
		}
		if req.Amount > planItem.Amount {
			return nil, fmt.Errorf("%w: %s->%s amount %d exceeds planned %d", ErrOverAllocate, req.InvoiceID, req.PaymentID, req.Amount, planItem.Amount)
		}
		out = append(out, confirmSelection{InvoiceID: req.InvoiceID, PaymentID: req.PaymentID, Amount: req.Amount, UnitCost: planItem.UnitCost})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].InvoiceID != out[j].InvoiceID {
			return out[i].InvoiceID < out[j].InvoiceID
		}
		return out[i].PaymentID < out[j].PaymentID
	})
	return out, nil
}

func validateSelection(selected []confirmSelection, docs map[[2]string]documentState) error {
	needInv := make(map[string]int64)
	needPay := make(map[string]int64)
	for _, item := range selected {
		inv, ok := docs[[2]string{KindInvoice, item.InvoiceID}]
		if !ok {
			return fmt.Errorf("%w: invoice %s", ErrNotFound, item.InvoiceID)
		}
		pay, ok := docs[[2]string{KindPayment, item.PaymentID}]
		if !ok {
			return fmt.Errorf("%w: payment %s", ErrNotFound, item.PaymentID)
		}
		needInv[item.InvoiceID] += item.Amount
		needPay[item.PaymentID] += item.Amount
		if needInv[item.InvoiceID] > inv.balance {
			return fmt.Errorf("%w: invoice %s requested %d over remaining %d", ErrOverAllocate, item.InvoiceID, needInv[item.InvoiceID], inv.balance)
		}
		if needPay[item.PaymentID] > pay.balance {
			return fmt.Errorf("%w: payment %s requested %d over remaining %d", ErrOverAllocate, item.PaymentID, needPay[item.PaymentID], pay.balance)
		}
	}
	return nil
}

// digestItem is the canonical, JSON-tagged shape of one confirmation item. It
// mirrors ConfirmItem so a request digest is stable across processes.
type digestItem struct {
	InvoiceID string `json:"invoiceId"`
	PaymentID string `json:"paymentId"`
	Amount    int64  `json:"amount"`
}

func requestDigest(req ConfirmationRequest) (string, error) {
	items := make([]digestItem, 0, len(req.Items))
	for _, it := range req.Items {
		items = append(items, digestItem(it))
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].InvoiceID != items[j].InvoiceID {
			return items[i].InvoiceID < items[j].InvoiceID
		}
		return items[i].PaymentID < items[j].PaymentID
	})
	payload, err := json.Marshal(struct {
		PlanGeneration int64        `json:"planGeneration"`
		SourceVersion  int64        `json:"sourceVersion"`
		Items          []digestItem `json:"items"`
	}{req.PlanGeneration, req.SourceVersion, items})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// Result returns a complete consistent snapshot: both sides' documents with
// balances and statuses, every confirmed write-off detail, and the current
// unconfirmed plan. It takes a read transaction so the snapshot cannot
// interleave with a concurrent write.
func (s *Store) Result() (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return Result{}, err
	}
	if err := s.drv.Begin(true); err != nil {
		return Result{}, err
	}
	defer func() { _ = s.drv.Rollback() }()

	res := Result{Customer: s.customer, Currency: s.currency}
	sourceVersion, err := s.metaInt(metaSourceVersion)
	if err != nil {
		return Result{}, err
	}
	res.SourceVersion = sourceVersion
	docs, err := s.allDocuments()
	if err != nil {
		return Result{}, err
	}
	for _, d := range docs {
		if d.Kind == KindInvoice {
			res.Invoices = append(res.Invoices, d)
			res.TotalInvoiceAmount, err = safeAdd(res.TotalInvoiceAmount, d.Amount)
			if err != nil {
				return Result{}, err
			}
		} else {
			res.Payments = append(res.Payments, d)
			res.TotalPaymentAmount, err = safeAdd(res.TotalPaymentAmount, d.Amount)
			if err != nil {
				return Result{}, err
			}
		}
	}
	plan, err := s.loadPlan()
	if err != nil {
		return Result{}, err
	}
	res.PlanGeneration = plan.Generation
	res.PlanItems = plan.Items
	res.TotalPlanned = plan.TotalAllocated
	res.TotalPlannedCost = plan.TotalCost
	allocations, totalConfirmed, err := s.allAllocations()
	if err != nil {
		return Result{}, err
	}
	res.Allocations = allocations
	res.TotalConfirmed = totalConfirmed
	return res, nil
}

func (s *Store) allDocuments() ([]Document, error) {
	_, rows, err := s.drv.Query(`SELECT kind, id, customer, currency, amount, version, confirmed_amount, balance, status FROM documents ORDER BY kind, id`)
	if err != nil {
		return nil, err
	}
	out := make([]Document, 0, len(rows))
	for _, r := range rows {
		out = append(out, Document{
			Kind:            asString(r[0]),
			ID:              asString(r[1]),
			Customer:        asString(r[2]),
			Currency:        asString(r[3]),
			Amount:          asInt(r[4]),
			Version:         asInt(r[5]),
			ConfirmedAmount: asInt(r[6]),
			Balance:         asInt(r[7]),
			Status:          asString(r[8]),
		})
	}
	return out, nil
}

func (s *Store) allAllocations() ([]Allocation, int64, error) {
	_, rows, err := s.drv.Query(`SELECT id, invoice_id, payment_id, amount, unit_cost, cost, plan_generation, invoice_version, payment_version, status FROM allocations ORDER BY invoice_id, payment_id, id`)
	if err != nil {
		return nil, 0, err
	}
	out := make([]Allocation, 0, len(rows))
	var total int64
	for _, r := range rows {
		a := Allocation{
			ID:             asInt(r[0]),
			InvoiceID:      asString(r[1]),
			PaymentID:      asString(r[2]),
			Amount:         asInt(r[3]),
			UnitCost:       asInt(r[4]),
			Cost:           asInt(r[5]),
			PlanGeneration: asInt(r[6]),
			InvoiceVersion: asInt(r[7]),
			PaymentVersion: asInt(r[8]),
			Status:         asString(r[9]),
		}
		total, err = safeAdd(total, a.Amount)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, a)
	}
	return out, total, nil
}
