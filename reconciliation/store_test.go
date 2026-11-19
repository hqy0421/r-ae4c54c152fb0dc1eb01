// Copyright [2024] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package reconciliation_test

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2/reconciliation"
)

func openTestStore(t *testing.T) *reconciliation.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := reconciliation.Open(filepath.Join(dir, "recon.db"), testOptions())
	if err != nil {
		skipWithoutSQLite(t, err)
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// skipWithoutSQLite skips storage tests on builds without cgo or libsqlite3,
// where the package reports ErrCgoRequired.
func skipWithoutSQLite(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, reconciliation.ErrCgoRequired) {
		t.Skipf("SQLite storage unavailable in this build: %v", err)
	}
}

func testOptions() reconciliation.Options {
	return reconciliation.Options{Customer: "CUST-1", Currency: "USD", BusyTimeout: 2 * time.Second}
}

func putInvoice(t *testing.T, s *reconciliation.Store, id string, amount, version int64) {
	t.Helper()
	if err := s.PutInvoice(reconciliation.Invoice{ID: id, Customer: "CUST-1", Currency: "USD", Amount: amount, Version: version}); err != nil {
		t.Fatalf("PutInvoice %s v%d: %v", id, version, err)
	}
}

func putPayment(t *testing.T, s *reconciliation.Store, id string, amount, version int64) {
	t.Helper()
	if err := s.PutPayment(reconciliation.Payment{ID: id, Customer: "CUST-1", Currency: "USD", Amount: amount, Version: version}); err != nil {
		t.Fatalf("PutPayment %s v%d: %v", id, version, err)
	}
}

func putEdge(t *testing.T, s *reconciliation.Store, invoiceID, paymentID string, unitCost, maxAmount int64) {
	t.Helper()
	if err := s.PutAllowedPair(reconciliation.AllowedPair{
		InvoiceID: invoiceID, PaymentID: paymentID, Customer: "CUST-1", Currency: "USD",
		UnitCost: unitCost, MaxAmount: maxAmount,
	}); err != nil {
		t.Fatalf("PutAllowedPair %s->%s: %v", invoiceID, paymentID, err)
	}
}

func confirmAll(t *testing.T, s *reconciliation.Store) reconciliation.ConfirmationResult {
	t.Helper()
	plan, err := s.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	res, err := s.Confirm(reconciliation.ConfirmationRequest{
		PlanGeneration: plan.Generation,
		SourceVersion:  plan.SourceVersion,
	})
	if err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	return res
}

func TestStorePersistsAndReopens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recon.db")

	s, err := reconciliation.Open(path, testOptions())
	if err != nil {
		skipWithoutSQLite(t, err)
		t.Fatalf("Open: %v", err)
	}
	if err := s.PutInvoice(reconciliation.Invoice{ID: "INV-1", Customer: "CUST-1", Currency: "USD", Amount: 1000}); err != nil {
		t.Fatalf("PutInvoice: %v", err)
	}
	if err := s.PutPayment(reconciliation.Payment{ID: "PAY-1", Customer: "CUST-1", Currency: "USD", Amount: 600}); err != nil {
		t.Fatalf("PutPayment: %v", err)
	}
	if err := s.PutAllowedPair(reconciliation.AllowedPair{InvoiceID: "INV-1", PaymentID: "PAY-1", Customer: "CUST-1", Currency: "USD", UnitCost: 4}); err != nil {
		t.Fatalf("PutAllowedPair: %v", err)
	}
	plan, err := s.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.TotalAllocated != 600 || len(plan.Items) != 1 {
		t.Fatalf("plan = %+v, want 600 across one item", plan)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := reconciliation.Open(path, testOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	plan2, err := s2.Plan()
	if err != nil {
		t.Fatalf("Plan after reopen: %v", err)
	}
	if plan2.TotalAllocated != 600 || plan2.Generation != plan.Generation {
		t.Fatalf("reopened plan = %+v, want generation %d total 600", plan2, plan.Generation)
	}
	res, err := s2.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if len(res.Invoices) != 1 || res.Invoices[0].Balance != 1000 {
		t.Fatalf("invoices = %+v", res.Invoices)
	}
	if res.Customer != "CUST-1" || res.Currency != "USD" {
		t.Fatalf("partition = %s/%s", res.Customer, res.Currency)
	}
}

func TestConfirmWritesDetailsAndRecomputesPlan(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 1000, 0)
	putPayment(t, s, "PAY-1", 400, 0)
	putPayment(t, s, "PAY-2", 400, 0)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	putEdge(t, s, "INV-1", "PAY-2", 1, 0)

	plan, err := s.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.TotalAllocated != 800 {
		t.Fatalf("planned total = %d, want 800", plan.TotalAllocated)
	}
	sourceAtPlan := plan.SourceVersion
	genAtPlan := plan.Generation

	res, err := s.Confirm(reconciliation.ConfirmationRequest{
		PlanGeneration: genAtPlan,
		SourceVersion:  sourceAtPlan,
		Items: []reconciliation.ConfirmItem{
			{InvoiceID: "INV-1", PaymentID: "PAY-1", Amount: 400},
		},
	})
	if err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	if res.TotalConfirmed != 400 || res.Idempotent {
		t.Fatalf("result = %+v", res)
	}
	if len(res.Allocations) != 1 || res.Allocations[0].InvoiceVersion != 1 || res.Allocations[0].PaymentVersion != 1 {
		t.Fatalf("allocations = %+v", res.Allocations)
	}
	if res.Allocations[0].PlanGeneration != genAtPlan {
		t.Fatalf("allocation generation = %d, want %d", res.Allocations[0].PlanGeneration, genAtPlan)
	}

	// The unconfirmed plan is recomputed from remaining balances only: the
	// confirmed 400 on INV-1/PAY-1 is excluded, and PAY-1's own remaining
	// balance is now zero, so the plan is INV-1(600)->PAY-2(400).
	plan2, err := s.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan2.Generation <= genAtPlan {
		t.Fatalf("generation did not advance: %d -> %d", genAtPlan, plan2.Generation)
	}
	if plan2.TotalAllocated != 400 || len(plan2.Items) != 1 || plan2.Items[0].PaymentID != "PAY-2" {
		t.Fatalf("new plan = %+v, want 400 on PAY-2", plan2)
	}

	docs := loadResult(t, s)
	inv := findDoc(t, docs.Invoices, "INV-1")
	if inv.ConfirmedAmount != 400 || inv.Balance != 600 || inv.Status != reconciliation.StatusPartiallyReconciled {
		t.Fatalf("invoice state = %+v", inv)
	}
	pay1 := findDoc(t, docs.Payments, "PAY-1")
	if pay1.ConfirmedAmount != 400 || pay1.Balance != 0 || pay1.Status != reconciliation.StatusReconciled {
		t.Fatalf("payment state = %+v", pay1)
	}
}

func TestConfirmedDetailsNeverReallocated(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 100, 0)
	putInvoice(t, s, "INV-2", 100, 0)
	putPayment(t, s, "PAY-1", 100, 0)
	putEdge(t, s, "INV-1", "PAY-1", 2, 0)
	putEdge(t, s, "INV-2", "PAY-1", 1, 0)

	// First plan prefers the cheaper edge (INV-2, cost 1).
	plan, _ := s.Plan()
	if len(plan.Items) != 1 || plan.Items[0].InvoiceID != "INV-2" {
		t.Fatalf("initial plan = %+v, want INV-2", plan)
	}
	confirmAll(t, s)

	// A correction makes the previously cheaper edge more expensive. Confirmed
	// INV-2->PAY-1 must stay put; the recomputed plan must not move it.
	putEdge(t, s, "INV-1", "PAY-1", 0, 0)
	putEdge(t, s, "INV-2", "PAY-1", 9, 0)

	res, err := s.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if len(res.Allocations) != 1 || res.Allocations[0].InvoiceID != "INV-2" || res.Allocations[0].Amount != 100 {
		t.Fatalf("confirmed allocations changed: %+v", res.Allocations)
	}
	if res.TotalPlanned != 0 {
		t.Fatalf("planned after full confirmation = %d, want 0", res.TotalPlanned)
	}
}

func TestSourceCorrectionRules(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 500, 1)
	putPayment(t, s, "PAY-1", 300, 1)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	confirmAll(t, s)

	// Cannot lower the invoice below its confirmed amount.
	err := s.PutInvoice(reconciliation.Invoice{ID: "INV-1", Customer: "CUST-1", Currency: "USD", Amount: 200, Version: 2})
	if !errors.Is(err, reconciliation.ErrConfirmedDocument) {
		t.Fatalf("lower below confirmed err = %v, want ErrConfirmedDocument", err)
	}
	// Cannot delete a document with confirmed details.
	if err := s.DeleteInvoice("INV-1"); !errors.Is(err, reconciliation.ErrConfirmedDocument) {
		t.Fatalf("delete confirmed invoice err = %v, want ErrConfirmedDocument", err)
	}
	// A stale version is rejected.
	err = s.PutInvoice(reconciliation.Invoice{ID: "INV-1", Customer: "CUST-1", Currency: "USD", Amount: 600, Version: 1})
	if !errors.Is(err, reconciliation.ErrStale) {
		t.Fatalf("same-version change err = %v, want ErrStale", err)
	}
	err = s.PutInvoice(reconciliation.Invoice{ID: "INV-1", Customer: "CUST-1", Currency: "USD", Amount: 600, Version: 0})
	if !errors.Is(err, reconciliation.ErrInvalid) && !errors.Is(err, reconciliation.ErrStale) {
		t.Fatalf("version 0 change err = %v, want rejection", err)
	}

	// Raising the invoice with a new version is allowed and leaves confirmed
	// details untouched.
	putInvoice(t, s, "INV-1", 600, 2)
	res, err := s.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	inv := findDoc(t, res.Invoices, "INV-1")
	if inv.Amount != 600 || inv.ConfirmedAmount != 300 || inv.Balance != 300 {
		t.Fatalf("invoice after raise = %+v", inv)
	}
	if len(res.Allocations) != 1 || res.Allocations[0].Amount != 300 {
		t.Fatalf("confirmed details disturbed: %+v", res.Allocations)
	}

	// Unconfirmed documents can be deleted.
	putInvoice(t, s, "INV-2", 50, 1)
	if err := s.DeleteInvoice("INV-2"); err != nil {
		t.Fatalf("delete unconfirmed invoice: %v", err)
	}
}

func TestConfirmStalenessAndIdempotency(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 1000, 0)
	putPayment(t, s, "PAY-1", 1000, 0)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)

	plan, _ := s.Plan()
	staleGen := plan.Generation
	staleSource := plan.SourceVersion

	// Advancing the source invalidates the old plan generation.
	putPayment(t, s, "PAY-2", 10, 0)
	if _, err := s.Confirm(reconciliation.ConfirmationRequest{PlanGeneration: staleGen, SourceVersion: staleSource}); !errors.Is(err, reconciliation.ErrStale) {
		t.Fatalf("stale confirm err = %v, want ErrStale", err)
	}

	plan, _ = s.Plan()
	req := reconciliation.ConfirmationRequest{
		IdempotencyKey: "confirm-1",
		PlanGeneration: plan.Generation,
		SourceVersion:  plan.SourceVersion,
		Items:          []reconciliation.ConfirmItem{{InvoiceID: "INV-1", PaymentID: "PAY-1", Amount: 250}},
	}
	first, err := s.Confirm(req)
	if err != nil {
		t.Fatalf("first confirm: %v", err)
	}
	if first.Idempotent {
		t.Fatalf("first confirm reported idempotent")
	}

	// Replaying the same request after state advanced still returns the stored
	// result and applies nothing twice.
	second, err := s.Confirm(req)
	if err != nil {
		t.Fatalf("replay confirm: %v", err)
	}
	if !second.Idempotent {
		t.Fatalf("replay not idempotent")
	}
	if second.TotalConfirmed != first.TotalConfirmed || len(second.Allocations) != len(first.Allocations) {
		t.Fatalf("replay differs: %+v vs %+v", second, first)
	}
	third, err := s.Confirm(req)
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if !third.Idempotent || third.NewPlanGeneration != first.NewPlanGeneration {
		t.Fatalf("third replay = %+v", third)
	}

	res, _ := s.Result()
	if res.TotalConfirmed != 250 {
		t.Fatalf("total confirmed = %d, want 250 (no double apply)", res.TotalConfirmed)
	}
	if len(res.Allocations) != 1 {
		t.Fatalf("allocations = %+v, want exactly one", res.Allocations)
	}

	// The same key with a different payload is a conflict.
	plan, _ = s.Plan()
	conflicting := req
	conflicting.Items = []reconciliation.ConfirmItem{{InvoiceID: "INV-1", PaymentID: "PAY-1", Amount: 300}}
	conflicting.PlanGeneration = plan.Generation
	conflicting.SourceVersion = plan.SourceVersion
	if _, err := s.Confirm(conflicting); !errors.Is(err, reconciliation.ErrIdempotencyConflict) {
		t.Fatalf("conflicting key err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestConfirmWithoutKeyIsIdempotentForIdenticalRequest(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 100, 0)
	putPayment(t, s, "PAY-1", 100, 0)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	plan, _ := s.Plan()
	req := reconciliation.ConfirmationRequest{PlanGeneration: plan.Generation, SourceVersion: plan.SourceVersion}
	if _, err := s.Confirm(req); err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	again, err := s.Confirm(req)
	if err != nil {
		t.Fatalf("repeat Confirm: %v", err)
	}
	if !again.Idempotent {
		t.Fatalf("identical request without key not treated as idempotent")
	}
	res, _ := s.Result()
	if res.TotalConfirmed != 100 {
		t.Fatalf("total confirmed = %d, want 100", res.TotalConfirmed)
	}
}

func TestConfirmCannotOverAllocate(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 100, 0)
	putPayment(t, s, "PAY-1", 100, 0)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	plan, _ := s.Plan()

	// More than the plan item.
	_, err := s.Confirm(reconciliation.ConfirmationRequest{
		PlanGeneration: plan.Generation, SourceVersion: plan.SourceVersion,
		Items: []reconciliation.ConfirmItem{{InvoiceID: "INV-1", PaymentID: "PAY-1", Amount: 101}},
	})
	if !errors.Is(err, reconciliation.ErrOverAllocate) {
		t.Fatalf("over plan item err = %v, want ErrOverAllocate", err)
	}

	// Two items on the same pair cannot jointly exceed the plan amount.
	_, err = s.Confirm(reconciliation.ConfirmationRequest{
		PlanGeneration: plan.Generation, SourceVersion: plan.SourceVersion,
		Items: []reconciliation.ConfirmItem{
			{InvoiceID: "INV-1", PaymentID: "PAY-1", Amount: 60},
			{InvoiceID: "INV-1", PaymentID: "PAY-1", Amount: 60},
		},
	})
	if err == nil {
		t.Fatalf("duplicate pair confirmation accepted")
	}

	// A pair not permitted by the plan is rejected.
	_, err = s.Confirm(reconciliation.ConfirmationRequest{
		PlanGeneration: plan.Generation, SourceVersion: plan.SourceVersion,
		Items: []reconciliation.ConfirmItem{{InvoiceID: "INV-1", PaymentID: "PAY-X", Amount: 1}},
	})
	if !errors.Is(err, reconciliation.ErrStale) {
		t.Fatalf("unknown pair err = %v, want ErrStale", err)
	}
}

func TestFailureLeavesPreviousStateIntact(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 100, 0)
	putPayment(t, s, "PAY-1", 100, 0)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	plan, _ := s.Plan()
	before, _ := s.Result()

	// An invalid source correction must not advance the source version or the
	// plan; the transaction rolls back. Version 1 already exists, so changing
	// the amount while keeping the version is rejected as stale.
	err := s.PutInvoice(reconciliation.Invoice{ID: "INV-1", Customer: "CUST-1", Currency: "USD", Amount: 999, Version: 1})
	if !errors.Is(err, reconciliation.ErrStale) {
		t.Fatalf("err = %v, want ErrStale", err)
	}
	after, _ := s.Result()
	if after.SourceVersion != before.SourceVersion || after.PlanGeneration != before.PlanGeneration {
		t.Fatalf("state advanced on failure: before=%+v after=%+v", before, after)
	}

	// A failed confirmation writes nothing.
	b, _ := s.Result()
	_, err = s.Confirm(reconciliation.ConfirmationRequest{PlanGeneration: plan.Generation + 99, SourceVersion: plan.SourceVersion})
	if !errors.Is(err, reconciliation.ErrStale) {
		t.Fatalf("err = %v, want ErrStale", err)
	}
	a, _ := s.Result()
	if a.TotalConfirmed != b.TotalConfirmed || len(a.Allocations) != len(b.Allocations) {
		t.Fatalf("failed confirm wrote details")
	}
}

func TestEdgeCapacityIsNotExceeded(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 100, 0)
	putPayment(t, s, "PAY-1", 100, 0)
	putEdge(t, s, "INV-1", "PAY-1", 1, 30)
	plan, _ := s.Plan()
	if plan.TotalAllocated != 30 {
		t.Fatalf("plan total = %d, want 30 (edge cap)", plan.TotalAllocated)
	}
	confirmAll(t, s)
	res, _ := s.Result()
	if res.TotalConfirmed != 30 {
		t.Fatalf("confirmed = %d, want 30", res.TotalConfirmed)
	}
	// Once the cap is consumed, the plan offers nothing more on that edge.
	plan2, _ := s.Plan()
	if plan2.TotalAllocated != 0 {
		t.Fatalf("plan after cap = %d, want 0", plan2.TotalAllocated)
	}
}

func TestImportBatchIsAtomic(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 100, 0)
	putPayment(t, s, "PAY-1", 100, 0)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	before := loadResult(t, s)

	// The second invoice belongs to another partition, so the whole batch must
	// be rejected and nothing written.
	err := s.Import(reconciliation.ImportBatch{
		Invoices: []reconciliation.Invoice{
			{ID: "INV-2", Customer: "CUST-1", Currency: "USD", Amount: 10},
			{ID: "INV-3", Customer: "OTHER", Currency: "USD", Amount: 20},
		},
	})
	if !errors.Is(err, reconciliation.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	after := loadResult(t, s)
	if len(after.Invoices) != len(before.Invoices) || after.SourceVersion != before.SourceVersion || after.PlanGeneration != before.PlanGeneration {
		t.Fatalf("batch partially applied: before=%+v after=%+v", before, after)
	}

	// A single stale version inside a batch also rolls back the whole batch.
	err = s.Import(reconciliation.ImportBatch{
		Invoices: []reconciliation.Invoice{{ID: "INV-4", Customer: "CUST-1", Currency: "USD", Amount: 50, Version: 1}},
		Payments: []reconciliation.Payment{{ID: "PAY-1", Customer: "CUST-1", Currency: "USD", Amount: 999, Version: 1}},
	})
	if !errors.Is(err, reconciliation.ErrStale) {
		t.Fatalf("err = %v, want ErrStale", err)
	}
	again := loadResult(t, s)
	if len(again.Invoices) != len(before.Invoices) {
		t.Fatalf("failed batch inserted documents: %+v", again.Invoices)
	}
}

func TestNoOpImportDoesNotInvalidatePlan(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 100, 1)
	putPayment(t, s, "PAY-1", 100, 1)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	before, _ := s.Plan()

	// Re-importing identical records changes nothing, so a plan confirmed
	// against the current generation remains valid.
	if err := s.Import(reconciliation.ImportBatch{
		Invoices:     []reconciliation.Invoice{{ID: "INV-1", Customer: "CUST-1", Currency: "USD", Amount: 100, Version: 1}},
		Payments:     []reconciliation.Payment{{ID: "PAY-1", Customer: "CUST-1", Currency: "USD", Amount: 100, Version: 1}},
		AllowedPairs: []reconciliation.AllowedPair{{InvoiceID: "INV-1", PaymentID: "PAY-1", Customer: "CUST-1", Currency: "USD", UnitCost: 1}},
	}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	after, _ := s.Plan()
	if after.Generation != before.Generation || after.SourceVersion != before.SourceVersion {
		t.Fatalf("no-op import bumped versions: before=%d/%d after=%d/%d", before.Generation, before.SourceVersion, after.Generation, after.SourceVersion)
	}
	if _, err := s.Confirm(reconciliation.ConfirmationRequest{PlanGeneration: before.Generation, SourceVersion: before.SourceVersion}); err != nil {
		t.Fatalf("confirm after no-op import: %v", err)
	}
}

func TestPartitionMismatchRejected(t *testing.T) {
	s := openTestStore(t)
	err := s.PutInvoice(reconciliation.Invoice{ID: "INV-9", Customer: "OTHER", Currency: "USD", Amount: 10})
	if !errors.Is(err, reconciliation.ErrInvalid) {
		t.Fatalf("customer mismatch err = %v, want ErrInvalid", err)
	}
	err = s.PutPayment(reconciliation.Payment{ID: "PAY-9", Customer: "CUST-1", Currency: "EUR", Amount: 10})
	if !errors.Is(err, reconciliation.ErrInvalid) {
		t.Fatalf("currency mismatch err = %v, want ErrInvalid", err)
	}
}

func TestOpenPartitionMismatchOnExistingDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recon.db")
	s, err := reconciliation.Open(path, testOptions())
	if err != nil {
		skipWithoutSQLite(t, err)
		t.Fatalf("Open: %v", err)
	}
	_ = s.Close()
	_, err = reconciliation.Open(path, reconciliation.Options{Customer: "CUST-2", Currency: "USD"})
	if !errors.Is(err, reconciliation.ErrConflict) {
		t.Fatalf("reopen other partition err = %v, want ErrConflict", err)
	}
}

func TestReopenContinuesAfterConfirmation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recon.db")
	s, err := reconciliation.Open(path, testOptions())
	if err != nil {
		skipWithoutSQLite(t, err)
		t.Fatalf("Open: %v", err)
	}
	putInvoice(t, s, "INV-1", 1000, 0)
	putPayment(t, s, "PAY-1", 1000, 0)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	plan, _ := s.Plan()
	res, err := s.Confirm(reconciliation.ConfirmationRequest{
		PlanGeneration: plan.Generation, SourceVersion: plan.SourceVersion,
		Items: []reconciliation.ConfirmItem{{InvoiceID: "INV-1", PaymentID: "PAY-1", Amount: 300}},
	})
	if err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: confirmed details, counters, and the recomputed plan survived.
	s2, err := reconciliation.Open(path, testOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	before := loadResult(t, s2)
	if before.TotalConfirmed != 300 || len(before.Allocations) != 1 {
		t.Fatalf("confirmed state after reopen = %+v", before)
	}
	if before.PlanGeneration != res.NewPlanGeneration {
		t.Fatalf("plan generation after reopen = %d, want %d", before.PlanGeneration, res.NewPlanGeneration)
	}

	// Continue the remaining balance from the reopened store.
	plan2, _ := s2.Plan()
	if plan2.TotalAllocated != 700 {
		t.Fatalf("remaining plan = %d, want 700", plan2.TotalAllocated)
	}
	res2, err := s2.Confirm(reconciliation.ConfirmationRequest{
		PlanGeneration: plan2.Generation, SourceVersion: plan2.SourceVersion,
	})
	if err != nil {
		t.Fatalf("second confirm: %v", err)
	}
	if res2.TotalConfirmed != 700 {
		t.Fatalf("second confirm total = %d, want 700", res2.TotalConfirmed)
	}
	after := loadResult(t, s2)
	if after.TotalConfirmed != 1000 || after.TotalPlanned != 0 {
		t.Fatalf("final state = confirmed %d planned %d, want 1000/0", after.TotalConfirmed, after.TotalPlanned)
	}
	inv := findDoc(t, after.Invoices, "INV-1")
	if inv.Status != reconciliation.StatusReconciled || inv.Balance != 0 {
		t.Fatalf("invoice final = %+v", inv)
	}
}

func TestConcurrentConfirmsDoNotDoubleApply(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 1000, 0)
	putPayment(t, s, "PAY-1", 1000, 0)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	plan, _ := s.Plan()

	const workers = 8
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			_, err := s.Confirm(reconciliation.ConfirmationRequest{
				IdempotencyKey: "same",
				PlanGeneration: plan.Generation,
				SourceVersion:  plan.SourceVersion,
				Items:          []reconciliation.ConfirmItem{{InvoiceID: "INV-1", PaymentID: "PAY-1", Amount: 111}},
			})
			errs <- err
		}()
	}
	for i := 0; i < workers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent confirm: %v", err)
		}
	}
	res, _ := s.Result()
	if res.TotalConfirmed != 111 || len(res.Allocations) != 1 {
		t.Fatalf("concurrent confirms applied more than once: confirmed=%d allocations=%d", res.TotalConfirmed, len(res.Allocations))
	}
}

func TestClosedStoreRejectsWrites(t *testing.T) {
	s := openTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.PutInvoice(reconciliation.Invoice{ID: "INV-1", Customer: "CUST-1", Currency: "USD", Amount: 1}); !errors.Is(err, reconciliation.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func loadResult(t *testing.T, s *reconciliation.Store) reconciliation.Result {
	t.Helper()
	res, err := s.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	return res
}

func findDoc(t *testing.T, docs []reconciliation.Document, id string) reconciliation.Document {
	t.Helper()
	for _, d := range docs {
		if d.ID == id {
			return d
		}
	}
	t.Fatalf("document %s not found in %+v", id, docs)
	return reconciliation.Document{}
}
