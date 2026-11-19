// Copyright [2024] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package reconciliation

import "errors"

// Document kinds. Invoices come from the receivables system; payments come
// from the cash receipts system.
const (
	KindInvoice = "invoice"
	KindPayment = "payment"
)

// Document status values.
const (
	// StatusOpen means the document has no confirmed write-off yet.
	StatusOpen = "open"
	// StatusPartiallyReconciled means some, but not all, of the document has
	// been confirmed as written off.
	StatusPartiallyReconciled = "partially_reconciled"
	// StatusReconciled means the whole document amount has been confirmed.
	StatusReconciled = "reconciled"
)

// Allocation status values. Only confirmed allocations are persisted as
// write-off details; unconfirmed allocations live in the current plan.
const (
	StatusConfirmed = "confirmed"
)

// Sentinel errors returned by this package. Callers should test errors with
// errors.Is because most returned errors wrap one of these values with
// additional detail.
var (
	// ErrStale reports that a plan generation, source version, or document
	// version no longer matches the persisted state. The caller must read the
	// latest plan and retry.
	ErrStale = errors.New("reconciliation: stale plan or source version")

	// ErrOverAllocate reports that confirming an allocation would exceed an
	// invoice balance, a payment balance, or an allowed-pair limit.
	ErrOverAllocate = errors.New("reconciliation: allocation exceeds available balance")

	// ErrConfirmedDocument reports an attempt to reduce a document below its
	// already confirmed amount, or to delete a document that has confirmed
	// write-off details.
	ErrConfirmedDocument = errors.New("reconciliation: document has confirmed allocations and is immutable")

	// ErrConflict reports a duplicate document or edge that cannot be inserted.
	ErrConflict = errors.New("reconciliation: duplicate or conflicting record")

	// ErrInvalid reports malformed input.
	ErrInvalid = errors.New("reconciliation: invalid input")

	// ErrNotFound reports a missing document or plan.
	ErrNotFound = errors.New("reconciliation: not found")

	// ErrIdempotencyConflict reports reuse of an idempotency key with a
	// different confirmation request.
	ErrIdempotencyConflict = errors.New("reconciliation: idempotency key reused with a different request")

	// ErrClosed reports use of a closed Store.
	ErrClosed = errors.New("reconciliation: store is closed")

	// ErrCgoRequired reports that SQLite storage is unavailable because the
	// package was built without cgo on Linux.
	ErrCgoRequired = errors.New("reconciliation: SQLite storage requires cgo and libsqlite3 on linux")
)

// Invoice is an input document from the receivables system. Amounts are
// expressed in the smallest indivisible unit of Currency (for example cents),
// never as floating point currency values.
type Invoice struct {
	// ID is the source-system invoice identifier. Required.
	ID string
	// Customer is the fixed customer identifier for this reconciliation
	// partition. Required.
	Customer string
	// Currency is the ISO currency code of the partition. Required.
	Currency string
	// Amount is the invoice amount in minor currency units. Must be >= 0.
	Amount int64
	// Version is the source version. When zero, insertion assigns version 1.
	// When non-zero it must be >= 1 and is stored verbatim for the first
	// version of the document.
	Version int64
}

// Payment is an input document from the cash receipts system. Amounts are
// expressed in minor currency units.
type Payment struct {
	// ID is the source-system payment identifier. Required.
	ID string
	// Customer is the fixed customer identifier for this reconciliation
	// partition. Required.
	Customer string
	// Currency is the ISO currency code of the partition. Required.
	Currency string
	// Amount is the payment amount in minor currency units. Must be >= 0.
	Amount int64
	// Version is the source version. See Invoice.Version.
	Version int64
}

// AllowedPair is an allowed invoice-to-payment write-off edge. An edge carries
// an integer unit cost: allocating Amount minor units along the edge costs
// UnitCost*Amount. MaxAmount limits the total amount that may ever be
// allocated along this edge; zero means unlimited.
type AllowedPair struct {
	// InvoiceID identifies the invoice side of the edge. Required.
	InvoiceID string
	// PaymentID identifies the payment side of the edge. Required.
	PaymentID string
	// Customer must equal the partition customer. Required.
	Customer string
	// Currency must equal the partition currency. Required.
	Currency string
	// UnitCost is the non-negative integer cost of allocating one minor unit
	// along this edge.
	UnitCost int64
	// MaxAmount is the maximum amount that may be allocated along this edge.
	// Zero means unlimited.
	MaxAmount int64
}

// Document is the persisted state of an invoice or payment. Balance is the
// amount that has not been confirmed as written off. PlannedAmount is the
// amount proposed by the current unconfirmed plan.
type Document struct {
	Kind            string
	ID              string
	Customer        string
	Currency        string
	Amount          int64
	Version         int64
	ConfirmedAmount int64
	Balance         int64
	PlannedAmount   int64
	Status          string
}

// Allocation is a confirmed write-off detail. Confirmed allocations are
// immutable and are never reassigned by later planning.
type Allocation struct {
	ID             int64
	InvoiceID      string
	PaymentID      string
	Amount         int64
	UnitCost       int64
	Cost           int64
	PlanGeneration int64
	InvoiceVersion int64
	PaymentVersion int64
	Status         string
}

// PlanItem is one unconfirmed proposed write-off in a plan generation.
type PlanItem struct {
	ID         int64
	Generation int64
	InvoiceID  string
	PaymentID  string
	Amount     int64
	UnitCost   int64
	Cost       int64
}

// PlannedAllocation is the result of the pure allocation optimizer.
type PlannedAllocation struct {
	InvoiceID string
	PaymentID string
	Amount    int64
	UnitCost  int64
	Cost      int64
}

// Balance is an input to the pure allocation optimizer.
type Balance struct {
	ID        string
	Remaining int64
}

// Plan is a persisted plan generation. Items are ordered by invoice and
// payment identifier. SourceVersion is the source state the plan was computed
// from; confirming against a different source state is rejected as stale.
type Plan struct {
	Generation     int64
	SourceVersion  int64
	TotalAllocated int64
	TotalCost      int64
	Items          []PlanItem
}

// Result is a complete, consistent snapshot of the reconciliation state: both
// sides' documents with balances and statuses, all confirmed write-off
// details, and the current unconfirmed plan.
type Result struct {
	Customer           string
	Currency           string
	SourceVersion      int64
	PlanGeneration     int64
	TotalInvoiceAmount int64
	TotalPaymentAmount int64
	TotalConfirmed     int64
	TotalPlanned       int64
	TotalPlannedCost   int64
	Invoices           []Document
	Payments           []Document
	Allocations        []Allocation
	PlanItems          []PlanItem
}

// ConfirmItem selects an amount of one planned invoice/payment pair to
// confirm. Amount must be greater than zero and no greater than the amount in
// the current plan item. The plan's unit cost is used; callers cannot change
// the cost at confirmation time.
type ConfirmItem struct {
	InvoiceID string
	PaymentID string
	Amount    int64
}

// ConfirmationRequest binds a confirmation to one plan generation and,
// optionally, to the source version observed by the caller. An empty Items
// slice confirms every item in the current plan. IdempotencyKey makes retries
// safe: repeating the same request returns the stored result without applying
// the write-off twice. When IdempotencyKey is empty, a deterministic key is
// derived from the request, which gives the same protection to identical
// retries.
type ConfirmationRequest struct {
	IdempotencyKey string
	PlanGeneration int64
	SourceVersion  int64
	Items          []ConfirmItem
}

// ConfirmationResult describes the confirmed write-offs and the plan
// generation recomputed from the remaining balances. Idempotent is true when
// the result was replayed from a previous identical request.
type ConfirmationResult struct {
	Idempotent        bool
	Allocations       []Allocation
	SourceVersion     int64
	PlanGeneration    int64
	NewPlanGeneration int64
	TotalConfirmed    int64
	TotalPlanned      int64
	TotalPlannedCost  int64
}
