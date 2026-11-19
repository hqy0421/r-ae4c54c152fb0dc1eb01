// Copyright [2024] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

/*
Package reconciliation implements exact partial write-off between receivable
invoices (one source system) and cash receipts (another source system) for a
single fixed customer and currency partition.

# Amounts

Every monetary value is an int64 count of the currency's smallest indivisible
unit (for example, cents). No floating point currency is ever used, and the
allocation arithmetic is exact.

# Allocation objective

Optimize solves the exact transportation problem over the remaining invoice and
payment balances and the allowed invoice-to-payment edges. It first maximizes
the total amount written off; among all solutions that reach that maximum it
minimizes the sum of UnitCost*Amount over the selected pairs. Allocations are
integers of at most the smaller of the two balances, so partial write-off is
exact rather than rounded. Results are deterministic.

# Confirmed and planned state

Confirmed allocations are immutable write-off details. They are never
reallocated by later planning. Unconfirmed plans are recomputed only from
balances net of confirmed amounts, so planning can never "spend" an amount
already written off.

# Source corrections

A source correction may raise a document's amount, but it may not lower an
amount below what has already been confirmed, and it may not delete a document
that has confirmed details. Corrections with an older version, or that change
an amount without advancing the version, are rejected as stale.

# Confirmation semantics

Confirm binds a request to the plan generation and source version the caller
observed. If the stored generation or source version has moved on, the request
is rejected as stale. Retrying the same request is idempotent: a supplied
IdempotencyKey is honoured, and without one a key is derived from the request,
so an identical retry returns the stored result without applying the write-off
twice. A confirmation can never allocate more than an invoice balance, a
payment balance, or an allowed edge's remaining capacity.

# Durability

Documents, confirmed write-off details, both sides' balances and statuses,
plan items, and version counters are committed together in a real SQLite
transaction. A failure rolls back to the previous state, and reopening the
database continues from the last committed state. Imports are applied as one
atomic batch.

# Export

The complete reconciliation result (both sides' documents with balances and
statuses, confirmed allocations, and the current plan) is exported through
goavro Codec as binary Avro, Avro JSON, or an Avro Object Container File. Source
invoices, payments, and allowed pairs are likewise decoded with goavro Codec.

This package only produces reconciliation data. It never moves or settles
funds, and it leaves the existing goavro API unchanged.
*/
package reconciliation
