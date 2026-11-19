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
	"errors"
	"testing"
)

func mustOptimize(t *testing.T, invoices, payments []Balance, edges []Edge) ([]PlannedAllocation, int64, int64) {
	t.Helper()
	allocs, amount, cost, err := Optimize(invoices, payments, edges)
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	return allocs, amount, cost
}

func TestOptimizeMaximizesAmountBeforeCost(t *testing.T) {
	// Invoice A=10 can match P1 at cost 5 or P2 at cost 1. Invoice B=10 only
	// matches P1 at cost 0. Allocating A->P1 maximizes cost early but strands
	// B, so it is not optimal. The maximum amount is 20, and the cheapest way
	// to reach it is B->P1 (cost 0) and A->P2 (cost 1*10=10).
	invoices := []Balance{{ID: "A", Remaining: 10}, {ID: "B", Remaining: 10}}
	payments := []Balance{{ID: "P1", Remaining: 10}, {ID: "P2", Remaining: 10}}
	edges := []Edge{
		{InvoiceID: "A", PaymentID: "P1", UnitCost: 5},
		{InvoiceID: "A", PaymentID: "P2", UnitCost: 1},
		{InvoiceID: "B", PaymentID: "P1", UnitCost: 0},
	}
	allocs, amount, cost := mustOptimize(t, invoices, payments, edges)
	if amount != 20 {
		t.Fatalf("amount = %d, want 20", amount)
	}
	if cost != 10 {
		t.Fatalf("cost = %d, want 10", cost)
	}
	got := map[[2]string]int64{}
	for _, a := range allocs {
		got[[2]string{a.InvoiceID, a.PaymentID}] = a.Amount
	}
	want := map[[2]string]int64{
		{"B", "P1"}: 10,
		{"A", "P2"}: 10,
	}
	if len(got) != len(want) {
		t.Fatalf("allocations = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("allocations = %v, want %v", got, want)
		}
	}
}

func TestOptimizeAmountBeatsCheaperPartialPlan(t *testing.T) {
	// One invoice of 10, two payments of 10 each, both allowed. Maximum amount
	// is 10. Among max-amount solutions the cheaper payment must win.
	invoices := []Balance{{ID: "I", Remaining: 10}}
	payments := []Balance{{ID: "P-expensive", Remaining: 10}, {ID: "P-cheap", Remaining: 10}}
	edges := []Edge{
		{InvoiceID: "I", PaymentID: "P-expensive", UnitCost: 9},
		{InvoiceID: "I", PaymentID: "P-cheap", UnitCost: 2},
	}
	allocs, amount, cost := mustOptimize(t, invoices, payments, edges)
	if amount != 10 {
		t.Fatalf("amount = %d, want 10", amount)
	}
	if cost != 20 {
		t.Fatalf("cost = %d, want 20 (10 units at cost 2)", cost)
	}
	if len(allocs) != 1 || allocs[0].PaymentID != "P-cheap" || allocs[0].Amount != 10 {
		t.Fatalf("allocations = %+v, want single P-cheap 10", allocs)
	}
}

func TestOptimizePartialAmountsExactInteger(t *testing.T) {
	// Invoice 7, payment 5: exact partial 5, not 7 and not 0.
	invoices := []Balance{{ID: "I", Remaining: 7}}
	payments := []Balance{{ID: "P", Remaining: 5}}
	edges := []Edge{{InvoiceID: "I", PaymentID: "P", UnitCost: 3}}
	allocs, amount, cost := mustOptimize(t, invoices, payments, edges)
	if amount != 5 || cost != 15 {
		t.Fatalf("amount=%d cost=%d, want 5/15", amount, cost)
	}
	if len(allocs) != 1 || allocs[0].Amount != 5 || allocs[0].Cost != 15 {
		t.Fatalf("allocations = %+v", allocs)
	}
}

func TestOptimizeRespectsEdgeMaxAmount(t *testing.T) {
	invoices := []Balance{{ID: "I", Remaining: 100}}
	payments := []Balance{{ID: "P1", Remaining: 100}, {ID: "P2", Remaining: 100}}
	edges := []Edge{
		{InvoiceID: "I", PaymentID: "P1", UnitCost: 1, MaxAmount: 40},
		{InvoiceID: "I", PaymentID: "P2", UnitCost: 1},
	}
	allocs, amount, cost := mustOptimize(t, invoices, payments, edges)
	if amount != 100 || cost != 100 {
		t.Fatalf("amount=%d cost=%d, want 100/100", amount, cost)
	}
	got := map[string]int64{}
	for _, a := range allocs {
		got[a.PaymentID] = a.Amount
	}
	if got["P1"] != 40 || got["P2"] != 60 {
		t.Fatalf("allocations = %v, want P1=40 P2=60", got)
	}
}

func TestOptimizeEmptyAndInvalid(t *testing.T) {
	if _, amount, cost, err := Optimize(nil, nil, nil); err != nil || amount != 0 || cost != 0 {
		t.Fatalf("empty: amount=%d cost=%d err=%v", amount, cost, err)
	}
	_, _, _, err := Optimize([]Balance{{ID: "i", Remaining: -1}}, nil, nil)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative balance err = %v, want ErrInvalid", err)
	}
	_, _, _, err = Optimize(
		[]Balance{{ID: "i", Remaining: 1}},
		[]Balance{{ID: "p", Remaining: 1}},
		[]Edge{{InvoiceID: "i", PaymentID: "p", UnitCost: -1}},
	)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative cost err = %v, want ErrInvalid", err)
	}
	_, _, _, err = Optimize(
		[]Balance{{ID: "i", Remaining: 1}},
		[]Balance{{ID: "p", Remaining: 1}},
		[]Edge{{InvoiceID: "i", PaymentID: "p"}, {InvoiceID: "i", PaymentID: "p"}},
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate edge err = %v, want ErrConflict", err)
	}
}

func TestOptimizeIsDeterministic(t *testing.T) {
	invoices := []Balance{{ID: "I2", Remaining: 5}, {ID: "I1", Remaining: 5}}
	payments := []Balance{{ID: "P2", Remaining: 5}, {ID: "P1", Remaining: 5}}
	edges := []Edge{
		{InvoiceID: "I1", PaymentID: "P1", UnitCost: 1},
		{InvoiceID: "I2", PaymentID: "P2", UnitCost: 1},
	}
	first, _, _ := mustOptimize(t, invoices, payments, edges)
	for i := 0; i < 20; i++ {
		again, _, _ := mustOptimize(t, invoices, payments, edges)
		if len(again) != len(first) {
			t.Fatalf("iteration %d length differs", i)
		}
		for j := range again {
			if again[j] != first[j] {
				t.Fatalf("iteration %d allocation %d differs: %+v vs %+v", i, j, again[j], first[j])
			}
		}
	}
}
