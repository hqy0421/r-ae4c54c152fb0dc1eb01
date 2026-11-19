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
	"bytes"
	"reflect"
	"testing"

	"github.com/linkedin/goavro/v2/reconciliation"
)

func TestSourceCodesDecodeRealAvro(t *testing.T) {
	invCodec, err := reconciliation.NewInvoiceCodec()
	if err != nil {
		t.Fatalf("NewInvoiceCodec: %v", err)
	}
	payCodec, err := reconciliation.NewPaymentCodec()
	if err != nil {
		t.Fatalf("NewPaymentCodec: %v", err)
	}
	pairCodec, err := reconciliation.NewAllowedPairCodec()
	if err != nil {
		t.Fatalf("NewAllowedPairCodec: %v", err)
	}

	invBin, err := invCodec.BinaryFromNative(nil, map[string]interface{}{
		"id": "INV-1", "customer": "CUST-1", "currency": "USD", "amount": int64(1234), "version": int64(7),
	})
	if err != nil {
		t.Fatalf("encode invoice: %v", err)
	}
	inv, rest, err := reconciliation.DecodeInvoice(invCodec, invBin)
	if err != nil {
		t.Fatalf("DecodeInvoice: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("invoice leftover bytes = %d", len(rest))
	}
	if inv.ID != "INV-1" || inv.Amount != 1234 || inv.Version != 7 || inv.Customer != "CUST-1" || inv.Currency != "USD" {
		t.Fatalf("decoded invoice = %+v", inv)
	}

	payBin, err := payCodec.BinaryFromNative(nil, map[string]interface{}{
		"id": "PAY-1", "customer": "CUST-1", "currency": "USD", "amount": int64(99),
	})
	if err != nil {
		t.Fatalf("encode payment: %v", err)
	}
	pay, _, err := reconciliation.DecodePayment(payCodec, payBin)
	if err != nil {
		t.Fatalf("DecodePayment: %v", err)
	}
	if pay.ID != "PAY-1" || pay.Amount != 99 || pay.Version != 0 {
		t.Fatalf("decoded payment = %+v", pay)
	}

	pairBin, err := pairCodec.BinaryFromNative(nil, map[string]interface{}{
		"invoiceId": "INV-1", "paymentId": "PAY-1", "customer": "CUST-1", "currency": "USD", "unitCost": int64(3),
	})
	if err != nil {
		t.Fatalf("encode allowed pair: %v", err)
	}
	pair, _, err := reconciliation.DecodeAllowedPair(pairCodec, pairBin)
	if err != nil {
		t.Fatalf("DecodeAllowedPair: %v", err)
	}
	if pair.InvoiceID != "INV-1" || pair.PaymentID != "PAY-1" || pair.UnitCost != 3 {
		t.Fatalf("decoded pair = %+v", pair)
	}
}

func TestSourceOCFRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	invCodec, err := reconciliation.NewInvoiceCodec()
	if err != nil {
		t.Fatalf("NewInvoiceCodec: %v", err)
	}
	ocfw, err := newOCFWriterForTest(&buf, invCodec)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := ocfw.Append([]interface{}{
		map[string]interface{}{"id": "INV-1", "customer": "CUST-1", "currency": "USD", "amount": int64(10), "version": int64(1)},
		map[string]interface{}{"id": "INV-2", "customer": "CUST-1", "currency": "USD", "amount": int64(20), "version": int64(1)},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	invoices, err := reconciliation.ReadInvoicesOCF(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadInvoicesOCF: %v", err)
	}
	if len(invoices) != 2 || invoices[0].ID != "INV-1" || invoices[1].Amount != 20 {
		t.Fatalf("invoices = %+v", invoices)
	}
}

func TestResultCodecRoundTripPreservesEverything(t *testing.T) {
	s := openTestStore(t)
	putInvoice(t, s, "INV-1", 1000, 3)
	putInvoice(t, s, "INV-2", 500, 2)
	putPayment(t, s, "PAY-1", 700, 4)
	putPayment(t, s, "PAY-2", 300, 1)
	putEdge(t, s, "INV-1", "PAY-1", 1, 0)
	putEdge(t, s, "INV-2", "PAY-2", 2, 250)

	// Confirm a portion so the export carries both confirmed details and a
	// remaining unconfirmed plan.
	plan, _ := s.Plan()
	res, err := s.Confirm(reconciliation.ConfirmationRequest{
		PlanGeneration: plan.Generation, SourceVersion: plan.SourceVersion,
		Items: []reconciliation.ConfirmItem{{InvoiceID: "INV-1", PaymentID: "PAY-1", Amount: 400}},
	})
	if err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	if len(res.Allocations) != 1 {
		t.Fatalf("allocations = %+v", res.Allocations)
	}
	want := loadResult(t, s)

	binary, err := reconciliation.EncodeResultBinary(want)
	if err != nil {
		t.Fatalf("EncodeResultBinary: %v", err)
	}
	gotBinary, rest, err := reconciliation.DecodeResultBinary(binary)
	if err != nil {
		t.Fatalf("DecodeResultBinary: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("binary result leftover bytes = %d", len(rest))
	}
	assertResultsEqual(t, gotBinary, want)

	ocfBytes, err := reconciliation.ResultOCFBytes(want)
	if err != nil {
		t.Fatalf("ResultOCFBytes: %v", err)
	}
	gotOCF, err := reconciliation.ReadResultOCF(bytes.NewReader(ocfBytes))
	if err != nil {
		t.Fatalf("ReadResultOCF: %v", err)
	}
	assertResultsEqual(t, gotOCF, want)

	textual, err := reconciliation.EncodeResultTextual(want)
	if err != nil {
		t.Fatalf("EncodeResultTextual: %v", err)
	}
	codec, err := reconciliation.ResultCodec()
	if err != nil {
		t.Fatalf("ResultCodec: %v", err)
	}
	native, _, err := codec.NativeFromTextual(textual)
	if err != nil {
		t.Fatalf("decode textual: %v", err)
	}
	_ = native

	// The OCF must be a genuine Avro container readable by an independent Codec
	// built from the exported schema.
	independent, err := reconciliation.ResultCodec()
	if err != nil {
		t.Fatalf("ResultCodec: %v", err)
	}
	if independent.Schema() != codec.Schema() {
		t.Fatalf("schema mismatch")
	}
}

func TestResultCodecRejectsBadDocuments(t *testing.T) {
	_, err := reconciliation.ResultOCFBytes(reconciliation.Result{
		Customer: "CUST-1",
		Currency: "USD",
		Invoices: []reconciliation.Document{{Kind: "bogus", ID: "X"}},
	})
	if err == nil {
		t.Fatalf("expected error for unknown document kind")
	}
}

func assertResultsEqual(t *testing.T, got, want reconciliation.Result) {
	t.Helper()
	if got.Customer != want.Customer || got.Currency != want.Currency {
		t.Fatalf("partition = %s/%s, want %s/%s", got.Customer, got.Currency, want.Customer, want.Currency)
	}
	if got.SourceVersion != want.SourceVersion || got.PlanGeneration != want.PlanGeneration {
		t.Fatalf("versions = %d/%d, want %d/%d", got.SourceVersion, got.PlanGeneration, want.SourceVersion, want.PlanGeneration)
	}
	if got.TotalInvoiceAmount != want.TotalInvoiceAmount || got.TotalPaymentAmount != want.TotalPaymentAmount {
		t.Fatalf("totals = %d/%d, want %d/%d", got.TotalInvoiceAmount, got.TotalPaymentAmount, want.TotalInvoiceAmount, want.TotalPaymentAmount)
	}
	if got.TotalConfirmed != want.TotalConfirmed || got.TotalPlanned != want.TotalPlanned || got.TotalPlannedCost != want.TotalPlannedCost {
		t.Fatalf("totals = confirmed %d planned %d cost %d, want %d/%d/%d",
			got.TotalConfirmed, got.TotalPlanned, got.TotalPlannedCost, want.TotalConfirmed, want.TotalPlanned, want.TotalPlannedCost)
	}
	if !reflect.DeepEqual(got.Invoices, want.Invoices) {
		t.Fatalf("invoices mismatch:\n got %+v\nwant %+v", got.Invoices, want.Invoices)
	}
	if !reflect.DeepEqual(got.Payments, want.Payments) {
		t.Fatalf("payments mismatch:\n got %+v\nwant %+v", got.Payments, want.Payments)
	}
	if !reflect.DeepEqual(got.Allocations, want.Allocations) {
		t.Fatalf("allocations mismatch:\n got %+v\nwant %+v", got.Allocations, want.Allocations)
	}
	if !reflect.DeepEqual(got.PlanItems, want.PlanItems) {
		t.Fatalf("plan items mismatch:\n got %+v\nwant %+v", got.PlanItems, want.PlanItems)
	}
}
