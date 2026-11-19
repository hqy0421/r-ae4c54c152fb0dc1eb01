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
	"bytes"
	"fmt"
	"io"

	"github.com/linkedin/goavro/v2"
)

// ResultSchema is the Avro schema of a complete reconciliation result. The
// documents on both sides carry their balance and status, the confirmed
// allocations are the immutable write-off details, and planItems is the
// unconfirmed plan for the current generation.
const ResultSchema = `{
  "type": "record",
  "name": "ReconciliationResult",
  "namespace": "recon",
  "fields": [
    {"name": "customer", "type": "string"},
    {"name": "currency", "type": "string"},
    {"name": "sourceVersion", "type": "long"},
    {"name": "planGeneration", "type": "long"},
    {"name": "totalInvoiceAmount", "type": "long"},
    {"name": "totalPaymentAmount", "type": "long"},
    {"name": "totalConfirmed", "type": "long"},
    {"name": "totalPlanned", "type": "long"},
    {"name": "totalPlannedCost", "type": "long"},
    {"name": "invoices", "type": {"type": "array", "items": {
      "type": "record", "name": "InvoiceState", "fields": [
        {"name": "id", "type": "string"},
        {"name": "customer", "type": "string"},
        {"name": "currency", "type": "string"},
        {"name": "amount", "type": "long"},
        {"name": "version", "type": "long"},
        {"name": "confirmedAmount", "type": "long"},
        {"name": "balance", "type": "long"},
        {"name": "status", "type": "string"}
      ]}}},
    {"name": "payments", "type": {"type": "array", "items": {
      "type": "record", "name": "PaymentState", "fields": [
        {"name": "id", "type": "string"},
        {"name": "customer", "type": "string"},
        {"name": "currency", "type": "string"},
        {"name": "amount", "type": "long"},
        {"name": "version", "type": "long"},
        {"name": "confirmedAmount", "type": "long"},
        {"name": "balance", "type": "long"},
        {"name": "status", "type": "string"}
      ]}}},
    {"name": "allocations", "type": {"type": "array", "items": {
      "type": "record", "name": "AllocationDetail", "fields": [
        {"name": "id", "type": "long"},
        {"name": "invoiceId", "type": "string"},
        {"name": "paymentId", "type": "string"},
        {"name": "amount", "type": "long"},
        {"name": "unitCost", "type": "long"},
        {"name": "cost", "type": "long"},
        {"name": "planGeneration", "type": "long"},
        {"name": "invoiceVersion", "type": "long"},
        {"name": "paymentVersion", "type": "long"},
        {"name": "status", "type": "string"}
      ]}}},
    {"name": "planItems", "type": {"type": "array", "items": {
      "type": "record", "name": "PlanItemDetail", "fields": [
        {"name": "id", "type": "long"},
        {"name": "generation", "type": "long"},
        {"name": "invoiceId", "type": "string"},
        {"name": "paymentId", "type": "string"},
        {"name": "amount", "type": "long"},
        {"name": "unitCost", "type": "long"},
        {"name": "cost", "type": "long"}
      ]}}}
  ]
}`

// ResultCodec returns the Avro codec used to export reconciliation results.
func ResultCodec() (*goavro.Codec, error) { return goavro.NewCodec(ResultSchema) }

// EncodeResultBinary encodes the result as a single binary Avro datum using the
// result Codec.
func EncodeResultBinary(res Result) ([]byte, error) {
	codec, err := ResultCodec()
	if err != nil {
		return nil, err
	}
	native, err := resultToNative(res)
	if err != nil {
		return nil, err
	}
	encoded, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("encode reconciliation result: %w", err)
	}
	return encoded, nil
}

// DecodeResultBinary decodes a result previously produced by
// EncodeResultBinary or WriteResultOCF, using the result Codec.
func DecodeResultBinary(buf []byte) (Result, []byte, error) {
	codec, err := ResultCodec()
	if err != nil {
		return Result{}, buf, err
	}
	native, rest, err := codec.NativeFromBinary(buf)
	if err != nil {
		return Result{}, buf, fmt.Errorf("%w: decode reconciliation result: %v", ErrInvalid, err)
	}
	res, err := resultFromNative(native)
	return res, rest, err
}

// WriteResultOCF writes the result as a complete Avro Object Container File
// using the result Codec, so external consumers can read it with any Avro
// implementation.
func WriteResultOCF(w io.Writer, res Result) error {
	codec, err := ResultCodec()
	if err != nil {
		return err
	}
	native, err := resultToNative(res)
	if err != nil {
		return err
	}
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{W: w, Codec: codec})
	if err != nil {
		return fmt.Errorf("create result OCF writer: %w", err)
	}
	if err := ocfw.Append([]interface{}{native}); err != nil {
		return fmt.Errorf("append reconciliation result: %w", err)
	}
	return nil
}

// ReadResultOCF reads a result written by WriteResultOCF.
func ReadResultOCF(r io.Reader) (Result, error) {
	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		return Result{}, fmt.Errorf("%w: read result OCF: %v", ErrInvalid, err)
	}
	var out Result
	count := 0
	for ocfr.Scan() {
		native, err := ocfr.Read()
		if err != nil {
			return Result{}, fmt.Errorf("%w: read result OCF datum: %v", ErrInvalid, err)
		}
		res, err := resultFromNative(native)
		if err != nil {
			return Result{}, err
		}
		out = res
		count++
	}
	if err := ocfr.Err(); err != nil {
		return Result{}, fmt.Errorf("%w: read result OCF: %v", ErrInvalid, err)
	}
	if count != 1 {
		return Result{}, fmt.Errorf("%w: result OCF contains %d records, want 1", ErrInvalid, count)
	}
	return out, nil
}

// EncodeResultTextual encodes the result as Avro JSON using the result Codec.
func EncodeResultTextual(res Result) ([]byte, error) {
	codec, err := ResultCodec()
	if err != nil {
		return nil, err
	}
	native, err := resultToNative(res)
	if err != nil {
		return nil, err
	}
	encoded, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("encode reconciliation result as textual Avro: %w", err)
	}
	return encoded, nil
}

func resultToNative(res Result) (interface{}, error) {
	invoices := make([]interface{}, 0, len(res.Invoices))
	for _, d := range res.Invoices {
		n, err := documentToNative(d)
		if err != nil {
			return nil, err
		}
		invoices = append(invoices, n)
	}
	payments := make([]interface{}, 0, len(res.Payments))
	for _, d := range res.Payments {
		n, err := documentToNative(d)
		if err != nil {
			return nil, err
		}
		payments = append(payments, n)
	}
	allocations := make([]interface{}, 0, len(res.Allocations))
	for _, a := range res.Allocations {
		allocations = append(allocations, map[string]interface{}{
			"id":             a.ID,
			"invoiceId":      a.InvoiceID,
			"paymentId":      a.PaymentID,
			"amount":         a.Amount,
			"unitCost":       a.UnitCost,
			"cost":           a.Cost,
			"planGeneration": a.PlanGeneration,
			"invoiceVersion": a.InvoiceVersion,
			"paymentVersion": a.PaymentVersion,
			"status":         a.Status,
		})
	}
	planItems := make([]interface{}, 0, len(res.PlanItems))
	for _, it := range res.PlanItems {
		planItems = append(planItems, map[string]interface{}{
			"id":         it.ID,
			"generation": it.Generation,
			"invoiceId":  it.InvoiceID,
			"paymentId":  it.PaymentID,
			"amount":     it.Amount,
			"unitCost":   it.UnitCost,
			"cost":       it.Cost,
		})
	}
	return map[string]interface{}{
		"customer":           res.Customer,
		"currency":           res.Currency,
		"sourceVersion":      res.SourceVersion,
		"planGeneration":     res.PlanGeneration,
		"totalInvoiceAmount": res.TotalInvoiceAmount,
		"totalPaymentAmount": res.TotalPaymentAmount,
		"totalConfirmed":     res.TotalConfirmed,
		"totalPlanned":       res.TotalPlanned,
		"totalPlannedCost":   res.TotalPlannedCost,
		"invoices":           invoices,
		"payments":           payments,
		"allocations":        allocations,
		"planItems":          planItems,
	}, nil
}

func documentToNative(d Document) (interface{}, error) {
	if d.Kind != KindInvoice && d.Kind != KindPayment {
		return nil, fmt.Errorf("%w: document %s has unknown kind %q", ErrInvalid, d.ID, d.Kind)
	}
	return map[string]interface{}{
		"id":              d.ID,
		"customer":        d.Customer,
		"currency":        d.Currency,
		"amount":          d.Amount,
		"version":         d.Version,
		"confirmedAmount": d.ConfirmedAmount,
		"balance":         d.Balance,
		"status":          d.Status,
	}, nil
}

func resultFromNative(native interface{}) (Result, error) {
	m, err := recordMap(native, "reconciliation result")
	if err != nil {
		return Result{}, err
	}
	var res Result
	if res.Customer, err = fieldString(m, "customer"); err != nil {
		return Result{}, err
	}
	if res.Currency, err = fieldString(m, "currency"); err != nil {
		return Result{}, err
	}
	for _, f := range []struct {
		name string
		dst  *int64
	}{
		{"sourceVersion", &res.SourceVersion},
		{"planGeneration", &res.PlanGeneration},
		{"totalInvoiceAmount", &res.TotalInvoiceAmount},
		{"totalPaymentAmount", &res.TotalPaymentAmount},
		{"totalConfirmed", &res.TotalConfirmed},
		{"totalPlanned", &res.TotalPlanned},
		{"totalPlannedCost", &res.TotalPlannedCost},
	} {
		if *f.dst, err = fieldInt64(m, f.name); err != nil {
			return Result{}, err
		}
	}
	if res.Invoices, err = documentsFromNative(m, "invoices", KindInvoice); err != nil {
		return Result{}, err
	}
	if res.Payments, err = documentsFromNative(m, "payments", KindPayment); err != nil {
		return Result{}, err
	}
	if res.Allocations, err = allocationsFromNative(m); err != nil {
		return Result{}, err
	}
	if res.PlanItems, err = planItemsFromNative(m); err != nil {
		return Result{}, err
	}
	return res, nil
}

func documentsFromNative(m map[string]interface{}, field, kind string) ([]Document, error) {
	raw, ok := m[field]
	if !ok {
		return nil, fmt.Errorf("%w: missing field %q", ErrInvalid, field)
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: field %q decoded as %T, want array", ErrInvalid, field, raw)
	}
	out := make([]Document, 0, len(items))
	for _, item := range items {
		dm, err := recordMap(item, field)
		if err != nil {
			return nil, err
		}
		d := Document{Kind: kind}
		if d.ID, err = fieldString(dm, "id"); err != nil {
			return nil, err
		}
		if d.Customer, err = fieldString(dm, "customer"); err != nil {
			return nil, err
		}
		if d.Currency, err = fieldString(dm, "currency"); err != nil {
			return nil, err
		}
		for _, f := range []struct {
			name string
			dst  *int64
		}{
			{"amount", &d.Amount},
			{"version", &d.Version},
			{"confirmedAmount", &d.ConfirmedAmount},
			{"balance", &d.Balance},
		} {
			if *f.dst, err = fieldInt64(dm, f.name); err != nil {
				return nil, err
			}
		}
		if d.Status, err = fieldString(dm, "status"); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

func allocationsFromNative(m map[string]interface{}) ([]Allocation, error) {
	raw, ok := m["allocations"]
	if !ok {
		return nil, fmt.Errorf("%w: missing field %q", ErrInvalid, "allocations")
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: field %q decoded as %T, want array", ErrInvalid, "allocations", raw)
	}
	out := make([]Allocation, 0, len(items))
	for _, item := range items {
		am, err := recordMap(item, "allocation")
		if err != nil {
			return nil, err
		}
		var a Allocation
		if a.ID, err = fieldInt64(am, "id"); err != nil {
			return nil, err
		}
		if a.InvoiceID, err = fieldString(am, "invoiceId"); err != nil {
			return nil, err
		}
		if a.PaymentID, err = fieldString(am, "paymentId"); err != nil {
			return nil, err
		}
		for _, f := range []struct {
			name string
			dst  *int64
		}{
			{"amount", &a.Amount},
			{"unitCost", &a.UnitCost},
			{"cost", &a.Cost},
			{"planGeneration", &a.PlanGeneration},
			{"invoiceVersion", &a.InvoiceVersion},
			{"paymentVersion", &a.PaymentVersion},
		} {
			if *f.dst, err = fieldInt64(am, f.name); err != nil {
				return nil, err
			}
		}
		if a.Status, err = fieldString(am, "status"); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

func planItemsFromNative(m map[string]interface{}) ([]PlanItem, error) {
	raw, ok := m["planItems"]
	if !ok {
		return nil, fmt.Errorf("%w: missing field %q", ErrInvalid, "planItems")
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: field %q decoded as %T, want array", ErrInvalid, "planItems", raw)
	}
	out := make([]PlanItem, 0, len(items))
	for _, item := range items {
		pm, err := recordMap(item, "plan item")
		if err != nil {
			return nil, err
		}
		var p PlanItem
		if p.ID, err = fieldInt64(pm, "id"); err != nil {
			return nil, err
		}
		if p.Generation, err = fieldInt64(pm, "generation"); err != nil {
			return nil, err
		}
		if p.InvoiceID, err = fieldString(pm, "invoiceId"); err != nil {
			return nil, err
		}
		if p.PaymentID, err = fieldString(pm, "paymentId"); err != nil {
			return nil, err
		}
		for _, f := range []struct {
			name string
			dst  *int64
		}{
			{"amount", &p.Amount},
			{"unitCost", &p.UnitCost},
			{"cost", &p.Cost},
		} {
			if *f.dst, err = fieldInt64(pm, f.name); err != nil {
				return nil, err
			}
		}
		out = append(out, p)
	}
	return out, nil
}

// ExportResultOCF reads a consistent snapshot from the Store and writes the
// complete reconciliation result to w as Avro OCF, using the result Codec.
func (s *Store) ExportResultOCF(w io.Writer) error {
	res, err := s.Result()
	if err != nil {
		return err
	}
	return WriteResultOCF(w, res)
}

// ResultOCFBytes returns the result encoded as an OCF in memory.
func ResultOCFBytes(res Result) ([]byte, error) {
	var buf bytes.Buffer
	if err := WriteResultOCF(&buf, res); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
