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
	"fmt"
	"io"

	"github.com/linkedin/goavro/v2"
)

// Avro schemas for the source records consumed by this package and for the
// exported reconciliation result. All monetary amounts use Avro "long" and are
// carried as int64 minor currency units.

// InvoiceSchema is the Avro schema of one receivable invoice record.
const InvoiceSchema = `{
  "type": "record",
  "name": "Invoice",
  "namespace": "recon",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "customer", "type": "string"},
    {"name": "currency", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "version", "type": "long", "default": 0}
  ]
}`

// PaymentSchema is the Avro schema of one cash receipt record.
const PaymentSchema = `{
  "type": "record",
  "name": "Payment",
  "namespace": "recon",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "customer", "type": "string"},
    {"name": "currency", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "version", "type": "long", "default": 0}
  ]
}`

// AllowedPairSchema is the Avro schema of one allowed invoice-to-payment edge.
const AllowedPairSchema = `{
  "type": "record",
  "name": "AllowedPair",
  "namespace": "recon",
  "fields": [
    {"name": "invoiceId", "type": "string"},
    {"name": "paymentId", "type": "string"},
    {"name": "customer", "type": "string"},
    {"name": "currency", "type": "string"},
    {"name": "unitCost", "type": "long"},
    {"name": "maxAmount", "type": "long", "default": 0}
  ]
}`

// NewInvoiceCodec returns an Avro codec for invoice source records.
func NewInvoiceCodec() (*goavro.Codec, error) { return goavro.NewCodec(InvoiceSchema) }

// NewPaymentCodec returns an Avro codec for payment source records.
func NewPaymentCodec() (*goavro.Codec, error) { return goavro.NewCodec(PaymentSchema) }

// NewAllowedPairCodec returns an Avro codec for allowed-pair source records.
func NewAllowedPairCodec() (*goavro.Codec, error) { return goavro.NewCodec(AllowedPairSchema) }

// DecodeInvoice decodes one binary Avro invoice datum, returning the invoice
// and any remaining bytes.
func DecodeInvoice(codec *goavro.Codec, buf []byte) (Invoice, []byte, error) {
	if codec == nil {
		return Invoice{}, buf, fmt.Errorf("%w: nil invoice codec", ErrInvalid)
	}
	native, rest, err := codec.NativeFromBinary(buf)
	if err != nil {
		return Invoice{}, buf, fmt.Errorf("%w: decode invoice: %v", ErrInvalid, err)
	}
	inv, err := invoiceFromNative(native)
	return inv, rest, err
}

// DecodePayment decodes one binary Avro payment datum.
func DecodePayment(codec *goavro.Codec, buf []byte) (Payment, []byte, error) {
	if codec == nil {
		return Payment{}, buf, fmt.Errorf("%w: nil payment codec", ErrInvalid)
	}
	native, rest, err := codec.NativeFromBinary(buf)
	if err != nil {
		return Payment{}, buf, fmt.Errorf("%w: decode payment: %v", ErrInvalid, err)
	}
	p, err := paymentFromNative(native)
	return p, rest, err
}

// DecodeAllowedPair decodes one binary Avro allowed-pair datum.
func DecodeAllowedPair(codec *goavro.Codec, buf []byte) (AllowedPair, []byte, error) {
	if codec == nil {
		return AllowedPair{}, buf, fmt.Errorf("%w: nil allowed-pair codec", ErrInvalid)
	}
	native, rest, err := codec.NativeFromBinary(buf)
	if err != nil {
		return AllowedPair{}, buf, fmt.Errorf("%w: decode allowed pair: %v", ErrInvalid, err)
	}
	pair, err := allowedPairFromNative(native)
	return pair, rest, err
}

// ReadInvoicesOCF decodes every invoice record from an Avro Object Container
// File read from r.
func ReadInvoicesOCF(r io.Reader) ([]Invoice, error) {
	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		return nil, fmt.Errorf("%w: read invoice OCF: %v", ErrInvalid, err)
	}
	var out []Invoice
	for ocfr.Scan() {
		native, err := ocfr.Read()
		if err != nil {
			return nil, fmt.Errorf("%w: read invoice OCF datum: %v", ErrInvalid, err)
		}
		inv, err := invoiceFromNative(native)
		if err != nil {
			return nil, err
		}
		out = append(out, inv)
	}
	if err := ocfr.Err(); err != nil {
		return nil, fmt.Errorf("%w: read invoice OCF: %v", ErrInvalid, err)
	}
	return out, nil
}

// ReadPaymentsOCF decodes every payment record from an Avro OCF.
func ReadPaymentsOCF(r io.Reader) ([]Payment, error) {
	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		return nil, fmt.Errorf("%w: read payment OCF: %v", ErrInvalid, err)
	}
	var out []Payment
	for ocfr.Scan() {
		native, err := ocfr.Read()
		if err != nil {
			return nil, fmt.Errorf("%w: read payment OCF datum: %v", ErrInvalid, err)
		}
		p, err := paymentFromNative(native)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if err := ocfr.Err(); err != nil {
		return nil, fmt.Errorf("%w: read payment OCF: %v", ErrInvalid, err)
	}
	return out, nil
}

// ReadAllowedPairsOCF decodes every allowed-pair record from an Avro OCF.
func ReadAllowedPairsOCF(r io.Reader) ([]AllowedPair, error) {
	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		return nil, fmt.Errorf("%w: read allowed-pair OCF: %v", ErrInvalid, err)
	}
	var out []AllowedPair
	for ocfr.Scan() {
		native, err := ocfr.Read()
		if err != nil {
			return nil, fmt.Errorf("%w: read allowed-pair OCF datum: %v", ErrInvalid, err)
		}
		p, err := allowedPairFromNative(native)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if err := ocfr.Err(); err != nil {
		return nil, fmt.Errorf("%w: read allowed-pair OCF: %v", ErrInvalid, err)
	}
	return out, nil
}

func invoiceFromNative(native interface{}) (Invoice, error) {
	m, err := recordMap(native, "invoice")
	if err != nil {
		return Invoice{}, err
	}
	id, err := fieldString(m, "id")
	if err != nil {
		return Invoice{}, err
	}
	customer, err := fieldString(m, "customer")
	if err != nil {
		return Invoice{}, err
	}
	currency, err := fieldString(m, "currency")
	if err != nil {
		return Invoice{}, err
	}
	amount, err := fieldInt64(m, "amount")
	if err != nil {
		return Invoice{}, err
	}
	version, err := fieldInt64Default(m, "version", 0)
	if err != nil {
		return Invoice{}, err
	}
	return Invoice{ID: id, Customer: customer, Currency: currency, Amount: amount, Version: version}, nil
}

func paymentFromNative(native interface{}) (Payment, error) {
	m, err := recordMap(native, "payment")
	if err != nil {
		return Payment{}, err
	}
	id, err := fieldString(m, "id")
	if err != nil {
		return Payment{}, err
	}
	customer, err := fieldString(m, "customer")
	if err != nil {
		return Payment{}, err
	}
	currency, err := fieldString(m, "currency")
	if err != nil {
		return Payment{}, err
	}
	amount, err := fieldInt64(m, "amount")
	if err != nil {
		return Payment{}, err
	}
	version, err := fieldInt64Default(m, "version", 0)
	if err != nil {
		return Payment{}, err
	}
	return Payment{ID: id, Customer: customer, Currency: currency, Amount: amount, Version: version}, nil
}

func allowedPairFromNative(native interface{}) (AllowedPair, error) {
	m, err := recordMap(native, "allowed pair")
	if err != nil {
		return AllowedPair{}, err
	}
	invoiceID, err := fieldString(m, "invoiceId")
	if err != nil {
		return AllowedPair{}, err
	}
	paymentID, err := fieldString(m, "paymentId")
	if err != nil {
		return AllowedPair{}, err
	}
	customer, err := fieldString(m, "customer")
	if err != nil {
		return AllowedPair{}, err
	}
	currency, err := fieldString(m, "currency")
	if err != nil {
		return AllowedPair{}, err
	}
	unitCost, err := fieldInt64(m, "unitCost")
	if err != nil {
		return AllowedPair{}, err
	}
	maxAmount, err := fieldInt64Default(m, "maxAmount", 0)
	if err != nil {
		return AllowedPair{}, err
	}
	return AllowedPair{InvoiceID: invoiceID, PaymentID: paymentID, Customer: customer, Currency: currency, UnitCost: unitCost, MaxAmount: maxAmount}, nil
}

func recordMap(native interface{}, what string) (map[string]interface{}, error) {
	m, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: %s record decoded as %T, want map", ErrInvalid, what, native)
	}
	return m, nil
}

func fieldString(m map[string]interface{}, name string) (string, error) {
	v, ok := m[name]
	if !ok {
		return "", fmt.Errorf("%w: missing field %q", ErrInvalid, name)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%w: field %q decoded as %T, want string", ErrInvalid, name, v)
	}
	return s, nil
}

func fieldInt64(m map[string]interface{}, name string) (int64, error) {
	v, ok := m[name]
	if !ok {
		return 0, fmt.Errorf("%w: missing field %q", ErrInvalid, name)
	}
	return asInt64(v, name)
}

func fieldInt64Default(m map[string]interface{}, name string, def int64) (int64, error) {
	v, ok := m[name]
	if !ok || v == nil {
		return def, nil
	}
	return asInt64(v, name)
}

func asInt64(v interface{}, name string) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int32:
		return int64(n), nil
	case int:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("%w: field %q decoded as %T, want long", ErrInvalid, name, v)
	}
}
