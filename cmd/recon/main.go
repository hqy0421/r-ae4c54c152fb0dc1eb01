// Copyright [2024] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

// Command recon builds and confirms exact partial write-off plans between
// receivables invoices and cash receipts. It imports source records from Avro
// Object Container Files, persists plans and confirmations in SQLite, and
// exports the complete reconciliation result as Avro.
//
// recon never moves money. It only produces reconciliation data.
//
// Usage:
//
//	recon -db recon.db -customer CUST-1 -currency USD import \
//	    -invoices invoices.avro -payments payments.avro -pairs pairs.avro
//	recon -db recon.db -customer CUST-1 -currency USD plan
//	recon -db recon.db -customer CUST-1 -currency USD confirm \
//	    -generation 3 -source-version 12 -key batch-1 -items items.json
//	recon -db recon.db -customer CUST-1 -currency USD export -out result.avro
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/linkedin/goavro/v2/reconciliation"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "recon: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	global := flag.NewFlagSet("recon", flag.ContinueOnError)
	dbPath := global.String("db", "", "path to the SQLite database (required)")
	customer := global.String("customer", "", "partition customer identifier (required)")
	currency := global.String("currency", "", "partition currency code (required)")
	global.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: recon -db PATH -customer ID -currency CODE <command> [flags]")
		fmt.Fprintln(os.Stderr, "commands: import, plan, confirm, export, show")
		global.PrintDefaults()
	}
	if err := global.Parse(args); err != nil {
		return err
	}
	rest := global.Args()
	if len(rest) == 0 {
		global.Usage()
		return fmt.Errorf("missing command")
	}
	if *dbPath == "" || *customer == "" || *currency == "" {
		global.Usage()
		return fmt.Errorf("-db, -customer, and -currency are required")
	}

	store, err := reconciliation.Open(*dbPath, reconciliation.Options{Customer: *customer, Currency: *currency})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	cmd, cmdArgs := rest[0], rest[1:]
	switch cmd {
	case "import":
		return runImport(store, cmdArgs)
	case "plan":
		return runPlan(store, cmdArgs)
	case "confirm":
		return runConfirm(store, cmdArgs)
	case "export":
		return runExport(store, cmdArgs)
	case "show":
		return runShow(store, cmdArgs)
	default:
		global.Usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func runImport(store *reconciliation.Store, args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	invoicesPath := fs.String("invoices", "", "OCF of invoice records")
	paymentsPath := fs.String("payments", "", "OCF of payment records")
	pairsPath := fs.String("pairs", "", "OCF of allowed-pair records")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *invoicesPath == "" && *paymentsPath == "" && *pairsPath == "" {
		return fmt.Errorf("import requires at least one of -invoices, -payments, -pairs")
	}

	var batch reconciliation.ImportBatch
	if *invoicesPath != "" {
		invoices, err := readInvoices(*invoicesPath)
		if err != nil {
			return err
		}
		batch.Invoices = invoices
	}
	if *paymentsPath != "" {
		payments, err := readPayments(*paymentsPath)
		if err != nil {
			return err
		}
		batch.Payments = payments
	}
	if *pairsPath != "" {
		pairs, err := readPairs(*pairsPath)
		if err != nil {
			return err
		}
		batch.AllowedPairs = pairs
	}
	// One transaction: either the whole batch lands or nothing does.
	if err := store.Import(batch); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "imported %d invoices, %d payments, %d allowed pairs\n",
		len(batch.Invoices), len(batch.Payments), len(batch.AllowedPairs))
	return nil
}

func readInvoices(path string) ([]reconciliation.Invoice, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return reconciliation.ReadInvoicesOCF(f)
}

func readPayments(path string) ([]reconciliation.Payment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return reconciliation.ReadPaymentsOCF(f)
}

func readPairs(path string) ([]reconciliation.AllowedPair, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return reconciliation.ReadAllowedPairsOCF(f)
}

func runPlan(store *reconciliation.Store, args []string) error {
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit JSON instead of a table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	plan, err := store.Plan()
	if err != nil {
		return err
	}
	if *asJSON {
		return writeJSON(plan)
	}
	fmt.Printf("generation %d  source version %d  allocated %d  cost %d\n",
		plan.Generation, plan.SourceVersion, plan.TotalAllocated, plan.TotalCost)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "INVOICE\tPAYMENT\tAMOUNT\tUNIT COST\tCOST")
	for _, it := range plan.Items {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\n", it.InvoiceID, it.PaymentID, it.Amount, it.UnitCost, it.Cost)
	}
	return tw.Flush()
}

func runConfirm(store *reconciliation.Store, args []string) error {
	fs := flag.NewFlagSet("confirm", flag.ContinueOnError)
	generation := fs.Int64("generation", -1, "plan generation to confirm (required)")
	sourceVersion := fs.Int64("source-version", -1, "source version observed with the plan (required)")
	key := fs.String("key", "", "idempotency key; identical retries are replayed")
	itemsPath := fs.String("items", "", "JSON file of {invoiceId,paymentId,amount} items; empty confirms the whole plan")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *generation < 0 || *sourceVersion < 0 {
		return fmt.Errorf("confirm requires -generation and -source-version")
	}
	req := reconciliation.ConfirmationRequest{
		IdempotencyKey: *key,
		PlanGeneration: *generation,
		SourceVersion:  *sourceVersion,
	}
	if *itemsPath != "" {
		raw, err := os.ReadFile(*itemsPath)
		if err != nil {
			return err
		}
		var items []reconciliation.ConfirmItem
		if err := json.Unmarshal(raw, &items); err != nil {
			return fmt.Errorf("parse items: %w", err)
		}
		req.Items = items
	}
	res, err := store.Confirm(req)
	if err != nil {
		return err
	}
	if res.Idempotent {
		fmt.Fprintln(os.Stderr, "replayed stored confirmation")
	}
	return writeJSON(res)
}

func runExport(store *reconciliation.Store, args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	out := fs.String("out", "", "write Avro OCF to this file; empty writes Avro JSON to stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	res, err := store.Result()
	if err != nil {
		return err
	}
	if *out == "" {
		textual, err := reconciliation.EncodeResultTextual(res)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(append(textual, '\n'))
		return err
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := reconciliation.WriteResultOCF(f, res); err != nil {
		return err
	}
	return f.Close()
}

func runShow(store *reconciliation.Store, args []string) error {
	fs := flag.NewFlagSet("show", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit JSON instead of a table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	res, err := store.Result()
	if err != nil {
		return err
	}
	if *asJSON {
		return writeJSON(res)
	}
	fmt.Printf("partition %s/%s  source version %d  plan generation %d\n",
		res.Customer, res.Currency, res.SourceVersion, res.PlanGeneration)
	fmt.Printf("invoices %d (%d)  payments %d (%d)  confirmed %d  planned %d cost %d\n",
		len(res.Invoices), res.TotalInvoiceAmount, len(res.Payments), res.TotalPaymentAmount,
		res.TotalConfirmed, res.TotalPlanned, res.TotalPlannedCost)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KIND\tID\tAMOUNT\tCONFIRMED\tBALANCE\tSTATUS")
	for _, d := range res.Invoices {
		fmt.Fprintf(tw, "invoice\t%s\t%d\t%d\t%d\t%s\n", d.ID, d.Amount, d.ConfirmedAmount, d.Balance, d.Status)
	}
	for _, d := range res.Payments {
		fmt.Fprintf(tw, "payment\t%s\t%d\t%d\t%d\t%s\n", d.ID, d.Amount, d.ConfirmedAmount, d.Balance, d.Status)
	}
	return tw.Flush()
}

func writeJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
