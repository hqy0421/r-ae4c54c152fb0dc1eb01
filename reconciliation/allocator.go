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
	"math"
	"sort"
	"strings"
)

// Edge is one allowed invoice-to-payment pair supplied to Optimize.
type Edge struct {
	InvoiceID string
	PaymentID string
	UnitCost  int64
	// MaxAmount caps the total allocated along the edge. Zero means unlimited.
	MaxAmount int64
}

// Optimize solves the exact partial write-off problem. It first maximizes the
// total amount written off, and among all maximum-amount solutions it
// minimizes the sum of UnitCost*Amount over the selected pairs. All amounts are
// integers in minor currency units.
//
// The inputs are the remaining (unconfirmed) balances of each invoice and
// payment. The returned allocations are deterministic: ordered by invoice
// identifier then payment identifier, and ties across equally optimal
// solutions are broken by the input ordering of invoices, payments, and edges
// after sorting those by identifier.
//
// The problem is a transportation problem with integer capacities and costs,
// solved as a min-cost max-flow. Because all capacities are integers, every
// augmentation moves an integral amount, so the optimum is integral.
func Optimize(invoices []Balance, payments []Balance, edges []Edge) ([]PlannedAllocation, int64, int64, error) {
	inv, err := normalizeBalances(invoices, "invoice")
	if err != nil {
		return nil, 0, 0, err
	}
	pay, err := normalizeBalances(payments, "payment")
	if err != nil {
		return nil, 0, 0, err
	}
	if len(inv) == 0 || len(pay) == 0 || len(edges) == 0 {
		return nil, 0, 0, nil
	}

	invIndex := make(map[string]int, len(inv))
	for i, b := range inv {
		invIndex[b.ID] = i
	}
	payIndex := make(map[string]int, len(pay))
	for i, b := range pay {
		payIndex[b.ID] = i
	}

	type parsedEdge struct {
		i, j      int
		unitCost  int64
		maxAmount int64
	}
	parsed := make([]parsedEdge, 0, len(edges))
	seen := make(map[[2]string]struct{}, len(edges))
	for _, e := range edges {
		if e.UnitCost < 0 {
			return nil, 0, 0, fmt.Errorf("%w: negative unit cost %d for %s->%s", ErrInvalid, e.UnitCost, e.InvoiceID, e.PaymentID)
		}
		if e.MaxAmount < 0 {
			return nil, 0, 0, fmt.Errorf("%w: negative max amount %d for %s->%s", ErrInvalid, e.MaxAmount, e.InvoiceID, e.PaymentID)
		}
		i, ok := invIndex[e.InvoiceID]
		if !ok {
			continue // balance absent or zero
		}
		j, ok := payIndex[e.PaymentID]
		if !ok {
			continue
		}
		key := [2]string{e.InvoiceID, e.PaymentID}
		if _, dup := seen[key]; dup {
			return nil, 0, 0, fmt.Errorf("%w: duplicate allowed pair %s->%s", ErrConflict, e.InvoiceID, e.PaymentID)
		}
		seen[key] = struct{}{}
		parsed = append(parsed, parsedEdge{i: i, j: j, unitCost: e.UnitCost, maxAmount: e.MaxAmount})
	}
	if len(parsed) == 0 {
		return nil, 0, 0, nil
	}
	sort.SliceStable(parsed, func(a, b int) bool {
		if parsed[a].i != parsed[b].i {
			return parsed[a].i < parsed[b].i
		}
		return parsed[b].j > parsed[a].j
	})

	nInv := len(inv)
	nPay := len(pay)
	source := 0
	invBase := 1
	payBase := invBase + nInv
	sink := payBase + nPay
	nodeCount := sink + 1

	g := newMCFGraph(nodeCount)
	constEdgeCap, err := maxFlowBound(inv, pay)
	if err != nil {
		return nil, 0, 0, err
	}
	type locatedEdge struct {
		from int
		idx  int
	}
	edgeIDs := make([]locatedEdge, len(parsed))
	for k, pe := range parsed {
		cap := pe.maxAmount
		if cap == 0 || cap > constEdgeCap {
			cap = constEdgeCap
		}
		from := invBase + pe.i
		edgeIDs[k] = locatedEdge{from: from, idx: g.addEdge(from, payBase+pe.j, cap, pe.unitCost)}
	}
	for i, b := range inv {
		g.addEdge(source, invBase+i, b.Remaining, 0)
	}
	for j, b := range pay {
		g.addEdge(payBase+j, sink, b.Remaining, 0)
	}

	fr := g.minCostMaxFlow(source, sink)

	// Convert flow on each pair edge back into allocations.
	byPair := make(map[[2]string]int64, len(parsed))
	for k, pe := range parsed {
		amount := g.flowOnEdge(edgeIDs[k].from, edgeIDs[k].idx)
		if amount <= 0 {
			continue
		}
		byPair[[2]string{inv[pe.i].ID, pay[pe.j].ID}] += amount
	}

	pairKeys := make([][2]string, 0, len(byPair))
	for k := range byPair {
		pairKeys = append(pairKeys, k)
	}
	sort.Slice(pairKeys, func(a, b int) bool {
		if pairKeys[a][0] != pairKeys[b][0] {
			return pairKeys[a][0] < pairKeys[b][0]
		}
		return pairKeys[a][1] < pairKeys[b][1]
	})

	costOf := make(map[[2]string]int64, len(parsed))
	for _, pe := range parsed {
		costOf[[2]string{inv[pe.i].ID, pay[pe.j].ID}] = pe.unitCost
	}

	var totalAmount, totalCost int64
	out := make([]PlannedAllocation, 0, len(pairKeys))
	for _, k := range pairKeys {
		amount := byPair[k]
		unitCost := costOf[k]
		cost, err := safeMul(amount, unitCost)
		if err != nil {
			return nil, 0, 0, err
		}
		totalAmount, err = safeAdd(totalAmount, amount)
		if err != nil {
			return nil, 0, 0, err
		}
		totalCost, err = safeAdd(totalCost, cost)
		if err != nil {
			return nil, 0, 0, err
		}
		out = append(out, PlannedAllocation{
			InvoiceID: k[0],
			PaymentID: k[1],
			Amount:    amount,
			UnitCost:  unitCost,
			Cost:      cost,
		})
	}
	if fr != totalAmount {
		return nil, 0, 0, fmt.Errorf("%w: total flow %d disagrees with planned allocations %d", ErrInvalid, fr, totalAmount)
	}
	return out, totalAmount, totalCost, nil
}

func normalizeBalances(in []Balance, side string) ([]Balance, error) {
	seen := make(map[string]struct{}, len(in))
	out := make([]Balance, 0, len(in))
	for _, b := range in {
		id := strings.TrimSpace(b.ID)
		if id == "" {
			return nil, fmt.Errorf("%w: %s balance with empty identifier", ErrInvalid, side)
		}
		if b.Remaining < 0 {
			return nil, fmt.Errorf("%w: %s %s has negative remaining balance %d", ErrInvalid, side, id, b.Remaining)
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("%w: duplicate %s balance %s", ErrConflict, side, id)
		}
		seen[id] = struct{}{}
		if b.Remaining == 0 {
			continue
		}
		out = append(out, Balance{ID: id, Remaining: b.Remaining})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func maxFlowBound(inv, pay []Balance) (int64, error) {
	var si, sp int64
	var err error
	for _, b := range inv {
		if si, err = safeAdd(si, b.Remaining); err != nil {
			return 0, err
		}
	}
	for _, b := range pay {
		if sp, err = safeAdd(sp, b.Remaining); err != nil {
			return 0, err
		}
	}
	if sp < si {
		return sp, nil
	}
	return si, nil
}

func safeAdd(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, fmt.Errorf("%w: integer overflow adding %d and %d", ErrInvalid, a, b)
	}
	return a + b, nil
}

func safeMul(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("%w: negative factor in %d*%d", ErrInvalid, a, b)
	}
	if a > math.MaxInt64/b {
		return 0, fmt.Errorf("%w: integer overflow multiplying %d and %d", ErrInvalid, a, b)
	}
	return a * b, nil
}

// min-cost max-flow with successive shortest augmenting paths. Costs are
// non-negative on forward edges; residual reverse edges have negative cost, so
// shortest paths use SPFA, which also handles them without potentials. Node and
// edge traversal order is insertion order, making results deterministic.
type mcfEdge struct {
	to, rev int
	cap     int64
	cost    int64
}

type mcfGraph struct {
	adj [][]mcfEdge
}

func newMCFGraph(n int) *mcfGraph {
	return &mcfGraph{adj: make([][]mcfEdge, n)}
}

// addEdge appends a forward edge and its residual counterpart, returning the
// index of the forward edge in its adjacency slice.
func (g *mcfGraph) addEdge(from, to int, cap, cost int64) int {
	fwd := len(g.adj[from])
	rev := len(g.adj[to])
	g.adj[from] = append(g.adj[from], mcfEdge{to: to, rev: rev, cap: cap, cost: cost})
	g.adj[to] = append(g.adj[to], mcfEdge{to: from, rev: fwd, cap: 0, cost: -cost})
	return fwd
}

// flowOnEdge reports the flow carried by the forward edge at index idx in the
// adjacency list of node from. It reads the residual capacity of the paired
// reverse edge, which equals the flow sent along the forward edge.
func (g *mcfGraph) flowOnEdge(from, idx int) int64 {
	e := g.adj[from][idx]
	return g.adj[e.to][e.rev].cap
}

// minCostMaxFlow augments along shortest (cheapest) paths until no path from s
// to t carries positive capacity. Returns the maximum flow.
func (g *mcfGraph) minCostMaxFlow(s, t int) int64 {
	n := len(g.adj)
	const inf = int64(math.MaxInt64)
	var totalFlow int64
	dist := make([]int64, n)
	inQueue := make([]bool, n)
	prevNode := make([]int, n)
	prevEdge := make([]int, n)

	for {
		for i := 0; i < n; i++ {
			dist[i] = inf
			inQueue[i] = false
			prevNode[i] = -1
			prevEdge[i] = -1
		}
		dist[s] = 0
		queue := []int{s}
		inQueue[s] = true
		for len(queue) > 0 {
			u := queue[0]
			queue = queue[1:]
			inQueue[u] = false
			for ei, e := range g.adj[u] {
				if e.cap <= 0 {
					continue
				}
				if nd, ok := addNoOverflow(dist[u], e.cost); ok && nd < dist[e.to] {
					dist[e.to] = nd
					prevNode[e.to] = u
					prevEdge[e.to] = ei
					if !inQueue[e.to] {
						inQueue[e.to] = true
						queue = append(queue, e.to)
					}
				}
			}
		}
		if dist[t] == inf {
			break
		}

		// Bottleneck along the path. Break ties toward the first (lowest
		// index) predecessor, which the deterministic relaxation above
		// already selects.
		bottleneck := inf
		for v := t; v != s; v = prevNode[v] {
			e := g.adj[prevNode[v]][prevEdge[v]]
			if e.cap < bottleneck {
				bottleneck = e.cap
			}
		}
		if bottleneck <= 0 {
			break
		}
		for v := t; v != s; v = prevNode[v] {
			u := prevNode[v]
			ei := prevEdge[v]
			e := &g.adj[u][ei]
			e.cap -= bottleneck
			g.adj[e.to][e.rev].cap += bottleneck
		}
		totalFlow += bottleneck
	}
	return totalFlow
}

func addNoOverflow(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}
