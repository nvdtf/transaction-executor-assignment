package main

import (
	"fmt"
	"testing"
)

// transfer implements Transaction interface for testing
type transfer struct {
	from  string
	to    string
	value int
}

func (t transfer) Updates(state AccountState) ([]AccountUpdate, error) {
	fromAcc := state.GetAccount(t.from)
	if fromAcc.Balance < uint(t.value) {
		return nil, fmt.Errorf("insufficient balance: account %s has %d, needs %d", t.from, fromAcc.Balance, t.value)
	}

	return []AccountUpdate{
		{Name: t.from, BalanceChange: -t.value},
		{Name: t.to, BalanceChange: t.value},
	}, nil
}

func TestStart_Example1(t *testing.T) {
	// Initial state setup
	initialState := []AccountValue{
		{Name: "A", Balance: 20},
		{Name: "B", Balance: 30},
		{Name: "C", Balance: 40},
	}

	// Create block with transactions
	blocks := []Block{{
		Transactions: []Transaction{
			transfer{from: "A", to: "B", value: 5},  // A->B: 5
			transfer{from: "B", to: "C", value: 10}, // B->C: 10
			transfer{from: "B", to: "C", value: 30}, // B->C: 30 (should fail)
		},
	}}

	// Execute blocks
	result, err := Start(blocks, initialState, 4)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify results
	expected := map[string]uint{
		"A": 15,
		"B": 25,
		"C": 50,
	}

	verifyResults(t, result, expected)
}

func TestStart_Example2(t *testing.T) {
	// Initial state setup
	initialState := []AccountValue{
		{Name: "A", Balance: 10},
		{Name: "B", Balance: 20},
		{Name: "C", Balance: 30},
		{Name: "D", Balance: 40},
	}

	// Create block with transactions
	blocks := []Block{{
		Transactions: []Transaction{
			transfer{from: "A", to: "B", value: 5},  // A->B: 5
			transfer{from: "C", to: "D", value: 10}, // C->D: 10
		},
	}}

	// Execute blocks
	result, err := Start(blocks, initialState, 4)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify results
	expected := map[string]uint{
		"A": 5,
		"B": 25,
		"C": 20,
		"D": 50,
	}

	verifyResults(t, result, expected)
}

func TestStart_MultipleBlocks(t *testing.T) {
	initialState := []AccountValue{
		{Name: "A", Balance: 100},
		{Name: "B", Balance: 100},
		{Name: "C", Balance: 100},
	}

	// Create multiple blocks
	blocks := []Block{
		{
			Transactions: []Transaction{
				transfer{from: "A", to: "B", value: 50}, // Block 1: A->B: 50
			},
		},
		{
			Transactions: []Transaction{
				transfer{from: "B", to: "C", value: 30}, // Block 2: B->C: 30
			},
		},
		{
			Transactions: []Transaction{
				transfer{from: "C", to: "A", value: 20}, // Block 3: C->A: 20
			},
		},
	}

	// Execute multiple times to ensure deterministic results
	var firstResult []AccountValue
	for i := 0; i < 5; i++ {
		result, err := Start(blocks, initialState, 4)
		if err != nil {
			t.Fatalf("Start failed on iteration %d: %v", i, err)
		}

		if i == 0 {
			firstResult = result
			continue
		}

		// Compare with first result to ensure deterministic execution
		if !compareResults(firstResult, result) {
			t.Errorf("Non-deterministic results detected on iteration %d", i)
		}
	}

	// Verify final balances
	expected := map[string]uint{
		"A": 70,  // 100 - 50 + 20
		"B": 120, // 100 + 50 - 30
		"C": 110, // 100 + 30 - 20
	}

	verifyResults(t, firstResult, expected)
}

// Helper function to verify results
func verifyResults(t *testing.T, result []AccountValue, expected map[string]uint) {
	t.Helper()
	if len(result) != len(expected) {
		t.Errorf("Expected %d accounts, got %d", len(expected), len(result))
	}

	for _, acc := range result {
		expectedBalance, exists := expected[acc.Name]
		if !exists {
			t.Errorf("Unexpected account %s in result", acc.Name)
			continue
		}
		if acc.Balance != expectedBalance {
			t.Errorf("Account %s: expected balance %d, got %d", acc.Name, expectedBalance, acc.Balance)
		}
	}
}

// compareResults compares two sets of account values
func compareResults(a, b []AccountValue) bool {
	if len(a) != len(b) {
		return false
	}

	aMap := make(map[string]uint)
	for _, acc := range a {
		aMap[acc.Name] = acc.Balance
	}

	for _, acc := range b {
		if aMap[acc.Name] != acc.Balance {
			return false
		}
	}

	return true
}

func TestStart_ConcurrentTransactions(t *testing.T) {
	initialState := []AccountValue{
		{Name: "A1", Balance: 100},
		{Name: "B1", Balance: 100},
		{Name: "A2", Balance: 100},
		{Name: "B2", Balance: 100},
		{Name: "A3", Balance: 100},
		{Name: "B3", Balance: 100},
	}

	// Create transactions that can be executed concurrently
	var transactions []Transaction
	for i := 1; i <= 3; i++ {
		transactions = append(transactions,
			transfer{from: fmt.Sprintf("A%d", i), to: fmt.Sprintf("B%d", i), value: 50},
		)
	}

	blocks := []Block{{Transactions: transactions}}

	// Execute multiple times to ensure deterministic results
	var firstResult []AccountValue
	for i := 0; i < 5; i++ {
		result, err := Start(blocks, initialState, 4)
		if err != nil {
			t.Fatalf("Start failed on iteration %d: %v", i, err)
		}

		if i == 0 {
			firstResult = result
			continue
		}

		// Compare with first result to ensure deterministic execution
		if !compareResults(firstResult, result) {
			t.Errorf("Non-deterministic results detected on iteration %d", i)
		}
	}

	// Verify final balances
	expected := map[string]uint{
		"A1": 50,  // 100 - 50
		"B1": 150, // 100 + 50
		"A2": 50,  // 100 - 50
		"B2": 150, // 100 + 50
		"A3": 50,  // 100 - 50
		"B3": 150, // 100 + 50
	}

	verifyResults(t, firstResult, expected)
}

// Add new test for different worker counts
func TestStart_DifferentWorkerCounts(t *testing.T) {
	initialState := []AccountValue{
		{Name: "A", Balance: 1000},
		{Name: "B", Balance: 1000},
		{Name: "C", Balance: 1000},
		{Name: "D", Balance: 1000},
		{Name: "E", Balance: 1000},
	}

	// Create a mix of dependent and independent transactions
	transactions := []Transaction{
		transfer{from: "A", to: "B", value: 100}, // T1: A->B
		transfer{from: "C", to: "D", value: 200}, // T2: C->D (independent from T1)
		transfer{from: "B", to: "E", value: 50},  // T3: depends on T1
		transfer{from: "D", to: "A", value: 75},  // T4: depends on T2
		transfer{from: "E", to: "C", value: 25},  // T5: depends on T3
	}

	blocks := []Block{{Transactions: transactions}}

	// Test with different worker counts
	workerCounts := []int{1, 2, 4, 8, 16}
	var firstResult []AccountValue

	for i, numWorkers := range workerCounts {
		result, err := Start(blocks, initialState, numWorkers)
		if err != nil {
			t.Fatalf("Start failed with %d workers: %v", numWorkers, err)
		}

		if i == 0 {
			firstResult = result
			continue
		}

		// Compare with first result to ensure deterministic execution
		if !compareResults(firstResult, result) {
			t.Errorf("Results with %d workers differ from results with %d workers",
				numWorkers, workerCounts[0])

			t.Logf("Expected (with %d workers): %+v", workerCounts[0], firstResult)
			t.Logf("Got (with %d workers): %+v", numWorkers, result)
		}
	}

	// Verify final balances
	expected := map[string]uint{
		"A": 975,  // 1000 - 100 + 75
		"B": 1050, // 1000 + 100 - 50
		"C": 825,  // 1000 - 200 + 25
		"D": 1125, // 1000 + 200 - 75
		"E": 1025, // 1000 + 50 - 25
	}

	verifyResults(t, firstResult, expected)
}
