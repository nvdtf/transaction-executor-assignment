package main

import (
	"sync"
)

// Start processes multiple blocks sequentially and returns the final account state
func Start(blocks []Block, initialState []AccountValue, numWorkers int) ([]AccountValue, error) {
	state := NewInMemoryAccountState(initialState)

	// Process each block sequentially
	for _, block := range blocks {
		if _, err := ExecuteBlock(block, state, numWorkers); err != nil {
			return nil, err
		}
	}

	return state.getSnapshot(), nil
}

type Block struct {
	Transactions []Transaction
}

type Transaction interface {
	Updates(AccountState) ([]AccountUpdate, error)
}

type AccountUpdate struct {
	Name          string
	BalanceChange int
}

type AccountValue struct {
	Name    string
	Balance uint
}

// AccountState interface for getting account information
type AccountState interface {
	GetAccount(name string) AccountValue
	ApplyUpdates([]AccountUpdate)
}

// ExecuteBlock takes a Block with transactions, and returns the updated account and with the updated balance.
func ExecuteBlock(block Block, state AccountState, numWorkers int) ([]AccountValue, error) {
	// Create channels for work distribution and result collection
	jobs := make(chan txJob, 1)
	results := make(chan txResult, 1)

	// Create worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}

	// Start a goroutine to close results channel after all workers finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process transactions sequentially
	for i, tx := range block.Transactions {
		// Send job with current state
		jobs <- txJob{
			transaction: tx,
			index:       i,
			state:       state,
		}

		// Get result
		result := <-results

		// Apply updates if transaction succeeded
		if result.err == nil {
			state.ApplyUpdates(result.updates)
		}
	}
	close(jobs)

	// Drain any remaining results
	for range results {
		// Drain channel
	}

	// Convert state to AccountValue slice
	if stateWithSnapshot, ok := state.(interface{ GetSnapshot() []AccountValue }); ok {
		return stateWithSnapshot.GetSnapshot(), nil
	}

	// If state doesn't support GetSnapshot, return empty slice
	return []AccountValue{}, nil
}

// txJob represents a transaction to be processed
type txJob struct {
	transaction Transaction
	index       int
	state       AccountState // Pass the current state to use
}

// txResult represents the result of processing a transaction
type txResult struct {
	updates []AccountUpdate
	index   int
	err     error
}

// worker processes transactions from the jobs channel
func worker(jobs <-chan txJob, results chan<- txResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		updates, err := job.transaction.Updates(job.state)
		results <- txResult{
			updates: updates,
			index:   job.index,
			err:     err,
		}
	}
}

// InMemoryAccountState implements AccountState with thread-safe operations
type InMemoryAccountState struct {
	accounts map[string]uint
	mu       sync.RWMutex
}

// NewInMemoryAccountState creates a new account state
func NewInMemoryAccountState(initialAccounts []AccountValue) *InMemoryAccountState {
	state := &InMemoryAccountState{
		accounts: make(map[string]uint),
	}

	for _, acc := range initialAccounts {
		state.accounts[acc.Name] = acc.Balance
	}

	return state
}

// GetAccount implements AccountState interface
func (s *InMemoryAccountState) GetAccount(name string) AccountValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	balance := s.accounts[name] // Returns 0 if account doesn't exist
	return AccountValue{
		Name:    name,
		Balance: balance,
	}
}

// applyUpdates applies a list of updates to the account state
func (s *InMemoryAccountState) applyUpdates(updates []AccountUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, update := range updates {
		currentBalance := s.accounts[update.Name]
		if update.BalanceChange >= 0 {
			s.accounts[update.Name] = currentBalance + uint(update.BalanceChange)
		} else {
			// Handle negative balance changes
			decrease := uint(-update.BalanceChange)
			if decrease > currentBalance {
				// This shouldn't happen if transaction validation is correct
				// but we protect against underflow just in case
				s.accounts[update.Name] = 0
			} else {
				s.accounts[update.Name] = currentBalance - decrease
			}
		}
	}
}

// getSnapshot returns the current state of all accounts
func (s *InMemoryAccountState) getSnapshot() []AccountValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]AccountValue, 0, len(s.accounts))
	for name, balance := range s.accounts {
		result = append(result, AccountValue{
			Name:    name,
			Balance: balance,
		})
	}
	return result
}

// Update InMemoryAccountState to implement the new interface method
func (s *InMemoryAccountState) ApplyUpdates(updates []AccountUpdate) {
	s.applyUpdates(updates)
}

// GetSnapshot returns the current state of all accounts
func (s *InMemoryAccountState) GetSnapshot() []AccountValue {
	return s.getSnapshot()
}
