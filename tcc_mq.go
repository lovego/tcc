package tcc

import (
	"database/sql"
	"errors"
	"strings"
	"sync"
	"time"
)

// func for mq handling.
func (tcc *TCC) confirmOrCancel(tx *sql.Tx) (time.Duration, bool, error) {
	data := tcc.msg.Data.(*tccData)
	if data.Status != statusConfirmed && data.Status != statusCanceled {
		// cancel tcc if trying timeout
		data.Status = statusCanceled
		if canCommit, err := tcc.update(setCanceled, statusTrying, "Cancel", tx); err != nil {
			return 0, canCommit, err
		}
	}

	confirm := data.Status == statusConfirmed
	if data.Concurrent {
		if confirm {
			return tcc.confirmConcurrently(data, tx)
		} else {
			return tcc.cancelConcurrently(data, tx)
		}
	} else {
		if confirm {
			return tcc.confirmSerially(data, tx)
		} else {
			return tcc.cancelSerially(data, tx)
		}
	}
}

func (tcc *TCC) confirmConcurrently(data *tccData, tx *sql.Tx) (time.Duration, bool, error) {
	var retryAfter time.Duration
	var canCommit = true
	var errs []string
	var wg sync.WaitGroup
	for i, action := range data.Actions {
		if action.Status != statusConfirmed {
			wg.Add(1)
			go func(action tccAction, i int) {
				if _retryAfter, _canCommit, err := action.confirm(tcc, tx, i); err != nil {
					if _retryAfter > retryAfter {
						retryAfter = _retryAfter
					}
					canCommit = canCommit && _canCommit
					errs = append(errs, action.Name+": "+err.Error())
				}
				wg.Done()
			}(action, i)
		}
	}
	wg.Wait()
	if len(errs) == 0 {
		return 0, true, nil
	}
	return retryAfter, canCommit, errors.New(strings.Join(errs, "; "))
}

func (tcc *TCC) cancelConcurrently(data *tccData, tx *sql.Tx) (time.Duration, bool, error) {
	var retryAfter time.Duration
	var canCommit = true
	var errs []string
	var wg sync.WaitGroup
	for i, action := range data.Actions {
		if action.Status != statusCanceled {
			wg.Add(1)
			go func(action tccAction, i int) {
				if _retryAfter, _canCommit, err := action.cancel(tcc, tx, i); err != nil {
					if _retryAfter > retryAfter {
						retryAfter = _retryAfter
					}
					canCommit = canCommit && _canCommit
					errs = append(errs, action.Name+": "+err.Error())
				}
				wg.Done()
			}(action, i)
		}
	}
	wg.Wait()
	if len(errs) == 0 {
		return 0, true, nil
	}
	return retryAfter, canCommit, errors.New(strings.Join(errs, "; "))
}

func (tcc *TCC) confirmSerially(data *tccData, tx *sql.Tx) (time.Duration, bool, error) {
	for i, action := range data.Actions {
		if action.Status != statusConfirmed {
			if retryAfter, canCommit, err := action.confirm(tcc, tx, i); err != nil {
				return retryAfter, canCommit, errors.New(action.Name + ": " + err.Error())
			}
		}
	}
	return 0, true, nil
}

func (tcc *TCC) cancelSerially(data *tccData, tx *sql.Tx) (time.Duration, bool, error) {
	for i := len(data.Actions) - 1; i >= 0; i-- {
		action := data.Actions[i]
		if action.Status != statusCanceled {
			if retryAfter, canCommit, err := action.cancel(tcc, tx, i); err != nil {
				return retryAfter, canCommit, errors.New(action.Name + ": " + err.Error())
			}
		}
	}
	return 0, true, nil
}
