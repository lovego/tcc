package tcc

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/lovego/errs"
	"github.com/lovego/sqlmq"
)

const (
	statusTrying    = "trying"
	statusConfirmed = "confirmed"
	statusCanceled  = "canceled"
)

type TCC struct {
	engine *Engine
	msg    *sqlmq.StdMessage
}

type tccData struct {
	Status     string      `json:",omitempty"`
	Concurrent bool        `json:",omitempty"` // if should do confirm or cancel concurrently.
	Actions    []tccAction `json:",omitempty"`
}

func (tcc *TCC) Try(action Action) error {
	if err := tcc.engine.checkAction(action); err != nil {
		return err
	}
	if marshaledAction, err := marshalAction(action); err != nil {
		return err
	} else if _, err := tcc.update(`data = jsonb_set(data, '{Actions}'::text,
		coalesce(data->'Actions', '[]'::jsonb) || `+quote(string(marshaledAction))+`::jsonb
	)`, statusTrying, nil); err != nil {
		return err
	}

	return action.Try()
}

var confirmSet = fmt.Sprintf(`status = '%s', retry_at = now()`, statusConfirmed)
var cancelSet = fmt.Sprintf(`status = '%s', retry_at = now()`, statusCanceled)

func (tcc *TCC) Confirm() error {
	if _, err := tcc.update(confirmSet, statusTrying, nil); err != nil {
		return err
	}
	tcc.engine.sqlmq.TriggerConsume()
	return nil
}

func (tcc *TCC) Cancel() error {
	if _, err := tcc.update(cancelSet, statusTrying, nil); err != nil {
		return err
	}
	tcc.engine.sqlmq.TriggerConsume()
	return nil
}

func (tcc *TCC) update(set, assertStatus string, db sqlmq.DBOrTx) (bool, error) {
	if data := tcc.msg.Data.(*tccData); data.Status != statusTrying {
		return true, fmt.Errorf("this tcc is aready %s, cann't Try again.", data.Status)
	}

	updateSql := fmt.Sprintf(`
	UPDATE %s
	SET %s
	WHERE id = %d AND queue = '%s' AND data->'Status' = to_jsonb('%s'::text)
	RETURNING id`,
		tcc.engine.mqTableName,
		set,
		tcc.msg.Id, queueName, assertStatus,
	)
	if db == nil {
		db = tcc.engine.sqlmq.DB
	}
	if result, err := db.Exec(updateSql); err != nil {
		return false, errs.Trace(err)
	} else if n, err := result.RowsAffected(); err != nil {
		return false, errs.Trace(err)
	} else if n == 1 {
		return true, nil
	}

	querySql := fmt.Sprintf(`
	SELECT data->'Status'#>>'{}' as status
	FROM %s
	WHERE id = %d AND queue = '%s'`,
		tcc.engine.mqTableName,
		tcc.msg.Id, queueName,
	)
	var nowStatus string
	if err := db.QueryRow(querySql).Scan(&nowStatus); err != nil {
		if err == sql.ErrNoRows {
			return true, fmt.Errorf("tcc(%d) not exists.", tcc.msg.Id)
		}
		return false, errs.Trace(err)
	}
	return true, fmt.Errorf("this tcc(%d) is %s, not %s.", tcc.msg.Id, nowStatus, assertStatus)
}

// quote a string, removing all zero byte('\000') in it.
func quote(s string) string {
	s = strings.Replace(s, "'", "''", -1)
	s = strings.Replace(s, "\000", "", -1)
	return "'" + s + "'"
}

func (tcc *TCC) confirmOrCancel(tx *sql.Tx) (bool, error) {
	data := tcc.msg.Data.(*tccData)
	if data.Status != statusConfirmed && data.Status != statusCanceled {
		// cancel tcc if trying timeout
		data.Status = statusCanceled
		if canCommit, err := tcc.update(cancelSet, statusTrying, tx); err != nil {
			return canCommit, err
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

func (tcc *TCC) confirmConcurrently(data *tccData, tx *sql.Tx) (bool, error) {
	var canCommit = true
	var errs []string
	var wg sync.WaitGroup
	for i, action := range data.Actions {
		if action.Status != statusConfirmed {
			wg.Add(1)
			go func(action tccAction, i int) {
				if _canCommit, err := action.confirm(tcc, tx, i); err != nil {
					canCommit = canCommit && _canCommit
					errs = append(errs, action.Name+": "+err.Error())
				}
				wg.Done()
			}(action, i)
		}
	}
	wg.Wait()
	if len(errs) == 0 {
		return true, nil
	}
	return canCommit, errors.New(strings.Join(errs, "; "))
}

func (tcc *TCC) cancelConcurrently(data *tccData, tx *sql.Tx) (bool, error) {
	var canCommit = true
	var errs []string
	var wg sync.WaitGroup
	for i, action := range data.Actions {
		if action.Status != statusCanceled {
			wg.Add(1)
			go func(action tccAction, i int) {
				if _canCommit, err := action.cancel(tcc, tx, i); err != nil {
					canCommit = canCommit && _canCommit
					errs = append(errs, action.Name+": "+err.Error())
				}
				wg.Done()
			}(action, i)
		}
	}
	wg.Wait()
	if len(errs) == 0 {
		return true, nil
	}
	return canCommit, errors.New(strings.Join(errs, "; "))
}

func (tcc *TCC) confirmSerially(data *tccData, tx *sql.Tx) (bool, error) {
	for i, action := range data.Actions {
		if action.Status != statusConfirmed {
			if canCommit, err := action.confirm(tcc, tx, i); err != nil {
				return canCommit, errors.New(action.Name + ": " + err.Error())
			}
		}
	}
	return true, nil
}

func (tcc *TCC) cancelSerially(data *tccData, tx *sql.Tx) (bool, error) {
	for i := len(data.Actions) - 1; i >= 0; i-- {
		action := data.Actions[i]
		if action.Status != statusCanceled {
			if canCommit, err := action.cancel(tcc, tx, i); err != nil {
				return canCommit, errors.New(action.Name + ": " + err.Error())
			}
		}
	}
	return true, nil
}
