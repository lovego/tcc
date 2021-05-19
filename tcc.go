package tcc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

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
	} else if _, err := tcc.update(`data = jsonb_set(data, '{Actions}'::text[],
		coalesce(data->'Actions', '[]'::jsonb) || `+quote(string(marshaledAction))+`::jsonb
	)`, statusTrying, "Try", nil); err != nil {
		return err
	}

	return action.Try()
}

var setTCCStatus = `data = jsonb_set(data, '{Status}'::text[], to_jsonb('%s'::text)), retry_at = now()`
var setConfirmed = fmt.Sprintf(setTCCStatus, statusConfirmed)
var setCanceled = fmt.Sprintf(setTCCStatus, statusCanceled)

func (tcc *TCC) Confirm() error {
	if _, err := tcc.update(setConfirmed, statusTrying, "Confirm", nil); err != nil {
		return err
	}
	tcc.msg.Data.(*tccData).Status = statusConfirmed
	tcc.engine.sqlmq.TriggerConsume()
	return nil
}

func (tcc *TCC) Cancel() error {
	if _, err := tcc.update(setCanceled, statusTrying, "Cancel", nil); err != nil {
		return err
	}
	tcc.msg.Data.(*tccData).Status = statusCanceled
	tcc.engine.sqlmq.TriggerConsume()
	return nil
}

func (tcc *TCC) update(set, assertStatus, method string, db sqlmq.DBOrTx) (bool, error) {
	if data := tcc.msg.Data.(*tccData); data.Status != assertStatus {
		return true, fmt.Errorf("tcc(%d) is %s, cann't %s", tcc.msg.Id, data.Status, method)
	}

	updateSql := fmt.Sprintf(`
	UPDATE %s
	SET %s
	WHERE id = %d AND queue = '%s' AND data->'Status' = to_jsonb('%s'::text)
	RETURNING id`,
		tcc.engine.mqTableName,
		set,
		tcc.msg.Id, tcc.engine.mqName, assertStatus,
	)
	if db == nil {
		db = tcc.engine.sqlmq.DB
	}
	ctx, cancel := sqlTimeout()
	defer cancel()
	if result, err := db.ExecContext(ctx, updateSql); err != nil {
		return false, errs.Trace(err)
	} else if n, err := result.RowsAffected(); err != nil {
		return false, errs.Trace(err)
	} else if n == 1 {
		return true, nil
	}

	return tcc.statusError(method, db)
}

func (tcc *TCC) statusError(method string, db sqlmq.DBOrTx) (bool, error) {
	querySql := fmt.Sprintf(`
	SELECT data->'Status'#>>'{}' as status
	FROM %s
	WHERE id = %d AND queue = '%s'`,
		tcc.engine.mqTableName,
		tcc.msg.Id, tcc.engine.mqName,
	)
	var nowStatus string
	ctx, cancel := sqlTimeout()
	defer cancel()
	if err := db.QueryRowContext(ctx, querySql).Scan(&nowStatus); err != nil {
		if err == sql.ErrNoRows {
			return true, fmt.Errorf("tcc(%d) not exists", tcc.msg.Id)
		}
		return false, errs.Trace(err)
	}
	return true, fmt.Errorf("tcc(%d) is %s, cann't %s", tcc.msg.Id, nowStatus, method)
}

func sqlTimeout() (context.Context, func()) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// quote a string, removing all zero byte('\000') in it.
func quote(s string) string {
	s = strings.Replace(s, "'", "''", -1)
	s = strings.Replace(s, "\000", "", -1)
	return "'" + s + "'"
}
