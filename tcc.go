package tcc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

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
	Status     string `json:",omitempty"`
	Concurrent bool   `json:",omitempty"` // do confirm or cancel concurrently or serially
	Actions    []struct {
		Name   string
		Action json.RawMessage
		action Action
	} `json:",omitempty"`
}

func (tcc *TCC) Try(action Action) error {
	if err := tcc.engine.checkAction(action); err != nil {
		return err
	}

	jsonAction, err := json.Marshal(action)
	if err != nil {
		return err
	}
	if err := tcc.update(`data = jsonb_set(data, '{Actions}'::text,
		coalesce(data->'Actions', '[]'::jsonb) || ` + quote(string(jsonAction)) + `::jsonb
	)`); err != nil {
		return err
	}

	return action.Try()
}

var confirm = fmt.Sprintf(`status = '%s', retry_at = now() `, statusConfirmed)
var cancel = fmt.Sprintf(`status = '%s', retry_at = now() `, statusCanceled)

func (tcc *TCC) Confirm() error {
	return tcc.update(confirm)
}

func (tcc *TCC) Cancel() error {
	return tcc.update(cancel)
}

var updateCond = fmt.Sprintf(`queue = '%s' AND data->'Status' == to_jsonb('%s'::text)`, queueName, statusTrying)

func (tcc *TCC) update(set string) error {
	if data := tcc.msg.Data.(*tccData); data.Status != statusTrying {
		return fmt.Errorf("this tcc is aready %s, cann't Try again.", data.Status)
	}

	sql := fmt.Sprintf(`
	UPDATE %s
	SET %s
	WHERE id = %d AND %s
	RETURNING id`,
		tcc.engine.mqTableName,
		set,
		tcc.msg.Id, updateCond,
	)

	var tccId int64
	if err := tcc.engine.sqlmq.DB.QueryRow(sql).Scan(&tccId); err != nil {
		return err
	}
	if tccId <= 0 || tccId != tcc.msg.Id {
		return fmt.Errorf(`this TCC not exists or not in trying status.`)
	}
	return nil
}

func (tcc *TCC) handle(tx *sql.Tx) error {
	return nil
}

// quote a string, removing all zero byte('\000') in it.
func quote(s string) string {
	s = strings.Replace(s, "'", "''", -1)
	s = strings.Replace(s, "\000", "", -1)
	return "'" + s + "'"
}
