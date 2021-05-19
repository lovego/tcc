package tcc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type tccAction struct {
	Name   string `json:",omitempty"`
	Raw    json.RawMessage
	Status string `json:",omitempty"`
}

func marshalAction(action Action) ([]byte, error) {
	actionJson, err := json.Marshal(action)
	if err != nil {
		return nil, err
	}
	return json.Marshal(tccAction{
		Name: action.Name(),
		Raw:  json.RawMessage(actionJson),
	})
}

func (ta tccAction) confirm(tcc *TCC, tx *sql.Tx, actionIndex int) (time.Duration, bool, error) {
	action, err := tcc.engine.unmarshalAction(ta.Name, ta.Raw)
	if err != nil {
		return time.Hour, true, err
	}
	if err := action.Confirm(); err != nil {
		return 0, true, err
	}
	return setActionStatus(tcc, tx, actionIndex, statusConfirmed, "confirm action")
}

func (ta tccAction) cancel(tcc *TCC, tx *sql.Tx, actionIndex int) (time.Duration, bool, error) {
	action, err := tcc.engine.unmarshalAction(ta.Name, ta.Raw)
	if err != nil {
		return time.Hour, true, err
	}
	if err := action.Cancel(); err != nil {
		return 0, true, err
	}
	return setActionStatus(tcc, tx, actionIndex, statusCanceled, "cancel action")
}

func setActionStatus(
	tcc *TCC, tx *sql.Tx, actionIndex int, status, method string,
) (time.Duration, bool, error) {
	setSql := fmt.Sprintf(
		`data = jsonb_set(data, '{Actions,%d,Status}'::text[], to_jsonb('%s'::text))`,
		actionIndex, status,
	)
	canCommit, err := tcc.update(setSql, status, method, tx)
	return 0, canCommit, err
}
