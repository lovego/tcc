package tcc

import (
	"database/sql"
	"encoding/json"
	"fmt"
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

func (ta tccAction) confirm(tcc *TCC, tx *sql.Tx, actionIndex int) error {
	action, err := tcc.engine.unmarshalAction(ta.Name, ta.Raw)
	if err != nil {
		return err
	}
	if err := action.Confirm(); err != nil {
		return err
	}
	return setActionStatus(tcc, tx, actionIndex, statusConfirmed)
}

func (ta tccAction) cancel(tcc *TCC, tx *sql.Tx, actionIndex int) error {
	action, err := tcc.engine.unmarshalAction(ta.Name, ta.Raw)
	if err != nil {
		return err
	}
	if err := action.Cancel(); err != nil {
		return err
	}
	return setActionStatus(tcc, tx, actionIndex, statusCanceled)
}

func setActionStatus(tcc *TCC, tx *sql.Tx, actionIndex int, status string) error {
	setSql := fmt.Sprintf(
		`data = jsonb_set(data, '{Actions,%d,Status}'::text[], to_jsonb('%s'::text))`,
		actionIndex, status,
	)
	return tcc.update(setSql, status, tx)
}
