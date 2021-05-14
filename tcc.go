package tcc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

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
	data := tcc.msg.Data.(*tccData)
	if data.Status != statusTrying {
		return fmt.Errorf("this tcc is aready %s, cann't Try again.", data.Status)
	}
	sql := fmt.Sprintf(`
	UPDATE %s
	SET data = jsonb_set(data)
	WHERE queue = '%s' AND data->'status' == to_jsonb('%s'::text) AND id = `,
		tcc.engine.mqTableName, queueName, statusTrying,
	)

	strconv.FormatInt(tcc.msg.Id, 10)

	tcc.engine.sqlmq.DB.Query()
	return action.Try()
}

func (tcc *TCC) Confirm() error {
}

func (tcc *TCC) Cancel() error {
}

func (tcc *TCC) handle(tx *sql.Tx) error {
}
