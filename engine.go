package tcc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/lovego/sqlmq"
)

type Engine struct {
	sqlmq       *sqlmq.SqlMQ
	mqTableName string
	actions     map[string]Action
	mutex       sync.RWMutex
}

type Action interface {
	Name() string
	Try() error
	Confirm() error
	Cancel() error
}

const queueName = "tccTransaction"

func NewEngine(mq *sqlmq.SqlMQ) *Engine {
	stdTable := mq.Table.(*sqlmq.StdTable)
	engine := &Engine{sqlmq: mq, mqTableName: stdTable.Name(), actions: make(map[string]Action)}
	mq.Register(queueName, engine.handle)
	return engine
}

func (engine *Engine) Register(actions ...Action) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	for _, action := range actions {
		if name := action.Name(); engine.actions[name] != nil {
			panic(time.Now().Format(time.RFC3339Nano) + " action " + name + " aready registered.")
		} else {
			engine.actions[name] = action
		}
	}
}

var errTccId = errors.New("tcc id error")

func (engine *Engine) New(timeout time.Duration, concurrent bool) (*TCC, error) {
	now := time.Now()

	msg := &sqlmq.StdMessage{
		Queue: queueName,
		Data: &tccData{
			Status:     statusTrying,
			Concurrent: concurrent,
		},
		CreatedAt: now,
		RetryAt:   now.Add(timeout),
	}
	if err := engine.sqlmq.Produce(nil, msg); err != nil {
		return nil, err
	}
	if msg.Id <= 0 {
		return nil, errTccId
	}
	return &TCC{engine: engine, msg: msg}, nil
}

func (engine *Engine) Run(timeout time.Duration, concurrent bool, actions ...Action) error {
	tcc, err := engine.New(timeout, concurrent)
	if err != nil {
		return err
	}
	for _, action := range actions {
		if err := tcc.Try(action); err != nil {
			if err2 := tcc.Cancel(); err2 != nil {
				engine.sqlmq.Logger.Error(err2)
			}
			return err
		}
	}
	return tcc.Confirm()
}

func (engine *Engine) checkAction(tried Action) error {
	name := tried.Name()
	engine.mutex.RLock()
	registered := engine.actions[name]
	engine.mutex.RUnlock()

	if registered == nil {
		return fmt.Errorf("action %s is not registerd.", name)
	}
	if reflect.TypeOf(registered) != reflect.TypeOf(tried) {
		return fmt.Errorf(
			`action %s has been "Register"ed with type "%T", but "Try"ed with type "%T"`,
			name, registered, tried,
		)
	}
	return nil
}

func (engine *Engine) handle(ctx context.Context, tx *sql.Tx, message sqlmq.Message) (
	time.Duration, bool, error,
) {
	msg := message.(*sqlmq.StdMessage)
	data := &tccData{}
	if err := json.Unmarshal(msg.Data.([]byte), &data); err != nil {
		return time.Hour, true, err
	}
	msg.Data = data
	if canCommit, err := (&TCC{engine: engine, msg: msg}).confirmOrCancel(tx); err != nil {
		var retryAfter time.Duration
		if _, ok := err.(unmarshalError); ok {
			retryAfter = time.Hour
		} else {
			retryAfter = getRetryAfter(int(msg.TryCount))
		}
		return retryAfter, canCommit, err
	}
	return 0, true, nil
}

type unmarshalError struct {
	error
}

func (engine *Engine) unmarshalAction(name string, b []byte) (Action, error) {
	engine.mutex.RLock()
	action := engine.actions[name]
	engine.mutex.RUnlock()

	if action == nil {
		return nil, unmarshalError{fmt.Errorf("action %s is not registerd.", name)}
	}
	actionPointer := reflect.New(reflect.TypeOf(action))
	if err := json.Unmarshal(b, actionPointer.Interface()); err != nil {
		return nil, unmarshalError{err}
	}
	return actionPointer.Elem().Interface().(Action), nil
}

var retryPeriods = []time.Duration{
	3 * time.Second,
	30 * time.Second,
	300 * time.Second,
	time.Hour,
}

func getRetryAfter(tryCount int) time.Duration {
	if tryCount >= len(retryPeriods) {
		tryCount = len(retryPeriods) - 1
	}
	return retryPeriods[tryCount]
}
