package tcc

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/lovego/errs"
	"github.com/lovego/sqlmq"
)

func ExampleTCC_allSuccess() {
	runTest(testAction1{}, testAction2{})

	// Output:
}

func runTest(actions ...Action) {
	tccEngine := NewEngine(getMQ())
	tccEngine.Register(actions...)

	err := tccEngine.Run(10*time.Second, true, actions...)
	if err != nil {
		fmt.Println(errs.WithStack(err))
	}
}

type testAction1 struct {
}

func (ta testAction1) Name() string {
	return "action1"
}
func (ta testAction1) Try() error {
	fmt.Println("action1 Try")
	return nil
}
func (ta testAction1) Confirm() error {
	fmt.Println("action1 Confirm")
	return nil
}
func (ta testAction1) Cancel() error {
	fmt.Println("action1 Cancel")
	return nil
}

type testAction2 struct {
}

func (ta testAction2) Name() string {
	return "action2"
}
func (ta testAction2) Try() error {
	fmt.Println("action2 Try")
	return nil
}
func (ta testAction2) Confirm() error {
	fmt.Println("action2 Confirm")
	return nil
}
func (ta testAction2) Cancel() error {
	fmt.Println("action2 Cancel")
	return nil
}

var mq *sqlmq.SqlMQ

func getMQ() *sqlmq.SqlMQ {
	if mq != nil {
		return mq
	}
	db := getDB()
	if _, err := db.Exec("DROP TABLE IF EXISTS sqlmq"); err != nil {
		panic(err)
	}
	mq = &sqlmq.SqlMQ{
		DB:    db,
		Table: sqlmq.NewStdTable(db, "sqlmq"),
	}
	go mq.Consume()
	return mq
}

func getDB() *sql.DB {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost/postgres?sslmode=disable")
	if err != nil {
		panic(err)
	}
	return db
}
