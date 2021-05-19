package tcc

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/lovego/errs"
	"github.com/lovego/sqlmq"
)

var testDB *sql.DB
var testMQ *sqlmq.SqlMQ
var tccEngine *Engine

func init() {
	testDB = getDB()
	testMQ = getMQ()
	tccEngine = NewEngine("test", testMQ)
	tccEngine.Register(testAction1{}, testAction2{}, testAction3{})
}

func ExampleTCC_success() {
	runTest(false, testAction1{}, testAction2{})
	// Output:
	// action1 Try
	// action2 Try
	// action1 Confirm
	// action2 Confirm
}

func ExampleTCC_success_concurrent() {
	runTest(true, testAction1{}, testAction2{})
	// Output:
	// action1 Try
	// action2 Try
	// action1 Confirm
	// action2 Confirm
}

func ExampleTCC_fail() {
	runTest(false, testAction1{}, testAction3{}, testAction2{})
	// Output:
	// action1 Try
	// action3 Try
	// error happened
	// action3 Cancel
	// action1 Cancel
}

func ExampleTCC_fail_concurrent() {
	runTest(true, testAction1{}, testAction3{}, testAction2{})
	// Output:
	// action1 Try
	// action3 Try
	// error happened
	// action3 Cancel
	// action1 Cancel
}

func runTest(concurrent bool, actions ...Action) {
	err := tccEngine.Run(10*time.Second, concurrent, actions...)
	if err != nil {
		fmt.Println(errs.WithStack(err))
	}
	time.Sleep(1 * time.Second)
}

type testAction1 struct {
	Data interface{}
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
	time.Sleep(time.Millisecond)
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
	time.Sleep(time.Millisecond)
	fmt.Println("action2 Confirm")
	return nil
}
func (ta testAction2) Cancel() error {
	fmt.Println("action2 Cancel")
	return nil
}

type testAction3 struct {
}

func (ta testAction3) Name() string {
	return "action3"
}
func (ta testAction3) Try() error {
	fmt.Println("action3 Try")
	return errors.New("error happened")
}
func (ta testAction3) Confirm() error {
	fmt.Println("action3 Confirm")
	return nil
}
func (ta testAction3) Cancel() error {
	fmt.Println("action3 Cancel")
	return nil
}

func getMQ() *sqlmq.SqlMQ {
	if _, err := testDB.Exec("DROP TABLE IF EXISTS sqlmq"); err != nil {
		panic(err)
	}
	mq := &sqlmq.SqlMQ{
		DB:            testDB,
		Table:         sqlmq.NewStdTable(testDB, "sqlmq", time.Hour),
		CleanInterval: time.Hour,
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
