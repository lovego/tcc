package tcc

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/lovego/errs"
	"github.com/lovego/logger"
	"github.com/lovego/sqlmq"
)

var testDB *sql.DB
var testMQ *sqlmq.SqlMQ
var tccEngine *Engine

func init() {
	testDB = getDB()
	testMQ = getMQ()
	tccEngine = NewEngine("test", testMQ)
	tccEngine.Register(
		testAction1{}, testAction2{}, testAction3{}, &testAction4{}, &testAction5{},
		&testAction6{}, &testAction7{},
	)
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

func ExampleTCC_confirmRetry() {
	runTest(false, testAction1{}, testAction2{}, &testAction4{})
	time.Sleep(4 * time.Second)
	// Output:
	// action1 Try
	// action2 Try
	// action4 Try
	// action1 Confirm
	// action2 Confirm
	// action4 Confirm 1
	// action4 Confirm 2
}

func ExampleTCC_confirmRetry_concurrently() {
	runTest(true, testAction1{}, testAction2{}, &testAction5{})
	time.Sleep(4 * time.Second)
	// Output:
	// action1 Try
	// action2 Try
	// action5 Try
	// action1 Confirm
	// action2 Confirm
	// action5 Confirm 1
	// action5 Confirm 2
}

func ExampleTCC_cancelRetry() {
	runTest(false, testAction1{}, testAction2{}, &testAction6{})
	time.Sleep(4 * time.Second)
	// Output:
	// action1 Try
	// action2 Try
	// action6 Try
	// error happened
	// action6 Cancel 1
	// action6 Cancel 2
	// action2 Cancel
	// action1 Cancel
}

func ExampleTCC_cancelRetry_concurrently() {
	runTest(true, testAction1{}, testAction2{}, &testAction7{})
	time.Sleep(4 * time.Second)
	// Output:
	// action1 Try
	// action2 Try
	// action7 Try
	// error happened
	// action2 Cancel
	// action1 Cancel
	// action7 Cancel 1
	// action7 Cancel 2
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

type testAction4 struct {
}

func (ta *testAction4) Name() string {
	return "action4"
}
func (ta *testAction4) Try() error {
	fmt.Println("action4 Try")
	return nil
}

var action4ConfirmCount int

func (ta *testAction4) Confirm() error {
	action4ConfirmCount++
	fmt.Printf("action4 Confirm %d\n", action4ConfirmCount)
	if action4ConfirmCount <= 1 {
		return errors.New("error happened")
	}
	return nil
}
func (ta *testAction4) Cancel() error {
	return nil
}

type testAction5 struct {
}

func (ta *testAction5) Name() string {
	return "action5"
}
func (ta *testAction5) Try() error {
	fmt.Println("action5 Try")
	return nil
}

var action5ConfirmCount int

func (ta *testAction5) Confirm() error {
	time.Sleep(2 * time.Millisecond)
	action5ConfirmCount++
	fmt.Printf("action5 Confirm %d\n", action5ConfirmCount)
	if action5ConfirmCount <= 1 {
		return errors.New("error happened")
	}
	return nil
}
func (ta *testAction5) Cancel() error {
	return nil
}

type testAction6 struct {
}

func (ta *testAction6) Name() string {
	return "action6"
}
func (ta *testAction6) Try() error {
	fmt.Println("action6 Try")
	return errors.New("error happened")
}

func (ta *testAction6) Confirm() error {
	return nil
}

var action6CancelCount int

func (ta *testAction6) Cancel() error {
	time.Sleep(2 * time.Millisecond)
	action6CancelCount++
	fmt.Printf("action6 Cancel %d\n", action6CancelCount)
	if action6CancelCount <= 1 {
		return errors.New("error happened")
	}
	return nil
}

type testAction7 struct {
}

func (ta *testAction7) Name() string {
	return "action7"
}
func (ta *testAction7) Try() error {
	fmt.Println("action7 Try")
	return errors.New("error happened")
}

func (ta *testAction7) Confirm() error {
	return nil
}

var action7CancelCount int

func (ta *testAction7) Cancel() error {
	time.Sleep(2 * time.Millisecond)
	action7CancelCount++
	fmt.Printf("action7 Cancel %d\n", action7CancelCount)
	if action7CancelCount <= 1 {
		return errors.New("error happened")
	}
	return nil
}

func getMQ() *sqlmq.SqlMQ {
	if _, err := testDB.Exec("DROP TABLE IF EXISTS sqlmq"); err != nil {
		panic(err)
	}
	logFile, err := os.Create(".log.json")
	if err != nil {
		panic(err)
	}
	mq := &sqlmq.SqlMQ{
		DB:     testDB,
		Table:  sqlmq.NewStdTable(testDB, "sqlmq", time.Hour),
		Logger: logger.New(logFile),
	}
	mq.Debug(true)
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
