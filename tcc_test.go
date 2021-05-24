package tcc

import (
	"fmt"
	"regexp"
	"time"

	"github.com/lovego/sqlmq"
)

func ExampleTCC_Try() {
	tcc, err := tccEngine.New(time.Minute, false)
	if err != nil {
		panic(err)
	}
	fmt.Println(tcc.Try(testAction{}))
	fmt.Println(tcc.Try(&testAction1{}))
	fmt.Println(tcc.Try(testAction1{Data: make(chan int)}))
	tcc.msg.Id = -9
	fmt.Println(tcc.Try(testAction1{}))
	// Output:
	// action test-action is not registered
	// action action1 has been registered with type "tcc.testAction1", but tried with type "*tcc.testAction1"
	// json: unsupported type: chan int
	// tcc(-9) not exists
}

func ExampleTCC_Confirm() {
	tcc, err := tccEngine.New(time.Minute, false)
	if err != nil {
		panic(err)
	}
	fmt.Println(tcc.Confirm())
	fmt.Println(tccId.ReplaceAllString(tcc.Confirm().Error(), "tcc(1)"))

	// Output:
	// <nil>
	// tcc(1) is confirmed, cann't Confirm
}

func ExampleTCC_Cancel() {
	tcc, err := tccEngine.New(time.Minute, false)
	if err != nil {
		panic(err)
	}
	fmt.Println(tcc.Confirm())
	fmt.Println(tccId.ReplaceAllString(tcc.Cancel().Error(), "tcc(1)"))

	// Output:
	// <nil>
	// tcc(1) is confirmed, cann't Cancel
}

func ExampleTCC_update() {
	tcc, err := tccEngine.New(time.Minute, false)
	if err != nil {
		panic(err)
	}
	fmt.Println(tcc.update("", statusTrying, "xx", testDB))
	// Output:
	// false pq: syntax error at or near "WHERE"
}

var tccId = regexp.MustCompile(`^tcc\(\d+\)`)

func ExampleTCC_statusError() {
	tcc, err := tccEngine.New(time.Minute, false)
	if err != nil {
		panic(err)
	}
	canCommit, err := tcc.statusError("confirm action", testDB)
	fmt.Println(canCommit, tccId.ReplaceAllString(err.Error(), "tcc(1)"))

	db := getDB()
	db.Close()

	fmt.Println(tcc.statusError("confirm action", db))
	// Output:
	// true tcc(1) is trying, cann't confirm action
	// false sql: database is closed
}

func ExampleTCC_confirmOrCancel() {
	tx, err := testDB.Begin()
	if err != nil {
		panic(err)
	}
	fmt.Println((&TCC{msg: &sqlmq.StdMessage{Data: &tccData{}}}).confirmOrCancel(tx))
	// Output:
	// 0s true tcc(0) is canceled, cann't Cancel
}
