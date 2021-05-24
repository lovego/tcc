package tcc

import (
	"fmt"
	"regexp"
	"time"

	"github.com/lovego/sqlmq"
)

var timePrefix = regexp.MustCompile(`^\S+ `)

func ExampleNewEngine() {
	defer func() {
		fmt.Println(timePrefix.ReplaceAllString(recover().(string), ""))
	}()
	NewEngine("test", testMQ)
	// Output:
	// queue tcc-test already registered
}

func ExampleEngine_Register() {
	defer func() {
		fmt.Println(timePrefix.ReplaceAllString(recover().(string), ""))
	}()
	tccEngine.Register(testAction1{})
	// Output:
	// action action1 already registered
}

func ExampleEngine_New() {
	tccEngine.mqName = "tcc-test2"
	defer func() {
		tccEngine.mqName = "tcc-test"
	}()
	_, err := tccEngine.New(time.Second, true)
	fmt.Println(err)
	fmt.Println(tccEngine.Run(time.Second, true))
	// Output:
	// unknown queue: tcc-test2
	// unknown queue: tcc-test2
}

func ExampleEngine_handle() {
	fmt.Println(tccEngine.handle(nil, nil, &sqlmq.StdMessage{
		Data: []byte{},
	}))
	fmt.Println(tccEngine.handle(nil, nil, &sqlmq.StdMessage{
		Data: []byte(`{"Status": "confirmed", "Actions":[{"Name":"test-action"}]}`),
	}))
	fmt.Println(tccEngine.handle(nil, nil, &sqlmq.StdMessage{
		Data: []byte(`{"Status": "canceled", "Actions":[{"Name":"action1","Raw":1}]}`),
	}))
	fmt.Println(tccEngine.handle(nil, nil, &sqlmq.StdMessage{
		Data: []byte(`{"Status": "confirmed", "Concurrent": true, "Actions":[{"Name":"test-action"}]}`),
	}))
	fmt.Println(tccEngine.handle(nil, nil, &sqlmq.StdMessage{
		Data: []byte(`{"Status": "canceled", "Concurrent": true, "Actions":[{"Name":"action1","Raw":1}]}`),
	}))
	// Output:
	// 1h0m0s true unexpected end of JSON input
	// 1h0m0s true test-action: action not registered
	// 1h0m0s true action1: json: cannot unmarshal number into Go value of type tcc.testAction1
	// 1h0m0s true test-action: action not registered
	// 1h0m0s true action1: json: cannot unmarshal number into Go value of type tcc.testAction1
}
func ExampleGetRetryAfter() {
	fmt.Println(getRetryAfter(0))
	fmt.Println(getRetryAfter(1))
	fmt.Println(getRetryAfter(2))
	fmt.Println(getRetryAfter(3))
	fmt.Println(getRetryAfter(4))

	// Output:
	// 3s
	// 30s
	// 5m0s
	// 1h0m0s
	// 1h0m0s
}

type testAction struct {
}

func (ta testAction) Name() string {
	return "test-action"
}
func (ta testAction) Try() error {
	return nil
}
func (ta testAction) Confirm() error {
	return nil
}
func (ta testAction) Cancel() error {
	return nil
}
