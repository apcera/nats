// Copyright 2012-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/encoders/builtin"
	"github.com/nats-io/nats.go/encoders/protobuf"
	"github.com/nats-io/nats.go/encoders/protobuf/testdata"
)

func NewDefaultEConn(t *testing.T) *nats.EncodedConn {
	ec, err := nats.NewEncodedConn(NewConnection(t, TEST_PORT), nats.DEFAULT_ENCODER)
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestEncBuiltinConstructorErrs(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	c := NewConnection(t, TEST_PORT)
	_, err := nats.NewEncodedConn(nil, "default")
	if err == nil {
		t.Fatal("Expected err for nil connection")
	}
	_, err = nats.NewEncodedConn(c, "foo22")
	if err == nil {
		t.Fatal("Expected err for bad encoder")
	}
	c.Close()
	_, err = nats.NewEncodedConn(c, "default")
	if err == nil {
		t.Fatal("Expected err for closed connection")
	}

}

func TestEncBuiltinMarshalString(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testString := "Hello World!"

	ec.Subscribe("enc_string", func(s string) {
		if s != testString {
			t.Fatalf("Received test string of '%s', wanted '%s'\n", s, testString)
		}
		ch <- true
	})
	ec.Publish("enc_string", testString)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinMarshalBytes(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testBytes := []byte("Hello World!")

	ec.Subscribe("enc_bytes", func(b []byte) {
		if !bytes.Equal(b, testBytes) {
			t.Fatalf("Received test bytes of '%s', wanted '%s'\n", b, testBytes)
		}
		ch <- true
	})
	ec.Publish("enc_bytes", testBytes)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinMarshalInt(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := 22

	ec.Subscribe("enc_int", func(n int) {
		if n != testN {
			t.Fatalf("Received test number of %d, wanted %d\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_int", testN)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinMarshalInt32(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := 22

	ec.Subscribe("enc_int", func(n int32) {
		if n != int32(testN) {
			t.Fatalf("Received test number of %d, wanted %d\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_int", testN)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinMarshalInt64(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := 22

	ec.Subscribe("enc_int", func(n int64) {
		if n != int64(testN) {
			t.Fatalf("Received test number of %d, wanted %d\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_int", testN)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinMarshalFloat32(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := float32(22)

	ec.Subscribe("enc_float", func(n float32) {
		if n != testN {
			t.Fatalf("Received test number of %f, wanted %f\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_float", testN)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinMarshalFloat64(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := float64(22.22)

	ec.Subscribe("enc_float", func(n float64) {
		if n != testN {
			t.Fatalf("Received test number of %f, wanted %f\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_float", testN)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinMarshalBool(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()
	ch := make(chan bool)
	expected := make(chan bool, 1)

	ec.Subscribe("enc_bool", func(b bool) {
		val := <-expected
		if b != val {
			t.Fatal("Boolean values did not match")
		}
		ch <- true
	})

	expected <- false
	ec.Publish("enc_bool", false)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}

	expected <- true
	ec.Publish("enc_bool", true)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinExtendedSubscribeCB(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()

	ch := make(chan bool)

	testString := "Hello World!"
	subject := "cb_args"

	ec.Subscribe(subject, func(subj, s string) {
		if s != testString {
			t.Fatalf("Received test string of '%s', wanted '%s'\n", s, testString)
		}
		if subj != subject {
			t.Fatalf("Received subject of '%s', wanted '%s'\n", subj, subject)
		}
		ch <- true
	})
	ec.Publish(subject, testString)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinExtendedSubscribeCB2(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()

	ch := make(chan bool)

	testString := "Hello World!"
	oSubj := "cb_args"
	oReply := "foobar"

	ec.Subscribe(oSubj, func(subj, reply, s string) {
		if s != testString {
			t.Fatalf("Received test string of '%s', wanted '%s'\n", s, testString)
		}
		if subj != oSubj {
			t.Fatalf("Received subject of '%s', wanted '%s'\n", subj, oSubj)
		}
		if reply != oReply {
			t.Fatalf("Received reply of '%s', wanted '%s'\n", reply, oReply)
		}
		ch <- true
	})
	ec.PublishRequest(oSubj, oReply, testString)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinRawMsgSubscribeCB(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()

	ch := make(chan bool)

	testString := "Hello World!"
	oSubj := "cb_args"
	oReply := "foobar"

	ec.Subscribe(oSubj, func(m *nats.Msg) {
		s := string(m.Data)
		if s != testString {
			t.Fatalf("Received test string of '%s', wanted '%s'\n", s, testString)
		}
		if m.Subject != oSubj {
			t.Fatalf("Received subject of '%s', wanted '%s'\n", m.Subject, oSubj)
		}
		if m.Reply != oReply {
			t.Fatalf("Received reply of '%s', wanted '%s'\n", m.Reply, oReply)
		}
		ch <- true
	})
	ec.PublishRequest(oSubj, oReply, testString)
	if e := Wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinRequest(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()

	expectedResp := "I can help!"

	ec.Subscribe("help", func(subj, reply, req string) {
		ec.Publish(reply, expectedResp)
	})

	var resp string

	err := ec.Request("help", "help me", &resp, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed at receiving proper response: %v\n", err)
	}
	if resp != expectedResp {
		t.Fatalf("Received reply '%s', wanted '%s'\n", resp, expectedResp)
	}
}

func TestEncBuiltinRequestReceivesMsg(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()

	expectedResp := "I can help!"

	ec.Subscribe("help", func(subj, reply, req string) {
		ec.Publish(reply, expectedResp)
	})

	var resp nats.Msg

	err := ec.Request("help", "help me", &resp, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed at receiving proper response: %v\n", err)
	}
	if string(resp.Data) != expectedResp {
		t.Fatalf("Received reply '%s', wanted '%s'\n", string(resp.Data), expectedResp)
	}
}

func TestEncBuiltinAsyncMarshalErr(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	defer ec.Close()

	ch := make(chan bool)

	testString := "Hello World!"
	subject := "err_marshall"

	ec.Subscribe(subject, func(subj, num int) {
		// This will never get called.
	})

	ec.Conn.Opts.AsyncErrorCB = func(c *nats.Conn, s *nats.Subscription, err error) {
		ch <- true
	}

	ec.Publish(subject, testString)
	if e := Wait(ch); e != nil {
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncBuiltinEncodeNil(t *testing.T) {
	de := &builtin.DefaultEncoder{}
	_, err := de.Encode("foo", nil)
	if err != nil {
		t.Fatalf("Expected no error encoding nil: %v", err)
	}
}

func TestEncBuiltinDecodeDefault(t *testing.T) {
	de := &builtin.DefaultEncoder{}
	b, err := de.Encode("foo", 22)
	if err != nil {
		t.Fatalf("Expected no error encoding number: %v", err)
	}
	var c chan bool
	err = de.Decode("foo", b, &c)
	if err == nil {
		t.Fatalf("Expected an error decoding")
	}
}

func TestEncDrainSupported(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewDefaultEConn(t)
	err := ec.Drain()
	if err != nil {
		t.Fatalf("Expected no error calling Drain(), got %v", err)
	}
}

const ENC_TEST_PORT = 8268

var options = nats.Options{
	Url:            fmt.Sprintf("nats://127.0.0.1:%d", ENC_TEST_PORT),
	AllowReconnect: true,
	MaxReconnect:   10,
	ReconnectWait:  100 * time.Millisecond,
	Timeout:        nats.DefaultTimeout,
}

func TestPublishErrorAfterSubscribeDecodeError(t *testing.T) {
	ts := RunServerOnPort(ENC_TEST_PORT)
	defer ts.Shutdown()
	opts := options
	nc, _ := opts.Connect()
	defer nc.Close()

	// Override default handler for test.
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, _ error) {})

	c, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)

	//Test message type
	type Message struct {
		Message string
	}
	const testSubj = "test"

	c.Subscribe(testSubj, func(msg *Message) {})

	// Publish invalid json to catch decode error in subscription callback
	c.Publish(testSubj, `foo`)
	c.Flush()

	// Next publish should be successful
	if err := c.Publish(testSubj, Message{"2"}); err != nil {
		t.Error("Fail to send correct json message after decode error in subscription")
	}
}

func TestPublishErrorAfterInvalidPublishMessage(t *testing.T) {
	ts := RunServerOnPort(ENC_TEST_PORT)
	defer ts.Shutdown()
	opts := options
	nc, _ := opts.Connect()
	defer nc.Close()
	c, _ := nats.NewEncodedConn(nc, protobuf.PROTOBUF_ENCODER)
	const testSubj = "test"

	c.Publish(testSubj, &testdata.Person{Name: "Anatolii"})

	// Publish invalid protobuf message to catch decode error
	c.Publish(testSubj, "foo")

	// Next publish with valid protobuf message should be successful
	if err := c.Publish(testSubj, &testdata.Person{Name: "Anatolii"}); err != nil {
		t.Error("Fail to send correct protobuf message after invalid message publishing", err)
	}
}

func TestVariousFailureConditions(t *testing.T) {
	ts := RunServerOnPort(ENC_TEST_PORT)
	defer ts.Shutdown()

	dch := make(chan bool)

	opts := options
	opts.AsyncErrorCB = func(_ *nats.Conn, _ *nats.Subscription, e error) {
		dch <- true
	}
	nc, _ := opts.Connect()
	nc.Close()

	if _, err := nats.NewEncodedConn(nil, protobuf.PROTOBUF_ENCODER); err == nil {
		t.Fatal("Expected an error")
	}

	if _, err := nats.NewEncodedConn(nc, protobuf.PROTOBUF_ENCODER); err == nil || err != nats.ErrConnectionClosed {
		t.Fatalf("Wrong error: %v instead of %v", err, nats.ErrConnectionClosed)
	}

	nc, _ = opts.Connect()
	defer nc.Close()

	if _, err := nats.NewEncodedConn(nc, "foo"); err == nil {
		t.Fatal("Expected an error")
	}

	c, err := nats.NewEncodedConn(nc, protobuf.PROTOBUF_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer c.Close()

	if _, err := c.Subscribe("bar", func(subj, obj string) {}); err != nil {
		t.Fatalf("Unable to create subscription: %v", err)
	}

	if err := c.Publish("bar", &testdata.Person{Name: "Ivan"}); err != nil {
		t.Fatalf("Unable to publish: %v", err)
	}

	if err := Wait(dch); err != nil {
		t.Fatal("Did not get the async error callback")
	}

	if err := c.PublishRequest("foo", "bar", "foo"); err == nil {
		t.Fatal("Expected an error")
	}

	if err := c.Request("foo", "foo", nil, 2*time.Second); err == nil {
		t.Fatal("Expected an error")
	}

	nc.Close()

	if err := c.PublishRequest("foo", "bar", &testdata.Person{Name: "Ivan"}); err == nil {
		t.Fatal("Expected an error")
	}

	resp := &testdata.Person{}
	if err := c.Request("foo", &testdata.Person{Name: "Ivan"}, resp, 2*time.Second); err == nil {
		t.Fatal("Expected an error")
	}

	if _, err := c.Subscribe("foo", nil); err == nil {
		t.Fatal("Expected an error")
	}

	if _, err := c.Subscribe("foo", func() {}); err == nil {
		t.Fatal("Expected an error")
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Expected an error")
			}
		}()
		if _, err := c.Subscribe("foo", "bar"); err == nil {
			t.Fatal("Expected an error")
		}
	}()
}

func TesEncodedConnRequest(t *testing.T) {
	ts := RunServerOnPort(ENC_TEST_PORT)
	defer ts.Shutdown()

	dch := make(chan bool)

	opts := options
	nc, _ := opts.Connect()
	defer nc.Close()

	c, err := nats.NewEncodedConn(nc, protobuf.PROTOBUF_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer c.Close()

	sentName := "Ivan"
	recvName := "Kozlovic"

	if _, err := c.Subscribe("foo", func(_, reply string, p *testdata.Person) {
		if p.Name != sentName {
			t.Fatalf("Got wrong name: %v instead of %v", p.Name, sentName)
		}
		c.Publish(reply, &testdata.Person{Name: recvName})
		dch <- true
	}); err != nil {
		t.Fatalf("Unable to create subscription: %v", err)
	}
	if _, err := c.Subscribe("foo", func(_ string, p *testdata.Person) {
		if p.Name != sentName {
			t.Fatalf("Got wrong name: %v instead of %v", p.Name, sentName)
		}
		dch <- true
	}); err != nil {
		t.Fatalf("Unable to create subscription: %v", err)
	}

	if err := c.Publish("foo", &testdata.Person{Name: sentName}); err != nil {
		t.Fatalf("Unable to publish: %v", err)
	}

	if err := Wait(dch); err != nil {
		t.Fatal("Did not get message")
	}
	if err := Wait(dch); err != nil {
		t.Fatal("Did not get message")
	}

	response := &testdata.Person{}
	if err := c.Request("foo", &testdata.Person{Name: sentName}, response, 2*time.Second); err != nil {
		t.Fatalf("Unable to publish: %v", err)
	}
	if response.Name != recvName {
		t.Fatalf("Wrong response: %v instead of %v", response.Name, recvName)
	}

	if err := Wait(dch); err != nil {
		t.Fatal("Did not get message")
	}
	if err := Wait(dch); err != nil {
		t.Fatal("Did not get message")
	}

	c2, err := nats.NewEncodedConn(nc, nats.GOB_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer c2.Close()

	if _, err := c2.QueueSubscribe("bar", "baz", func(m *nats.Msg) {
		response := &nats.Msg{Subject: m.Reply, Data: []byte(recvName)}
		c2.Conn.PublishMsg(response)
		dch <- true
	}); err != nil {
		t.Fatalf("Unable to create subscription: %v", err)
	}

	mReply := nats.Msg{}
	if err := c2.Request("bar", &nats.Msg{Data: []byte(sentName)}, &mReply, 2*time.Second); err != nil {
		t.Fatalf("Unable to send request: %v", err)
	}
	if string(mReply.Data) != recvName {
		t.Fatalf("Wrong reply: %v instead of %v", string(mReply.Data), recvName)
	}

	if err := Wait(dch); err != nil {
		t.Fatal("Did not get message")
	}

	if c.LastError() != nil {
		t.Fatalf("Unexpected connection error: %v", c.LastError())
	}
	if c2.LastError() != nil {
		t.Fatalf("Unexpected connection error: %v", c2.LastError())
	}
}

func TestRequestGOB(t *testing.T) {
	ts := RunServerOnPort(ENC_TEST_PORT)
	defer ts.Shutdown()

	type Request struct {
		Name string
	}

	type Person struct {
		Name string
		Age  int
	}

	nc, err := nats.Connect(options.Url)
	if err != nil {
		t.Fatalf("Could not connect: %v", err)
	}
	defer nc.Close()

	ec, err := nats.NewEncodedConn(nc, nats.GOB_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer ec.Close()

	ec.QueueSubscribe("foo.request", "g", func(subject, reply string, r *Request) {
		if r.Name != "meg" {
			t.Fatalf("Expected request to be 'meg', got %q", r)
		}
		response := &Person{Name: "meg", Age: 21}
		ec.Publish(reply, response)
	})

	reply := Person{}
	if err := ec.Request("foo.request", &Request{Name: "meg"}, &reply, time.Second); err != nil {
		t.Fatalf("Failed to receive response: %v", err)
	}
	if reply.Name != "meg" || reply.Age != 21 {
		t.Fatalf("Did not receive proper response, %+v", reply)
	}
}

func TestContextEncodedRequestWithTimeout(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	c, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(100 * time.Millisecond)
	ctx, cancelCB := context.WithDeadline(context.Background(), deadline)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	type request struct {
		Message string `json:"message"`
	}
	type response struct {
		Code int `json:"code"`
	}
	c.Subscribe("slow", func(_, reply string, req *request) {
		got := req.Message
		expected := "Hello"
		if got != expected {
			t.Errorf("Expected to receive request with %q, got %q", got, expected)
		}

		// simulates latency into the client so that timeout is hit.
		time.Sleep(40 * time.Millisecond)
		c.Publish(reply, &response{Code: 200})
	})

	for i := 0; i < 2; i++ {
		req := &request{Message: "Hello"}
		resp := &response{}
		err := c.RequestWithContext(ctx, "slow", req, resp)
		if err != nil {
			t.Fatalf("Expected encoded request with context to not fail: %s", err)
		}
		got := resp.Code
		expected := 200
		if got != expected {
			t.Errorf("Expected to receive %v, got: %v", expected, got)
		}
	}

	// A third request with latency would make the context
	// reach the deadline.
	req := &request{Message: "Hello"}
	resp := &response{}
	err = c.RequestWithContext(ctx, "slow", req, resp)
	if err == nil {
		t.Fatal("Expected request with context to reach deadline")
	}

	// Reported error is "context deadline exceeded" from Context package,
	// which implements net.Error Timeout interface.
	type timeoutError interface {
		Timeout() bool
	}
	timeoutErr, ok := err.(timeoutError)
	if !ok || !timeoutErr.Timeout() {
		t.Errorf("Expected to have a timeout error")
	}
	expected := `context deadline exceeded`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}
}

func TestContextEncodedRequestWithTimeoutCanceled(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	c, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer c.Close()

	ctx, cancelCB := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	type request struct {
		Message string `json:"message"`
	}
	type response struct {
		Code int `json:"code"`
	}

	c.Subscribe("fast", func(_, reply string, req *request) {
		got := req.Message
		expected := "Hello"
		if got != expected {
			t.Errorf("Expected to receive request with %q, got %q", got, expected)
		}

		// simulates latency into the client so that timeout is hit.
		time.Sleep(40 * time.Millisecond)

		c.Publish(reply, &response{Code: 200})
	})

	// Fast request should not fail
	req := &request{Message: "Hello"}
	resp := &response{}
	c.RequestWithContext(ctx, "fast", req, resp)
	expectedCode := 200
	if resp.Code != expectedCode {
		t.Errorf("Expected to receive %d, got: %d", expectedCode, resp.Code)
	}

	// Cancel the context already so that rest of requests fail.
	cancelCB()

	err = c.RequestWithContext(ctx, "fast", req, resp)
	if err == nil {
		t.Fatal("Expected request with timeout context to fail")
	}

	// Reported error is "context canceled" from Context package,
	// which is not a timeout error.
	type timeoutError interface {
		Timeout() bool
	}
	if _, ok := err.(timeoutError); ok {
		t.Errorf("Expected to not have a timeout error")
	}
	expected := `context canceled`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}

	// 2nd request should fail again even if fast because context has already been canceled
	err = c.RequestWithContext(ctx, "fast", req, resp)
	if err == nil {
		t.Fatal("Expected request with timeout context to fail")
	}
}

func TestContextEncodedRequestWithCancel(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	c, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer c.Close()

	ctx, cancelCB := context.WithCancel(context.Background())
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	// timer which cancels the context though can also be arbitrarily extended
	expirationTimer := time.AfterFunc(100*time.Millisecond, func() {
		cancelCB()
	})

	type request struct {
		Message string `json:"message"`
	}
	type response struct {
		Code int `json:"code"`
	}
	c.Subscribe("slow", func(_, reply string, req *request) {
		got := req.Message
		expected := "Hello"
		if got != expected {
			t.Errorf("Expected to receive request with %q, got %q", got, expected)
		}

		// simulates latency into the client so that timeout is hit.
		time.Sleep(40 * time.Millisecond)
		c.Publish(reply, &response{Code: 200})
	})
	c.Subscribe("slower", func(_, reply string, req *request) {
		got := req.Message
		expected := "World"
		if got != expected {
			t.Errorf("Expected to receive request with %q, got %q", got, expected)
		}

		// we know this request will take longer so extend the timeout
		expirationTimer.Reset(100 * time.Millisecond)

		// slower reply which would have hit original timeout
		time.Sleep(90 * time.Millisecond)
		c.Publish(reply, &response{Code: 200})
	})

	for i := 0; i < 2; i++ {
		req := &request{Message: "Hello"}
		resp := &response{}
		err := c.RequestWithContext(ctx, "slow", req, resp)
		if err != nil {
			t.Fatalf("Expected encoded request with context to not fail: %s", err)
		}
		got := resp.Code
		expected := 200
		if got != expected {
			t.Errorf("Expected to receive %v, got: %v", expected, got)
		}
	}

	// A third request with latency would make the context
	// get canceled, but these reset the timer so deadline
	// gets extended:
	for i := 0; i < 10; i++ {
		req := &request{Message: "World"}
		resp := &response{}
		err := c.RequestWithContext(ctx, "slower", req, resp)
		if err != nil {
			t.Fatalf("Expected request with context to not fail: %s", err)
		}
		got := resp.Code
		expected := 200
		if got != expected {
			t.Errorf("Expected to receive %d, got: %d", expected, got)
		}
	}

	req := &request{Message: "Hello"}
	resp := &response{}

	// One more slow request will expire the timer and cause an error...
	err = c.RequestWithContext(ctx, "slow", req, resp)
	if err == nil {
		t.Fatal("Expected request with cancellation context to fail")
	}

	// ...though reported error is "context canceled" from Context package,
	// which is not a timeout error.
	type timeoutError interface {
		Timeout() bool
	}
	if _, ok := err.(timeoutError); ok {
		t.Errorf("Expected to not have a timeout error")
	}
	expected := `context canceled`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}
}

func TestContextEncodedRequestWithDeadline(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	c, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(100 * time.Millisecond)
	ctx, cancelCB := context.WithDeadline(context.Background(), deadline)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	type request struct {
		Message string `json:"message"`
	}
	type response struct {
		Code int `json:"code"`
	}
	c.Subscribe("slow", func(_, reply string, req *request) {
		got := req.Message
		expected := "Hello"
		if got != expected {
			t.Errorf("Expected to receive request with %q, got %q", got, expected)
		}

		// simulates latency into the client so that timeout is hit.
		time.Sleep(40 * time.Millisecond)
		c.Publish(reply, &response{Code: 200})
	})

	for i := 0; i < 2; i++ {
		req := &request{Message: "Hello"}
		resp := &response{}
		err := c.RequestWithContext(ctx, "slow", req, resp)
		if err != nil {
			t.Fatalf("Expected encoded request with context to not fail: %s", err)
		}
		got := resp.Code
		expected := 200
		if got != expected {
			t.Errorf("Expected to receive %v, got: %v", expected, got)
		}
	}

	// A third request with latency would make the context
	// reach the deadline.
	req := &request{Message: "Hello"}
	resp := &response{}
	err = c.RequestWithContext(ctx, "slow", req, resp)
	if err == nil {
		t.Fatal("Expected request with context to reach deadline")
	}

	// Reported error is "context deadline exceeded" from Context package,
	// which implements net.Error Timeout interface.
	type timeoutError interface {
		Timeout() bool
	}
	timeoutErr, ok := err.(timeoutError)
	if !ok || !timeoutErr.Timeout() {
		t.Errorf("Expected to have a timeout error")
	}
	expected := `context deadline exceeded`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}
}

func TestEncodedContextInvalid(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	c, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer c.Close()

	type request struct {
		Message string `json:"message"`
	}
	type response struct {
		Code int `json:"code"`
	}
	req := &request{Message: "Hello"}
	resp := &response{}
	//nolint:staticcheck // SA1012 testing that passing nil fails
	err = c.RequestWithContext(nil, "slow", req, resp)
	if err == nil {
		t.Fatal("Expected request to fail with error")
	}
	if err != nats.ErrInvalidContext {
		t.Errorf("Expected request to fail with invalid context: %s", err)
	}
}
