// Copyright 2018-2023 The NATS Authors
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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Drain can be very useful for graceful shutdown of subscribers.
// Especially queue subscribers.
func TestDrain(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()
	nc := NewDefaultConnection(t)
	defer nc.Close()

	done := make(chan bool)
	received := int32(0)
	expected := int32(100)

	cb := func(_ *nats.Msg) {
		// Allow this to back up.
		time.Sleep(time.Millisecond)
		rcvd := atomic.AddInt32(&received, 1)
		if rcvd >= expected {
			done <- true
		}
	}

	sub, err := nc.Subscribe("foo", cb)
	if err != nil {
		t.Fatalf("Error creating subscription; %v", err)
	}

	for i := int32(0); i < expected; i++ {
		nc.Publish("foo", []byte("Don't forget about me"))
	}

	// Drain it and make sure we receive all messages.
	sub.Drain()
	select {
	case <-done:
		break
	case <-time.After(5 * time.Second):
		r := atomic.LoadInt32(&received)
		if r != expected {
			t.Fatalf("Did not receive all messages: %d of %d", r, expected)
		}
	}
}

func TestDrainQueueSub(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()
	nc := NewDefaultConnection(t)
	defer nc.Close()

	done := make(chan bool)
	received := int32(0)
	expected := int32(4096)
	numSubs := int32(32)

	checkDone := func() int32 {
		rcvd := atomic.AddInt32(&received, 1)
		if rcvd >= expected {
			done <- true
		}
		return rcvd
	}

	callback := func(m *nats.Msg) {
		rcvd := checkDone()
		// Randomly replace this sub from time to time.
		if rcvd%3 == 0 {
			m.Sub.Drain()
			// Create a new one that we will not drain.
			nc.QueueSubscribe("foo", "bar", func(m *nats.Msg) { checkDone() })
		}
	}

	for i := int32(0); i < numSubs; i++ {
		_, err := nc.QueueSubscribe("foo", "bar", callback)
		if err != nil {
			t.Fatalf("Error creating subscription; %v", err)
		}
	}

	for i := int32(0); i < expected; i++ {
		nc.Publish("foo", []byte("Don't forget about me"))
	}

	select {
	case <-done:
		break
	case <-time.After(5 * time.Second):
		r := atomic.LoadInt32(&received)
		if r != expected {
			t.Fatalf("Did not receive all messages: %d of %d", r, expected)
		}
	}
}

func waitFor(t *testing.T, totalWait, sleepDur time.Duration, f func() error) {
	t.Helper()
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		err = f()
		if err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestDrainUnSubs(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()
	nc := NewDefaultConnection(t)
	defer nc.Close()

	num := 100
	subs := make([]*nats.Subscription, num)

	// Normal Unsubscribe
	for i := 0; i < num; i++ {
		sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {})
		if err != nil {
			t.Fatalf("Error creating subscription; %v", err)
		}
		subs[i] = sub
	}

	if numSubs := nc.NumSubscriptions(); numSubs != num {
		t.Fatalf("Expected %d subscriptions, got %d", num, numSubs)
	}
	for i := 0; i < num; i++ {
		subs[i].Unsubscribe()
	}
	if numSubs := nc.NumSubscriptions(); numSubs != 0 {
		t.Fatalf("Expected no subscriptions, got %d", numSubs)
	}

	// Drain version
	for i := 0; i < num; i++ {
		sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {})
		if err != nil {
			t.Fatalf("Error creating subscription; %v", err)
		}
		subs[i] = sub
	}

	if numSubs := nc.NumSubscriptions(); numSubs != num {
		t.Fatalf("Expected %d subscriptions, got %d", num, numSubs)
	}
	for i := 0; i < num; i++ {
		subs[i].Drain()
	}
	// Should happen quickly that we get to zero, so do not need to wait long.
	waitFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if numSubs := nc.NumSubscriptions(); numSubs != 0 {
			return fmt.Errorf("Expected no subscriptions, got %d", numSubs)
		}
		return nil
	})
}

func TestDrainSlowSubscriber(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()
	nc := NewDefaultConnection(t)
	defer nc.Close()

	received := int32(0)

	sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {
		atomic.AddInt32(&received, 1)
		time.Sleep(100 * time.Millisecond)
	})
	if err != nil {
		t.Fatalf("Error creating subscription; %v", err)
	}

	total := 10
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Slow Slow"))
	}
	nc.Flush()

	pmsgs, _, _ := sub.Pending()
	if pmsgs != total && pmsgs != total-1 {
		t.Fatalf("Expected most messages to be pending, but got %d vs %d", pmsgs, total)
	}
	sub.Drain()

	// Should take a second or so to drain away.
	waitFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		// Wait for it to become invalid. Once drained it is unsubscribed.
		_, _, err := sub.Pending()
		if err != nats.ErrBadSubscription {
			return fmt.Errorf("Still valid")
		}
		r := int(atomic.LoadInt32(&received))
		if r != total {
			t.Fatalf("Did not receive all messages, got %d vs %d", r, total)
		}
		return nil
	})
}

func TestDrainConnection(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	done := make(chan bool)
	rdone := make(chan bool)

	closed := func(nc *nats.Conn) {
		done <- true
	}

	url := fmt.Sprintf("nats://127.0.0.1:%d", nats.DefaultPort)
	nc, err := nats.Connect(url, nats.ClosedHandler(closed))
	if err != nil {
		t.Fatalf("Failed to create default connection: %v", err)
	}
	defer nc.Close()

	nc2, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to create default connection: %v", err)
	}
	defer nc2.Close()

	received := int32(0)
	responses := int32(0)
	expected := int32(50)
	sleep := 10 * time.Millisecond

	// Create the listener for responses on "bar"
	_, err = nc2.Subscribe("bar", func(_ *nats.Msg) {
		r := atomic.AddInt32(&responses, 1)
		if r == expected {
			rdone <- true
		}
	})
	if err != nil {
		t.Fatalf("Error creating subscription for responses: %v", err)
	}

	// Create a slow subscriber for the responder
	sub, err := nc.Subscribe("foo", func(m *nats.Msg) {
		time.Sleep(sleep)
		atomic.AddInt32(&received, 1)
		err := nc.Publish(m.Reply, []byte("Stop bugging me"))
		if err != nil {
			t.Errorf("Publisher received an error sending response: %v\n", err)
		}
	})
	if err != nil {
		t.Fatalf("Error creating subscription; %v", err)
	}

	// Publish some messages
	for i := int32(0); i < expected; i++ {
		nc.PublishRequest("foo", "bar", []byte("Slow Slow"))
	}

	drainStart := time.Now()
	nc.Drain()

	// Sub should be disabled immediately
	if err := sub.Unsubscribe(); err == nil {
		t.Fatalf("Expected to receive an error on Unsubscribe after drain")
	}
	// Also can not create any new subs
	if _, err := nc.Subscribe("foo", func(_ *nats.Msg) {}); err == nil {
		t.Fatalf("Expected to receive an error on new Subscription after drain")
	}

	// Make sure we can still publish, this is for any responses.
	if err := nc.Publish("baz", []byte("Slow Slow")); err != nil {
		t.Fatalf("Expected to not receive an error on Publish after drain, got %v", err)
	}

	// Wait for the closed state from nc
	select {
	case <-done:
		if time.Since(drainStart) < (sleep * time.Duration(expected)) {
			t.Fatalf("Drain exited too soon\n")
		}
		r := atomic.LoadInt32(&received)
		if r != expected {
			t.Fatalf("Did not receive all messages from Drain, %d vs %d", r, expected)
		}
		break
	case <-time.After(2 * time.Second):
		t.Fatalf("Timeout waiting for closed state for connection")
	}

	// Now make sure all responses were received.
	select {
	case <-rdone:
		r := atomic.LoadInt32(&responses)
		if r != expected {
			t.Fatalf("Did not receive all responses, %d vs %d", r, expected)
		}
		break
	case <-time.After(2 * time.Second):
		t.Fatalf("Timeout waiting for all the responses")
	}
}

func TestDrainConnectionAutoUnsub(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	errors := int32(0)
	received := int32(0)
	expected := int32(10)

	done := make(chan bool)

	closed := func(nc *nats.Conn) {
		done <- true
	}

	errCb := func(nc *nats.Conn, s *nats.Subscription, err error) {
		atomic.AddInt32(&errors, 1)
	}

	url := fmt.Sprintf("nats://127.0.0.1:%d", nats.DefaultPort)
	nc, err := nats.Connect(url, nats.ErrorHandler(errCb), nats.ClosedHandler(closed))
	if err != nil {
		t.Fatalf("Failed to create default connection: %v", err)
	}
	defer nc.Close()

	sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {
		// So they back up a bit in client and allow drain to do its thing.
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&received, 1)

	})
	if err != nil {
		t.Fatalf("Error creating subscription; %v", err)
	}

	sub.AutoUnsubscribe(int(expected))

	// Publish some messages
	for i := 0; i < 50; i++ {
		nc.Publish("foo", []byte("Only 10 please!"))
	}
	// Flush here so messages coming back into client.
	nc.Flush()

	// Now add drain state.
	time.Sleep(10 * time.Millisecond)
	nc.Drain()

	// Wait for the closed state from nc
	select {
	case <-done:
		errs := atomic.LoadInt32(&errors)
		if errs > 0 {
			t.Fatalf("Did not expect any errors, got %d", errs)
		}
		r := atomic.LoadInt32(&received)
		if r != expected {
			t.Fatalf("Did not receive all messages from Drain, %d vs %d", r, expected)
		}
		break
	case <-time.After(2 * time.Second):
		t.Fatalf("Timeout waiting for closed state for connection")
	}
}

func TestDrainConnLastError(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	done := make(chan bool, 1)
	closedCb := func(nc *nats.Conn) {
		done <- true
	}

	nc, err := nats.Connect(nats.DefaultURL,
		nats.ClosedHandler(closedCb),
		nats.DrainTimeout(time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to create default connection: %v", err)
	}
	defer nc.Close()

	// Override default handler for test.
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, _ error) {})

	wg := sync.WaitGroup{}
	wg.Add(1)
	if _, err := nc.Subscribe("foo", func(_ *nats.Msg) {
		// So they back up a bit in client to make drain timeout
		time.Sleep(100 * time.Millisecond)
		wg.Done()

	}); err != nil {
		t.Fatalf("Error creating subscription; %v", err)
	}

	if err := nc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nc.Drain(); err != nil {
		t.Fatalf("Error on drain: %v", err)
	}

	select {
	case <-done:
		if e := nc.LastError(); e == nil || e != nats.ErrDrainTimeout {
			t.Fatalf("Expected last error to be set to %v, got %v", nats.ErrDrainTimeout, e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Timeout waiting for closed state for connection")
	}

	// Wait for subscription callback to return
	wg.Wait()
}

func TestDrainConnDuringReconnect(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	done := make(chan bool, 1)
	closedCb := func(nc *nats.Conn) {
		done <- true
	}

	nc, err := nats.Connect(nats.DefaultURL,
		nats.ClosedHandler(closedCb),
		nats.DrainTimeout(20*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to create default connection: %v", err)
	}
	defer nc.Close()

	// Shutdown the server.
	s.Shutdown()

	waitFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if nc.IsReconnecting() {
			return nil
		}
		return fmt.Errorf("Not reconnecting yet")
	})

	// This should work correctly.
	if err := nc.Drain(); err != nats.ErrConnectionReconnecting {
		t.Fatalf("Unexpected error on drain: %v", err)
	}

	// Closed should still fire.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Timeout waiting for closed state for connection")
	}
}
