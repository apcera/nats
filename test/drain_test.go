// Copyright 2018 The NATS Authors
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
	"sync/atomic"
	"testing"
	"time"

	nats "github.com/nats-io/go-nats"
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
		t.Fatalf("Error creating subscription; %v\n", err)
	}

	for i := int32(0); i < expected; i++ {
		nc.Publish("foo", []byte("Don't forget about me"))
	}

	// Drain it and make sure we receive all messages.
	sub.Drain()
	select {
	case <-done:
		break
	case <-time.After(2 * time.Second):
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
			t.Fatalf("Error creating subscription; %v\n", err)
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
			t.Fatalf("Error creating subscription; %v\n", err)
		}
		subs[i] = sub
	}

	if numSubs := nc.NumSubscriptions(); numSubs != num {
		t.Fatalf("Expected %d subscriptions, got %d\n", num, numSubs)
	}
	for i := 0; i < num; i++ {
		subs[i].Unsubscribe()
	}
	if numSubs := nc.NumSubscriptions(); numSubs != 0 {
		t.Fatalf("Expected no subscriptions, got %d\n", numSubs)
	}

	// Drain version
	for i := 0; i < num; i++ {
		sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {})
		if err != nil {
			t.Fatalf("Error creating subscription; %v\n", err)
		}
		subs[i] = sub
	}

	if numSubs := nc.NumSubscriptions(); numSubs != num {
		t.Fatalf("Expected %d subscriptions, got %d\n", num, numSubs)
	}
	for i := 0; i < num; i++ {
		subs[i].Drain()
	}
	// Should happen quickly that we get to zero, so do not need to wait long.
	waitFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if numSubs := nc.NumSubscriptions(); numSubs != 0 {
			return fmt.Errorf("Expected no subscriptions, got %d\n", numSubs)
		}
		return nil
	})
}

func TestDrainSlowSubscriber(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()
	nc := NewDefaultConnection(t)
	defer nc.Close()

	sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {
		time.Sleep(100 * time.Millisecond)
	})
	if err != nil {
		t.Fatalf("Error creating subscription; %v\n", err)
	}

	total := 10

	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Slow Slow"))
	}

	nc.Flush()
	pmsgs, _, _ := sub.Pending()
	if pmsgs != total && pmsgs != total-1 {
		t.Fatalf("Expected most messages to be pending, but got %d vs %d\n", pmsgs, total)
	}
	// Should take a second or so to drain away.
	waitFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		pmsgs, _, _ := sub.Pending()
		if pmsgs != 0 {
			return fmt.Errorf("Expected no pending, got %d\n", pmsgs)
		}
		return nil
	})
}
