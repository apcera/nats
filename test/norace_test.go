// Copyright 2019-2025 The NATS Authors
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

//go:build !race && !skip_no_race_tests
// +build !race,!skip_no_race_tests

package test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNoRaceObjectContextOpt(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	time.AfterFunc(100*time.Millisecond, cancel)

	start := time.Now()
	_, err = obs.Put(&nats.ObjectMeta{Name: "TEST"}, &slow{1000}, nats.Context(ctx))
	expectErr(t, err)
	if delta := time.Since(start); delta > time.Second {
		t.Fatalf("Cancel took too long: %v", delta)
	}
	si, err := js.StreamInfo("OBJ_OBJS")
	expectOk(t, err)
	if si.State.Msgs != 0 {
		t.Fatalf("Expected no messages after canceling put, got %+v", si.State)
	}

	// Now put a large object in there.
	blob := make([]byte, 16*1024*1024)
	rand.Read(blob)
	_, err = obs.PutBytes("BLOB", blob)
	expectOk(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	time.AfterFunc(10*time.Millisecond, cancel)

	start = time.Now()
	_, err = obs.GetBytes("BLOB", nats.Context(ctx))
	expectErr(t, err)
	if delta := time.Since(start); delta > 2500*time.Millisecond {
		t.Fatalf("Cancel took too long: %v", delta)
	}
}

type slow struct{ n int }

func (sr *slow) Read(p []byte) (n int, err error) {
	if sr.n <= 0 {
		return 0, io.EOF
	}
	sr.n--
	time.Sleep(10 * time.Millisecond)
	p[0] = 'A'
	return 1, nil
}

func TestNoRaceObjectDoublePut(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	_, err = obs.PutBytes("A", bytes.Repeat([]byte("A"), 1_000_000))
	expectOk(t, err)

	_, err = obs.PutBytes("A", bytes.Repeat([]byte("a"), 20_000_000))
	expectOk(t, err)

	_, err = obs.GetBytes("A")
	expectOk(t, err)
}

func TestNoRaceJetStreamConsumerSlowConsumer(t *testing.T) {
	// This test fails many times, need to look harder at the imbalance.
	t.SkipNow()

	s := RunServerOnPort(-1)
	defer shutdownJSServerAndRemoveStorage(t, s)

	if err := s.EnableJetStream(nil); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "PENDING_TEST",
		Subjects: []string{"js.p"},
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	// Override default handler for test.
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, _ error) {})

	// Queue up 1M small messages.
	toSend := uint64(1000000)
	for i := uint64(0); i < toSend; i++ {
		nc.Publish("js.p", []byte("ok"))
	}
	nc.Flush()

	str, err := js.StreamInfo("PENDING_TEST")
	if err != nil {
		t.Fatal(err)
	}

	if nm := str.State.Msgs; nm != toSend {
		t.Fatalf("Expected to have stored all %d msgs, got only %d", toSend, nm)
	}

	var received uint64
	done := make(chan bool, 1)

	js.Subscribe("js.p", func(m *nats.Msg) {
		received++
		if received >= toSend {
			done <- true
		}
		meta, err := m.Metadata()
		if err != nil {
			t.Fatalf("could not get message metadata: %s", err)
		}
		if meta.Sequence.Stream != received {
			t.Errorf("Missed a sequence, was expecting %d but got %d, last error: '%v'", received, meta.Sequence.Stream, nc.LastError())
			nc.Close()
		}
		m.Ack()
	})

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Failed to get all %d messages, only got %d", toSend, received)
	case <-done:
	}
}

func TestNoRaceJetStreamPushFlowControlHeartbeats_SubscribeSync(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	errHandler := nats.ErrorHandler(func(c *nats.Conn, sub *nats.Subscription, err error) {
		t.Logf("WARN: %s", err)
	})

	nc, js := jsClient(t, s, errHandler)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Burst and try to hit the flow control limit of the server.
	const totalMsgs = 16536
	payload := strings.Repeat("A", 1024)
	for i := 0; i < totalMsgs; i++ {
		if _, err := js.Publish("foo", []byte(fmt.Sprintf("i:%d/", i)+payload)); err != nil {
			t.Fatal(err)
		}
	}

	hbTimer := 100 * time.Millisecond
	sub, err := js.SubscribeSync("foo",
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(1),
		nats.EnableFlowControl(),
		nats.IdleHeartbeat(hbTimer),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}
	if !info.Config.FlowControl {
		t.Fatal("Expected Flow Control to be enabled")
	}
	if info.Config.Heartbeat != hbTimer {
		t.Errorf("Expected %v, got: %v", hbTimer, info.Config.Heartbeat)
	}

	m, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error getting next message: %v", err)
	}
	meta, err := m.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if meta.NumPending > totalMsgs {
		t.Logf("WARN: More pending messages than expected (%v), got: %v", totalMsgs, meta.NumPending)
	}
	err = m.Ack()
	if err != nil {
		t.Fatal(err)
	}

	recvd := 1
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		m, err := sub.NextMsg(1 * time.Second)
		if err != nil {
			t.Fatalf("Error getting next message: %v", err)
		}
		if len(m.Data) == 0 {
			t.Fatalf("Unexpected empty message: %+v", m)
		}

		if err := m.AckSync(); err != nil {
			t.Fatalf("Error on ack message: %v", err)
		}
		recvd++

		if recvd == totalMsgs {
			break
		}
	}

	t.Run("idle heartbeats", func(t *testing.T) {
		// Delay to get a few heartbeats.
		time.Sleep(4 * hbTimer)

		timeout = time.Now().Add(5 * time.Second)
		for time.Now().Before(timeout) {
			msg, err := sub.NextMsg(200 * time.Millisecond)
			if err != nil {
				if err == nats.ErrTimeout {
					// If timeout, ok to stop checking for the test.
					break
				}
				t.Fatal(err)
			}
			if len(msg.Data) == 0 {
				t.Fatalf("Unexpected empty message: %+v", m)
			}

			recvd++
			meta, err := msg.Metadata()
			if err != nil {
				t.Fatal(err)
			}
			if meta.NumPending == 0 {
				break
			}
		}
		if recvd > totalMsgs {
			t.Logf("WARN: Received more messages than expected (%v), got: %v", totalMsgs, recvd)
		}
	})

	t.Run("with context", func(t *testing.T) {
		sub, err := js.SubscribeSync("foo",
			nats.AckWait(30*time.Second),
			nats.Durable("bar"),
			nats.EnableFlowControl(),
			nats.IdleHeartbeat(hbTimer),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		info, err = sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		if !info.Config.FlowControl {
			t.Fatal("Expected Flow Control to be enabled")
		}

		recvd = 0
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			default:
			}

			m, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				t.Fatalf("Error getting next message: %v", err)
			}
			if len(m.Data) == 0 {
				t.Fatalf("Unexpected empty message: %+v", m)
			}

			if err := m.Ack(); err != nil {
				t.Fatalf("Error on ack message: %v", err)
			}
			recvd++

			if recvd >= totalMsgs {
				break
			}
		}

		// Delay to get a few heartbeats.
		time.Sleep(4 * hbTimer)
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
	FOR_LOOP:
		for {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					break FOR_LOOP
				}
			default:
			}

			msg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				if err == context.DeadlineExceeded {
					break
				}
				t.Fatal(err)
			}
			if len(msg.Data) == 0 {
				t.Fatalf("Unexpected empty message: %+v", m)
			}
			recvd++
			meta, err := msg.Metadata()
			if err != nil {
				t.Fatal(err)
			}
			if meta.NumPending == 0 {
				break
			}
		}
		if recvd > totalMsgs {
			t.Logf("WARN: Received more messages than expected (%v), got: %v", totalMsgs, recvd)
		}
	})
}

func TestNoRaceJetStreamPushFlowControlHeartbeats_SubscribeAsync(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Burst and try to hit the flow control limit of the server.
	const totalMsgs = 16536
	payload := strings.Repeat("A", 1024)
	for i := 0; i < totalMsgs; i++ {
		if _, err := js.Publish("foo", []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}

	recvd := make(chan *nats.Msg, totalMsgs)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errCh := make(chan error)
	hbTimer := 100 * time.Millisecond
	sub, err := js.Subscribe("foo", func(msg *nats.Msg) {
		if len(msg.Data) == 0 {
			errCh <- fmt.Errorf("Unexpected empty message: %+v", msg)
		}
		recvd <- msg

		if len(recvd) == totalMsgs {
			cancel()
		}
	}, nats.EnableFlowControl(), nats.IdleHeartbeat(hbTimer))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}
	if !info.Config.FlowControl {
		t.Fatal("Expected Flow Control to be enabled")
	}
	if info.Config.Heartbeat != hbTimer {
		t.Errorf("Expected %v, got: %v", hbTimer, info.Config.Heartbeat)
	}

	<-ctx.Done()

	got := len(recvd)
	expected := totalMsgs
	if got != expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	// Wait for a couple of heartbeats to arrive and confirm there is no error.
	select {
	case <-time.After(1 * time.Second):
	case err := <-errCh:
		t.Fatal(err)
	}
}

func TestNoRaceJetStreamPushFlowControlHeartbeats_ChanSubscribe(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	errHandler := nats.ErrorHandler(func(c *nats.Conn, sub *nats.Subscription, err error) {
		t.Logf("WARN: %s : %v", err, sub.Subject)
	})

	nc, js := jsClient(t, s, errHandler)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Burst and try to hit the flow control limit of the server.
	const totalMsgs = 16536
	payload := strings.Repeat("A", 1024)
	for i := 0; i < totalMsgs; i++ {
		if _, err := js.Publish("foo", []byte(fmt.Sprintf("i:%d/", i)+payload)); err != nil {
			t.Fatal(err)
		}
	}

	hbTimer := 100 * time.Millisecond
	mch := make(chan *nats.Msg, 16536)
	sub, err := js.ChanSubscribe("foo", mch,
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(1),
		nats.EnableFlowControl(),
		nats.IdleHeartbeat(hbTimer),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}
	if !info.Config.FlowControl {
		t.Fatal("Expected Flow Control to be enabled")
	}
	if info.Config.Heartbeat != hbTimer {
		t.Errorf("Expected %v, got: %v", hbTimer, info.Config.Heartbeat)
	}

	getNextMsg := func(mch chan *nats.Msg, timeout time.Duration) (*nats.Msg, error) {
		t.Helper()
		select {
		case m := <-mch:
			return m, nil
		case <-time.After(timeout):
			return nil, nats.ErrTimeout
		}
	}

	m, err := getNextMsg(mch, 1*time.Second)
	if err != nil {
		t.Fatalf("Error getting next message: %v", err)
	}
	meta, err := m.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if meta.NumPending > totalMsgs {
		t.Logf("WARN: More pending messages than expected (%v), got: %v", totalMsgs, meta.NumPending)
	}
	err = m.Ack()
	if err != nil {
		t.Fatal(err)
	}

	recvd := 1
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case m := <-mch:
			if err != nil {
				t.Fatalf("Error getting next message: %v", err)
			}
			if len(m.Data) == 0 {
				t.Fatalf("Unexpected empty message: %+v", m)
			}

			if err := m.Ack(); err != nil {
				t.Fatalf("Error on ack message: %v", err)
			}
			recvd++

			if recvd == totalMsgs {
				done()
			}
		}
	}

	t.Run("idle heartbeats", func(t *testing.T) {
		// Delay to get a few heartbeats.
		time.Sleep(4 * hbTimer)

		ctx, done := context.WithTimeout(context.Background(), 1*time.Second)
		defer done()
	Loop:
		for {
			select {
			case <-ctx.Done():
				break Loop
			case msg := <-mch:
				if err != nil {
					if err == nats.ErrTimeout {
						// If timeout, ok to stop checking for the test.
						break Loop
					}
					t.Fatal(err)
				}
				if len(msg.Data) == 0 {
					t.Fatalf("Unexpected empty message: %+v", m)
				}

				recvd++
				meta, err := msg.Metadata()
				if err != nil {
					t.Fatal(err)
				}
				if meta.NumPending == 0 {
					break Loop
				}
			}
		}
		if recvd > totalMsgs {
			t.Logf("WARN: Received more messages than expected (%v), got: %v", totalMsgs, recvd)
		}
	})
}

func TestNoRaceJetStreamPushFlowControl_SubscribeAsyncAndChannel(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	errCh := make(chan error)
	errHandler := nats.ErrorHandler(func(c *nats.Conn, sub *nats.Subscription, err error) {
		errCh <- err
	})
	nc, err := nats.Connect(s.ClientURL(), errHandler)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	const totalMsgs = 10_000

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(totalMsgs))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	go func() {
		payload := strings.Repeat("O", 4096)
		for i := 0; i < totalMsgs; i++ {
			js.PublishAsync("foo", []byte(payload))
		}
	}()

	// Small channel that blocks and then buffered channel that can deliver all
	// messages without blocking.
	recvd := make(chan *nats.Msg, 64)
	delivered := make(chan *nats.Msg, totalMsgs)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Dispatch channel consumer
	go func() {
		for m := range recvd {
			select {
			case <-ctx.Done():
				return
			default:
			}

			delivered <- m
			if len(delivered) == totalMsgs {
				cancel()
				return
			}
		}
	}()

	sub, err := js.Subscribe("foo", func(msg *nats.Msg) {
		// Cause bottleneck by having channel block when full
		// because of work taking long.
		recvd <- msg
	}, nats.EnableFlowControl(), nats.IdleHeartbeat(5*time.Second))

	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// Set this lower then normal to make sure we do not exceed bytes pending with FC turned on.
	sub.SetPendingLimits(totalMsgs, 4*1024*1024) // This matches server window for flowcontrol.

	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}
	if !info.Config.FlowControl {
		t.Fatal("Expected Flow Control to be enabled")
	}
	<-ctx.Done()

	got := len(delivered)
	expected := totalMsgs
	if got != expected {
		t.Errorf("Expected %d messages, got: %d", expected, got)
	}

	// Wait for a couple of heartbeats to arrive and confirm there is no error.
	select {
	case <-time.After(1 * time.Second):
	case err := <-errCh:
		t.Errorf("error handler: %v", err)
	}
}

func TestNoRaceJetStreamChanSubscribeStall(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: enabled
		no_auth_user: pc
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: pc, password: foo} ]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create a stream.
	if _, err = js.AddStream(&nats.StreamConfig{Name: "STALL"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.StreamInfo("STALL")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	msg := []byte(strings.Repeat("A", 512))
	toSend := 100_000
	for i := 0; i < toSend; i++ {
		// Use plain NATS here for speed.
		if _, err := js.PublishAsync("STALL", msg); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for messages")
	}
	nc.Flush()

	batch := 100
	msgs := make(chan *nats.Msg, batch-2)
	sub, err := js.ChanSubscribe("STALL", msgs,
		nats.Durable("dlc"),
		nats.EnableFlowControl(),
		nats.IdleHeartbeat(5*time.Second),
		nats.MaxAckPending(batch-2),
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	for received := 0; received < toSend; {
		select {
		case m := <-msgs:
			received++
			meta, _ := m.Metadata()
			if meta.Sequence.Consumer != uint64(received) {
				t.Fatalf("Missed something, wanted %d but got %d", received, meta.Sequence.Consumer)
			}
			m.Ack()
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for messages, last received was %d", received)
		}
	}
}
