// Copyright 2022-2025 The NATS Authors
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
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrderedConsumerConsume(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	t.Run("base usage, delete consumer", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, js)
		wg.Wait()

		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		wg.Add(len(testMsgs))
		publishTestMsgs(t, js)
		wg.Wait()

		l.Stop()
	})

	t.Run("reset consumer before receiving any messages", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg := &sync.WaitGroup{}
		l, err := c.Consume(func(msg jetstream.Msg) {
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(500 * time.Millisecond)

		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		wg.Add(len(testMsgs))
		publishTestMsgs(t, js)
		wg.Wait()

		l.Stop()
	})

	t.Run("with custom start seq", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		publishTestMsgs(t, js)
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: 3})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs) - 2)
		l, err := c.Consume(func(msg jetstream.Msg) {
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		wg.Wait()

		time.Sleep(500 * time.Millisecond)
		// now delete consumer again and publish some more messages, all should be received normally
		info, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if err := s.DeleteConsumer(ctx, info.Config.Name); err != nil {
			t.Fatal(err)
		}
		wg.Add(len(testMsgs))
		publishTestMsgs(t, js)
		wg.Wait()
	})

	t.Run("base usage, server shutdown", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		errs := make(chan error)
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
			if errors.Is(err, jetstream.ErrConsumerNotFound) {
				errs <- err
			}
		}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, js)
		wg.Wait()

		srv = restartBasicJSServer(t, srv)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		wg.Add(len(testMsgs))
		publishTestMsgs(t, js)
		wg.Wait()

		l.Stop()
	})

	t.Run("base usage, missing heartbeat", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// we have to delete consumer before trying to consume
		// in order to get missing heartbeats
		if err := s.DeleteConsumer(ctx, c.CachedInfo().Config.Name); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		errs := make(chan error)
		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
			errs <- err
		}), jetstream.PullHeartbeat(1*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, js)
		select {
		case err := <-errs:
			if !errors.Is(err, jetstream.ErrNoHeartbeat) {
				t.Fatalf("Expected error: %v; got: %v", jetstream.ErrNoHeartbeat, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for error")
		}
		wg.Wait()

		wg.Add(len(testMsgs))
		publishTestMsgs(t, js)
		wg.Wait()
		l.Stop()
	})

	t.Run("consumer used as fetch", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, js)
		msgs, err := c.Fetch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for range msgs.Messages() {
		}
		if _, err := c.Consume(func(msg jetstream.Msg) {}); !errors.Is(err, jetstream.ErrOrderConsumerUsedAsFetch) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrOrderConsumerUsedAsFetch, err)
		}
	})

	t.Run("error running concurrent consume requests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		cc, err := c.Consume(func(msg jetstream.Msg) {})
		defer cc.Stop()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := c.Consume(func(msg jetstream.Msg) {}); !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrOrderedConsumerConcurrentRequests, err)
		}
	})

	t.Run("with auto unsubscribe", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(50)
		_, err = c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			msg.Ack()
			wg.Done()
		}, jetstream.StopAfter(50), jetstream.PullMaxMessages(40))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg.Wait()
		time.Sleep(10 * time.Millisecond)
		ci, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.NumPending != 50 {
			t.Fatalf("Unexpected number of pending messages; want 50; got %d", ci.NumPending)
		}
		if ci.NumAckPending != 0 {
			t.Fatalf("Unexpected number of ack pending messages; want 0; got %d", ci.NumAckPending)
		}
		if ci.NumWaiting != 0 {
			t.Fatalf("Unexpected number of waiting pull requests; want 0; got %d", ci.NumWaiting)
		}
		time.Sleep(10 * time.Millisecond)
	})

	t.Run("with auto unsubscribe and consumer reset", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(100)
		_, err = c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			msg.Ack()
			wg.Done()
		}, jetstream.StopAfter(150), jetstream.PullMaxMessages(40))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		wg.Wait()
		if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		wg.Add(50)
		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		wg.Wait()

		time.Sleep(10 * time.Millisecond)
		ci, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.NumPending != 50 {
			t.Fatalf("Unexpected number of pending messages; want 50; got %d", ci.NumPending)
		}
		if ci.NumAckPending != 0 {
			t.Fatalf("Unexpected number of ack pending messages; want 0; got %d", ci.NumAckPending)
		}
		if ci.NumWaiting != 0 {
			t.Fatalf("Unexpected number of waiting pull requests; want 0; got %d", ci.NumWaiting)
		}
		time.Sleep(10 * time.Millisecond)
	})

	t.Run("drain mode", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		wg := &sync.WaitGroup{}
		wg.Add(5)
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		cc.Drain()
		wg.Wait()
	})

	t.Run("stop consume during reset", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for i := 0; i < 10; i++ {
			c, err := s.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			cc, err := c.Consume(func(msg jetstream.Msg) {
				msg.Ack()
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if err := s.DeleteConsumer(context.Background(), c.CachedInfo().Name); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			cc.Stop()
			time.Sleep(50 * time.Millisecond)
		}
	})

	t.Run("wait for closed after drain", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs := make([]jetstream.Msg, 0)
		lock := sync.Mutex{}
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			lock.Lock()
			msgs = append(msgs, msg)
			lock.Unlock()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		closed := cc.Closed()
		time.Sleep(100 * time.Millisecond)
		if err := s.DeleteConsumer(context.Background(), c.CachedInfo().Name); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		publishTestMsgs(t, js)

		// wait for the consumer to be recreated before calling drain
		for i := 0; i < 5; i++ {
			_, err = c.Info(ctx)
			if err != nil {
				if errors.Is(err, jetstream.ErrConsumerNotFound) {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				t.Fatalf("Unexpected error: %v", err)
			}
			break
		}

		cc.Drain()

		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for consume to be closed")
		}

		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Unexpected received message count after consume closed; want %d; got %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("wait for closed on already closed consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs := make([]jetstream.Msg, 0)
		lock := sync.Mutex{}
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			lock.Lock()
			msgs = append(msgs, msg)
			lock.Unlock()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		if err := s.DeleteConsumer(context.Background(), c.CachedInfo().Name); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		cc.Stop()

		time.Sleep(100 * time.Millisecond)

		select {
		case <-cc.Closed():
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for consume to be closed")
		}
	})
}

func TestOrderedConsumerMessages(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	t.Run("base usage, delete consumer", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer it.Stop()

		publishTestMsgs(t, js)
		for i := 0; i < 5; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs = append(msgs, msg)
		}
		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		publishTestMsgs(t, js)
		for i := 0; i < 5; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs = append(msgs, msg)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("base usage, server restart", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer it.Stop()

		publishTestMsgs(t, js)
		for i := 0; i < 5; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs = append(msgs, msg)
		}
		srv = restartBasicJSServer(t, srv)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		publishTestMsgs(t, js)
		for i := 0; i < 5; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs = append(msgs, msg)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("base usage, missing heartbeat", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages(jetstream.PullHeartbeat(1 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer it.Stop()

		publishTestMsgs(t, js)
		for i := 0; i < 5; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs = append(msgs, msg)
		}
		publishTestMsgs(t, js)
		for i := 0; i < 5; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs = append(msgs, msg)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("with auto unsubscribe", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "test", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages(jetstream.StopAfter(50), jetstream.PullMaxMessages(40))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 50; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			if err := msg.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)

		}
		if _, err := it.Next(); err != jetstream.ErrMsgIteratorClosed {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgIteratorClosed, err)
		}
		if len(msgs) != 50 {
			t.Fatalf("Unexpected received message count; want %d; got %d", 50, len(msgs))
		}
		ci, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.NumPending != 50 {
			t.Fatalf("Unexpected number of pending messages; want 50; got %d", ci.NumPending)
		}
		if ci.NumAckPending != 0 {
			t.Fatalf("Unexpected number of ack pending messages; want 0; got %d", ci.NumAckPending)
		}
		if ci.NumWaiting != 0 {
			t.Fatalf("Unexpected number of waiting pull requests; want 0; got %d", ci.NumWaiting)
		}
	})

	t.Run("with auto unsubscribe and consumer reset", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "test", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages(jetstream.StopAfter(150), jetstream.PullMaxMessages(40))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 100; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			if err := msg.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
		}
		if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		for i := 0; i < 50; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			if err := msg.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
		}
		if _, err := it.Next(); err != jetstream.ErrMsgIteratorClosed {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgIteratorClosed, err)
		}
		if len(msgs) != 150 {
			t.Fatalf("Unexpected received message count; want %d; got %d", 50, len(msgs))
		}
		ci, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.NumPending != 50 {
			t.Fatalf("Unexpected number of pending messages; want 50; got %d", ci.NumPending)
		}
		if ci.NumAckPending != 0 {
			t.Fatalf("Unexpected number of ack pending messages; want 0; got %d", ci.NumAckPending)
		}
		if ci.NumWaiting != 0 {
			t.Fatalf("Unexpected number of waiting pull requests; want 0; got %d", ci.NumWaiting)
		}
	})

	t.Run("consumer used as fetch", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, js)
		msgs, err := c.Fetch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for range msgs.Messages() {
		}
		if _, err := c.Messages(); !errors.Is(err, jetstream.ErrOrderConsumerUsedAsFetch) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrOrderConsumerUsedAsFetch, err)
		}
	})

	t.Run("error running concurrent consume requests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		cc, err := c.Messages()
		defer cc.Stop()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := c.Messages(); !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrOrderedConsumerConcurrentRequests, err)
		}
	})

	t.Run("drain mode", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, js)
		go func() {
			time.Sleep(100 * time.Millisecond)
			it.Drain()
		}()
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			msgs = append(msgs, msg)
		}
		_, err = it.Next()
		if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgIteratorClosed, err)
		}

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count after drain; want %d; got %d", len(testMsgs), len(msgs))
		}
	})
}

func TestOrderedConsumerFetch(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	t.Run("base usage, delete consumer", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)

		publishTestMsgs(t, js)
		res, err := c.Fetch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		publishTestMsgs(t, js)
		res, err = c.Fetch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("with custom deliver policy", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs := make([]jetstream.Msg, 0)

		for i := 0; i < 5; i++ {
			if _, err := js.Publish(context.Background(), "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		for i := 0; i < 5; i++ {
			if _, err := js.Publish(context.Background(), "FOO.B", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}

		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
			DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		res, err := c.Fetch(int(c.CachedInfo().NumPending), jetstream.FetchMaxWait(1*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}

		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		if len(msgs) != 2 {
			t.Fatalf("Expected %d messages; got: %d", 2, len(msgs))
		}
		expectedSubjects := []string{"FOO.A", "FOO.B"}

		for i := range msgs {
			if msgs[i].Subject() != expectedSubjects[i] {
				t.Fatalf("Expected subject: %s; got: %s", expectedSubjects[i], msgs[i].Subject())
			}
		}
	})

	t.Run("consumer used as consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		cc.Stop()

		_, err = c.Fetch(5)
		if !errors.Is(err, jetstream.ErrOrderConsumerUsedAsConsume) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderConsumerUsedAsConsume, err)
		}
	})

	t.Run("concurrent fetch requests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, js)
		res, err := c.Fetch(1, jetstream.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		_, err = c.Fetch(1)
		if !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderedConsumerConcurrentRequests, err)
		}
		for msg := range res.Messages() {
			msg.Ack()
		}
	})
}

func TestOrderedConsumerFetchBytes(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	t.Run("base usage, delete consumer", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)

		publishTestMsgs(t, js)
		res, err := c.FetchBytes(500, jetstream.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		publishTestMsgs(t, js)
		res, err = c.Fetch(500, jetstream.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("consumer used as consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		cc.Stop()

		_, err = c.FetchBytes(500)
		if !errors.Is(err, jetstream.ErrOrderConsumerUsedAsConsume) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderConsumerUsedAsConsume, err)
		}
	})

	t.Run("concurrent fetch requests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, js)
		res, err := c.FetchBytes(500, jetstream.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		_, err = c.FetchBytes(500)
		if !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderedConsumerConcurrentRequests, err)
		}
		for msg := range res.Messages() {
			msg.Ack()
		}
	})
}

func TestOrderedConsumerNext(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	t.Run("base usage, delete consumer", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, js)
		_, err = c.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		_, err = c.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
	})

	t.Run("consumer used as consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		cc.Stop()

		_, err = c.Next()
		if !errors.Is(err, jetstream.ErrOrderConsumerUsedAsConsume) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderConsumerUsedAsConsume, err)
		}
	})

	t.Run("preserve sequence after fetch error", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error during publish: %s", err)
		}
		msg, err := c.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if meta.Sequence.Stream != 1 {
			t.Fatalf("Expected sequence: %d; got: %d", 1, meta.Sequence.Stream)
		}

		// get next message, it should time out (no more messages on stream)
		_, err = c.Next(jetstream.FetchMaxWait(100 * time.Millisecond))
		if !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("Expected error: %s; got: %s", nats.ErrTimeout, err)
		}

		if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error during publish: %s", err)
		}

		// get next message, it should have stream sequence 2
		msg, err = c.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		meta, err = msg.Metadata()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if meta.Sequence.Stream != 2 {
			t.Fatalf("Expected sequence: %d; got: %d", 2, meta.Sequence.Stream)
		}
	})
}

func TestOrderedConsumerFetchNoWait(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	t.Run("base usage, delete consumer", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)

		publishTestMsgs(t, js)
		res, err := c.FetchNoWait(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		publishTestMsgs(t, js)
		res, err = c.FetchNoWait(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("consumer used as consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		cc.Stop()

		_, err = c.FetchNoWait(5)
		if !errors.Is(err, jetstream.ErrOrderConsumerUsedAsConsume) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderConsumerUsedAsConsume, err)
		}
	})
}

func TestOrderedConsumerInfo(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := js.OrderedConsumer(ctx, "foo", jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cc, err := c.Consume(func(msg jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	defer cc.Stop()

	info, err := c.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	initialName := info.Name

	if err := s.DeleteConsumer(ctx, initialName); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	time.Sleep(50 * time.Millisecond)

	info, err = c.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if info.Name == initialName {
		t.Fatalf("New consumer should be returned; got: %s", info.Name)
	}
}

func TestOrderedConsumerNextTimeout(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = c.Next(jetstream.FetchMaxWait(1 * time.Second))
	if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("Expected error: %v; got: %v", nats.ErrTimeout, err)
	}
}

func TestOrderedConsumerNextOrder(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publishFailed := make(chan error, 1)

	go func() {
		for i := 0; i < 1000; i++ {
			_, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("%d", 1)))
			if err != nil {
				publishFailed <- err
			}
		}
	}()

	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 1000; i++ {

		select {
		case err := <-publishFailed:
			t.Fatalf("Publish error: %v", err)
		default:
		}

		msg, err := c.Next(jetstream.FetchMaxWait(5 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if meta.Sequence.Stream != uint64(i+1) {
			t.Fatalf("Unexpected sequence number: %d", meta.Sequence.Stream)
		}
	}
}

func TestOrderedConsumerConfig(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	s, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	tests := []struct {
		name     string
		config   jetstream.OrderedConsumerConfig
		expected jetstream.ConsumerConfig
	}{
		{
			name:   "default config",
			config: jetstream.OrderedConsumerConfig{},
			expected: jetstream.ConsumerConfig{
				DeliverPolicy:     jetstream.DeliverAllPolicy,
				AckPolicy:         jetstream.AckNonePolicy,
				MaxDeliver:        -1,
				MaxWaiting:        512,
				InactiveThreshold: 5 * time.Minute,
				Replicas:          1,
				MemoryStorage:     true,
			},
		},
		{
			name: "custom inactive threshold",
			config: jetstream.OrderedConsumerConfig{
				InactiveThreshold: 10 * time.Second,
			},
			expected: jetstream.ConsumerConfig{
				DeliverPolicy:     jetstream.DeliverAllPolicy,
				AckPolicy:         jetstream.AckNonePolicy,
				MaxDeliver:        -1,
				MaxWaiting:        512,
				InactiveThreshold: 10 * time.Second,
				Replicas:          1,
				MemoryStorage:     true,
			},
		},
		{
			name: "custom opt start seq and inactive threshold",
			config: jetstream.OrderedConsumerConfig{
				DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
				OptStartSeq:       10,
				InactiveThreshold: 10 * time.Second,
			},
			expected: jetstream.ConsumerConfig{
				OptStartSeq:       10,
				DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
				AckPolicy:         jetstream.AckNonePolicy,
				MaxDeliver:        -1,
				MaxWaiting:        512,
				InactiveThreshold: 10 * time.Second,
				Replicas:          1,
				MemoryStorage:     true,
			},
		},
		{
			name: "all fields customized, start with custom seq",
			config: jetstream.OrderedConsumerConfig{
				FilterSubjects:    []string{"foo.a", "foo.b"},
				DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
				OptStartSeq:       10,
				ReplayPolicy:      jetstream.ReplayOriginalPolicy,
				InactiveThreshold: 10 * time.Second,
				HeadersOnly:       true,
				Metadata:          map[string]string{"foo": "a"},
			},
			expected: jetstream.ConsumerConfig{
				FilterSubjects:    []string{"foo.a", "foo.b"},
				OptStartSeq:       10,
				DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
				AckPolicy:         jetstream.AckNonePolicy,
				MaxDeliver:        -1,
				MaxWaiting:        512,
				InactiveThreshold: 10 * time.Second,
				Replicas:          1,
				MemoryStorage:     true,
				HeadersOnly:       true,
				Metadata:          map[string]string{"foo": "a"},
			},
		},
		{
			name: "all fields customized, start with custom time",
			config: jetstream.OrderedConsumerConfig{
				FilterSubjects:    []string{"foo.a", "foo.b"},
				DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
				OptStartTime:      &time.Time{},
				ReplayPolicy:      jetstream.ReplayOriginalPolicy,
				InactiveThreshold: 10 * time.Second,
				HeadersOnly:       true,
				Metadata:          map[string]string{"foo": "a"},
			},
			expected: jetstream.ConsumerConfig{
				FilterSubjects:    []string{"foo.a", "foo.b"},
				OptStartTime:      &time.Time{},
				DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
				AckPolicy:         jetstream.AckNonePolicy,
				MaxDeliver:        -1,
				MaxWaiting:        512,
				InactiveThreshold: 10 * time.Second,
				Replicas:          1,
				MemoryStorage:     true,
				HeadersOnly:       true,
				Metadata:          map[string]string{"foo": "a"},
			},
		},
		{
			name: "both start seq and time set, deliver policy start seq",
			config: jetstream.OrderedConsumerConfig{
				FilterSubjects:    []string{"foo.a", "foo.b"},
				DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
				OptStartSeq:       10,
				OptStartTime:      &time.Time{},
				ReplayPolicy:      jetstream.ReplayOriginalPolicy,
				InactiveThreshold: 10 * time.Second,
				HeadersOnly:       true,
			},
			expected: jetstream.ConsumerConfig{
				FilterSubjects:    []string{"foo.a", "foo.b"},
				OptStartSeq:       10,
				OptStartTime:      nil,
				DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
				AckPolicy:         jetstream.AckNonePolicy,
				MaxDeliver:        -1,
				MaxWaiting:        512,
				InactiveThreshold: 10 * time.Second,
				Replicas:          1,
				MemoryStorage:     true,
				HeadersOnly:       true,
			},
		},
		{
			name: "both start seq and time set, deliver policy start time",
			config: jetstream.OrderedConsumerConfig{
				FilterSubjects:    []string{"foo.a", "foo.b"},
				DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
				OptStartSeq:       10,
				OptStartTime:      &time.Time{},
				ReplayPolicy:      jetstream.ReplayOriginalPolicy,
				InactiveThreshold: 10 * time.Second,
				HeadersOnly:       true,
			},
			expected: jetstream.ConsumerConfig{
				FilterSubjects:    []string{"foo.a", "foo.b"},
				OptStartSeq:       0,
				OptStartTime:      &time.Time{},
				DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
				AckPolicy:         jetstream.AckNonePolicy,
				MaxDeliver:        -1,
				MaxWaiting:        512,
				InactiveThreshold: 10 * time.Second,
				Replicas:          1,
				MemoryStorage:     true,
				HeadersOnly:       true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := s.OrderedConsumer(context.Background(), test.config)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			cfg := c.CachedInfo().Config
			test.expected.Name = cfg.Name

			if test.config.Metadata != nil {
				for k, v := range test.config.Metadata {
					if cfg.Metadata[k] != v {
						t.Fatalf("Expected config %+v, got %+v", test.expected, cfg)
					}
				}
			}
			test.expected.Metadata = cfg.Metadata
			if !reflect.DeepEqual(test.expected, cfg) {
				t.Fatalf("Expected config %+v, got %+v", test.expected, cfg)
			}
		})
	}
}
