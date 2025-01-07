// Copyright 2020-2025 The NATS Authors
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
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand"
	"net"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	natsserver "github.com/nats-io/nats-server/v2/test"
)

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

func restartBasicJSServer(t *testing.T, s *server.Server) *server.Server {
	opts := natsserver.DefaultTestOptions
	clientURL, err := url.Parse(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	port, err := strconv.Atoi(clientURL.Port())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	opts.Port = port
	opts.JetStream = true
	opts.StoreDir = s.JetStreamConfig().StoreDir
	s.Shutdown()
	s.WaitForShutdown()
	return RunServerWithOptions(&opts)
}

func TestJetStreamNotEnabled(t *testing.T) {
	s := RunServerOnPort(-1)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AccountInfo(); err != nats.ErrJetStreamNotEnabled {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestJetStreamErrors(t *testing.T) {
	t.Run("API error", func(t *testing.T) {
		conf := createConfFile(t, []byte(`
			listen: 127.0.0.1:-1
			no_auth_user: rip
			jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
			accounts: {
				JS: {
					jetstream: enabled
					users: [ {user: dlc, password: foo} ]
				},
				IU: {
					users: [ {user: rip, password: bar} ]
				},
			}
		`))
		defer os.Remove(conf)

		s, _ := RunServerWithConfig(conf)
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		_, err := js.AccountInfo()
		// check directly to var (backwards compatible)
		if err != nats.ErrJetStreamNotEnabledForAccount {
			t.Fatalf("Did not get the proper error, got %v", err)
		}

		// matching via errors.Is
		if ok := errors.Is(err, nats.ErrJetStreamNotEnabledForAccount); !ok {
			t.Fatal("Expected ErrJetStreamNotEnabledForAccount")
		}

		// matching wrapped via error.Is
		err2 := fmt.Errorf("custom error: %w", nats.ErrJetStreamNotEnabledForAccount)
		if ok := errors.Is(err2, nats.ErrJetStreamNotEnabledForAccount); !ok {
			t.Fatal("Expected wrapped ErrJetStreamNotEnabled")
		}

		// via classic type assertion.
		jserr, ok := err.(nats.JetStreamError)
		if !ok {
			t.Fatal("Expected a JetStreamError")
		}
		expected := nats.JSErrCodeJetStreamNotEnabledForAccount
		if jserr.APIError().ErrorCode != expected {
			t.Fatalf("Expected: %v, got: %v", expected, jserr.APIError().ErrorCode)
		}
		if jserr.APIError() == nil {
			t.Fatal("Expected APIError")
		}

		// matching to interface via errors.As(...)
		var apierr nats.JetStreamError
		ok = errors.As(err, &apierr)
		if !ok {
			t.Fatal("Expected a JetStreamError")
		}
		if apierr.APIError() == nil {
			t.Fatal("Expected APIError")
		}
		if apierr.APIError().ErrorCode != expected {
			t.Fatalf("Expected: %v, got: %v", expected, apierr.APIError().ErrorCode)
		}
		expectedMessage := "nats: jetstream not enabled for account"
		if apierr.Error() != expectedMessage {
			t.Fatalf("Expected: %v, got: %v", expectedMessage, apierr.Error())
		}

		// an APIError also implements the JetStreamError interface.
		var _ nats.JetStreamError = &nats.APIError{}

		// matching arbitrary custom error via errors.Is(...)
		customErr := &nats.APIError{ErrorCode: expected}
		if ok := errors.Is(customErr, nats.ErrJetStreamNotEnabledForAccount); !ok {
			t.Fatal("Expected wrapped ErrJetStreamNotEnabledForAccount")
		}
		customErr = &nats.APIError{ErrorCode: 1}
		if ok := errors.Is(customErr, nats.ErrJetStreamNotEnabledForAccount); ok {
			t.Fatal("Expected to not match ErrJetStreamNotEnabled")
		}
		var cerr nats.JetStreamError
		if ok := errors.As(customErr, &cerr); !ok {
			t.Fatal("Expected custom error to be a JetStreamError")
		}

		// matching to concrete type via errors.As(...)
		var aerr *nats.APIError
		ok = errors.As(err, &aerr)
		if !ok {
			t.Fatal("Expected an APIError")
		}
		if aerr.ErrorCode != expected {
			t.Fatalf("Expected: %v, got: %v", expected, aerr.ErrorCode)
		}
		expectedMessage = "nats: jetstream not enabled for account"
		if aerr.Error() != expectedMessage {
			t.Fatalf("Expected: %v, got: %v", expectedMessage, apierr.Error())
		}
	})

	t.Run("test non-api error", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		// stream with empty name
		_, err := js.AddStream(&nats.StreamConfig{})
		if err == nil {
			t.Fatalf("Expected error, got nil")
		}

		// check directly to var (backwards compatible)
		if err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidStreamName, err)
		}

		// matching via errors.Is
		if ok := errors.Is(err, nats.ErrStreamNameRequired); !ok {
			t.Fatalf("Expected: %v; got: %v", nats.ErrStreamNameRequired, err)
		}

		// matching wrapped via error.Is
		err2 := fmt.Errorf("custom error: %w", nats.ErrStreamNameRequired)
		if ok := errors.Is(err2, nats.ErrStreamNameRequired); !ok {
			t.Fatal("Expected wrapped ErrStreamNameRequired")
		}

		// via classic type assertion.
		jserr, ok := err.(nats.JetStreamError)
		if !ok {
			t.Fatal("Expected a JetStreamError")
		}
		if jserr.APIError() != nil {
			t.Fatalf("Expected: empty APIError; got: %v", jserr.APIError())
		}

		// matching to interface via errors.As(...)
		var jserr2 nats.JetStreamError
		ok = errors.As(err, &jserr2)
		if !ok {
			t.Fatal("Expected a JetStreamError")
		}
		if jserr2.APIError() != nil {
			t.Fatalf("Expected: empty APIError; got: %v", jserr2.APIError())
		}
		expectedMessage := "nats: stream name is required"
		if jserr2.Error() != expectedMessage {
			t.Fatalf("Expected: %v, got: %v", expectedMessage, jserr2.Error())
		}

		// matching to concrete type via errors.As(...)
		var aerr *nats.APIError
		ok = errors.As(err, &aerr)
		if ok {
			t.Fatal("Expected ErrStreamNameRequired not to map to APIError")
		}
	})

}

func TestJetStreamPublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Make sure we get a proper failure when no stream is present.
	_, err = js.Publish("foo", []byte("Hello JS"))
	if err != nats.ErrNoStreamResponse {
		t.Fatalf("Expected a no stream error but got %v", err)
	}

	// Create the stream using our client API.
	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test", "foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Double check that file-based storage is default.
	if si.Config.Storage != nats.FileStorage {
		t.Fatalf("Expected FileStorage as default, got %v", si.Config.Storage)
	}

	// Lookup the stream for testing.
	_, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	var pa *nats.PubAck
	expect := func(seq, nmsgs uint64) {
		t.Helper()
		if seq > 0 && pa == nil {
			t.Fatalf("Missing pubAck to test sequence %d", seq)
		}
		if pa != nil {
			if pa.Stream != "TEST" {
				t.Fatalf("Wrong stream name, expected %q, got %q", "TEST", pa.Stream)
			}
			if seq > 0 && pa.Sequence != seq {
				t.Fatalf("Wrong stream sequence, expected %d, got %d", seq, pa.Sequence)
			}
		}

		stream, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("stream lookup failed: %v", err)
		}
		if stream.State.Msgs != nmsgs {
			t.Fatalf("Expected %d messages, got %d", nmsgs, stream.State.Msgs)
		}
	}

	msg := []byte("Hello JS")

	// Basic publish like NATS core.
	pa, err = js.Publish("foo", msg)
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	expect(1, 1)

	// Test stream expectation.
	pa, err = js.Publish("foo", msg, nats.ExpectStream("ORDERS"))
	if err == nil || !strings.Contains(err.Error(), "stream does not match") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Test last sequence expectation.
	pa, err = js.Publish("foo", msg, nats.ExpectLastSequence(10))
	if err == nil || !strings.Contains(err.Error(), "wrong last sequence") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Messages should have been rejected.
	expect(0, 1)

	// Using PublishMsg API and accessing directly the Header map.
	msg2 := nats.NewMsg("foo")
	msg2.Header[nats.ExpectedLastSeqHdr] = []string{"10"}
	pa, err = js.PublishMsg(msg2)
	if err == nil || !strings.Contains(err.Error(), "wrong last sequence") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Messages should have been rejected.
	expect(0, 1)

	// Send in a stream with a msgId
	pa, err = js.Publish("foo", msg, nats.MsgId("ZZZ"))
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	expect(2, 2)

	// Send in the same message with same msgId.
	pa, err = js.Publish("foo", msg, nats.MsgId("ZZZ"))
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	if pa.Sequence != 2 {
		t.Fatalf("Expected sequence of 2, got %d", pa.Sequence)
	}
	if !pa.Duplicate {
		t.Fatalf("Expected duplicate to be set")
	}
	expect(2, 2)

	// Now try to send one in with the wrong last msgId.
	pa, err = js.Publish("foo", msg, nats.ExpectLastMsgId("AAA"))
	if err == nil || !strings.Contains(err.Error(), "wrong last msg") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Make sure expected sequence works.
	pa, err = js.Publish("foo", msg, nats.ExpectLastSequence(22))
	if err == nil || !strings.Contains(err.Error(), "wrong last sequence") {
		t.Fatalf("Expected an error, got %v", err)
	}
	expect(0, 2)

	// This should work ok.
	pa, err = js.Publish("foo", msg, nats.ExpectLastSequence(2))
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	expect(3, 3)

	// JetStream Headers are case-sensitive right now,
	// so this will not activate the check.
	msg3 := nats.NewMsg("foo")
	msg3.Header["nats-expected-last-sequence"] = []string{"4"}
	pa, err = js.PublishMsg(msg3)
	if err != nil {
		t.Fatalf("Expected an error, got %v", err)
	}
	expect(4, 4)

	// Now test context and timeouts.
	// Both set should fail.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = js.Publish("foo", msg, nats.AckWait(time.Second), nats.Context(ctx))
	if err != nats.ErrContextAndTimeout {
		t.Fatalf("Expected %q, got %q", nats.ErrContextAndTimeout, err)
	}

	// Create dummy listener for timeout and context tests.
	sub, err := nc.SubscribeSync("baz")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	_, err = js.Publish("baz", msg, nats.AckWait(time.Nanosecond))
	if err != nats.ErrTimeout {
		t.Fatalf("Expected %q, got %q", nats.ErrTimeout, err)
	}

	go cancel()
	_, err = js.Publish("baz", msg, nats.Context(ctx))
	if err != context.Canceled {
		t.Fatalf("Expected %q, got %q", context.Canceled, err)
	}

	// Test ExpectLastSequencePerSubject. Just make sure that we set the header.
	sub, err = nc.SubscribeSync("test")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	js.Publish("test", []byte("msg"), nats.ExpectLastSequencePerSubject(1))
	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error on next msg: %v", err)
	}
	if m.Header.Get(nats.ExpectedLastSubjSeqHdr) != "1" {
		t.Fatalf("Header ExpectLastSequencePerSubject not set: %+v", m.Header)
	}
}

func TestJetStreamSubscribe(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	expectConsumers := func(t *testing.T, expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			var infos []*nats.ConsumerInfo
			for info := range js.Consumers("TEST") {
				infos = append(infos, info)
			}
			if len(infos) != expected {
				return fmt.Errorf("Expected %d consumers, got: %d", expected, len(infos))
			}
			return nil
		})
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz", "foo.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Lookup the stream for testing.
	_, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	// If stream name is not specified, then the subject is required.
	if _, err := js.SubscribeSync(""); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check that if stream name is present, then technically the subject does not have to.
	sub, err := js.SubscribeSync("", nats.BindStream("TEST"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	initialPending, err := sub.InitialConsumerPending()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if initialPending != 0 {
		t.Fatalf("Expected no initial pending, got %d", initialPending)
	}
	sub.Unsubscribe()

	// Check that Queue subscribe with HB or FC fails.
	_, err = js.QueueSubscribeSync("foo", "wq", nats.IdleHeartbeat(time.Second))
	if err == nil || !strings.Contains(err.Error(), "heartbeat") {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.QueueSubscribeSync("foo", "wq", nats.EnableFlowControl())
	if err == nil || !strings.Contains(err.Error(), "flow control") {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that Queue subscribe without durable name requires queue name
	// to not have "." in the name.
	_, err = js.QueueSubscribeSync("foo", "bar.baz")
	if err != nats.ErrInvalidConsumerName {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg := []byte("Hello JS")

	// Basic publish like NATS core.
	js.Publish("foo", msg)

	q := make(chan *nats.Msg, 4)

	checkSub, err := nc.SubscribeSync("ivan")
	if err != nil {
		t.Fatalf("Error on sub: %v", err)
	}

	// Now create a simple ephemeral consumer.
	sub1, err := js.Subscribe("foo", func(m *nats.Msg) {
		q <- m
	}, nats.DeliverSubject("ivan"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub1.Unsubscribe()

	select {
	case m := <-q:
		if _, err := m.Metadata(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := checkSub.NextMsg(time.Second); err != nil {
			t.Fatal("Wrong deliver subject")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the messages in time")
	}

	// Now do same but sync.
	sub2, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub2.Unsubscribe()

	waitForPending := func(t *testing.T, sub *nats.Subscription, n int) {
		t.Helper()
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _, _ := sub.Pending(); msgs == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		msgs, _, _ := sub.Pending()
		t.Fatalf("Expected to receive %d messages, but got %d", n, msgs)
	}

	waitForPending(t, sub2, 1)

	toSend := 10
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}

	done := make(chan bool, 1)
	var received int
	sub3, err := js.Subscribe("bar", func(m *nats.Msg) {
		received++
		if received == toSend {
			done <- true
		}
	})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectConsumers(t, 3)
	defer sub3.Unsubscribe()

	initialPending, err = sub3.InitialConsumerPending()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if initialPending != 10 {
		t.Fatalf("Expected initial pending of 10, got %d", initialPending)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive all of the messages in time")
	}

	// If we are here we have received all of the messages.
	// We hang the ConsumerInfo option off of the subscription, so we use that to check status.
	// We may need to retry this check since the acks sent by the client have to be processed
	// on the server.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		info, _ := sub3.ConsumerInfo()
		if info.Config.AckPolicy != nats.AckExplicitPolicy {
			t.Fatalf("Expected ack explicit policy, got %q", info.Config.AckPolicy)
		}
		if info.Delivered.Consumer != uint64(toSend) {
			return fmt.Errorf("Expected to have received all %d messages, got %d", toSend, info.Delivered.Consumer)
		}
		// Make sure we auto-ack'd
		if info.AckFloor.Consumer != uint64(toSend) {
			return fmt.Errorf("Expected to have ack'd all %d messages, got ack floor of %d", toSend, info.AckFloor.Consumer)
		}
		return nil
	})
	sub3.Unsubscribe()
	sub2.Unsubscribe()
	sub1.Unsubscribe()
	expectConsumers(t, 0)

	// Now create a sync subscriber that is durable.
	dname := "derek"
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectConsumers(t, 1)

	// Make sure we registered as a durable.
	info, _ := sub.ConsumerInfo()
	if info.Config.Durable != dname {
		t.Fatalf("Expected durable name to be set to %q, got %q", dname, info.Config.Durable)
	}
	deliver := info.Config.DeliverSubject

	// Drain subscription, this will delete the consumer.
	go func() {
		time.Sleep(250 * time.Millisecond)
		for {
			if _, err := sub.NextMsg(500 * time.Millisecond); err != nil {
				return
			}
		}
	}()
	sub.Drain()
	nc.Flush()
	expectConsumers(t, 0)

	// This will recreate a new instance.
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info, err := sub.ConsumerInfo(); err != nil || info.Config.DeliverSubject == deliver {
		t.Fatal("Expected delivery subject to be different")
	}
	expectConsumers(t, 1)

	// Subscribing again with same subject and durable name is an error.
	if _, err := js.SubscribeSync("foo", nats.Durable(dname)); err == nil {
		t.Fatal("Unexpected success")
	}
	expectConsumers(t, 1)

	// Delete the durable.
	sub.Unsubscribe()
	expectConsumers(t, 0)

	// Create again and make sure that works.
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectConsumers(t, 1)

	if deliver == sub.Subject {
		t.Fatalf("Expected delivery subject to be different then %q", deliver)
	}
	sub.Unsubscribe()
	expectConsumers(t, 0)

	// Create a queue group on "bar" with no explicit durable name, which
	// means that the queue name will be used as the durable name.
	sub1, err = js.QueueSubscribeSync("bar", "v0")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub1.Unsubscribe()
	waitForPending(t, sub1, 10)
	expectConsumers(t, 1)

	// Since the above JS consumer is created on subject "bar", trying to
	// add a member to the same group but on subject "baz" should fail.
	if _, err = js.QueueSubscribeSync("baz", "v0"); err == nil {
		t.Fatal("Unexpected success")
	}

	// If the queue group is different, but we try to attach to the existing
	// JS consumer that is created for group "v0", then this should fail.
	if _, err = js.QueueSubscribeSync("bar", "v1", nats.Durable("v0")); err == nil {
		t.Fatal("Unexpected success")
	}
	// However, if a durable name is specified, creating a queue sub with
	// the same queue name is ok, but will feed from a different JS consumer.
	sub2, err = js.QueueSubscribeSync("bar", "v0", nats.Durable("otherQueueDurable"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub2.Unsubscribe()
	waitForPending(t, sub2, 10)
	expectConsumers(t, 2)

	sub1.Unsubscribe()
	sub2.Unsubscribe()
	expectConsumers(t, 0)

	// Now try pull based subscribers.

	// Check some error conditions first.
	if _, err := js.Subscribe("bar", nil); err != nats.ErrBadSubscription {
		t.Fatalf("Expected an error trying to create subscriber with nil callback, got %v", err)
	}

	// Since v2.7.0, we can create pull consumers with ephemerals.
	sub, err = js.PullSubscribe("bar", "")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	sub.Unsubscribe()

	// Pull consumer with AckNone policy
	sub, err = js.PullSubscribe("bar", "", nats.AckNone())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	sub.Unsubscribe()

	// Can't specify DeliverSubject for pull subscribers
	_, err = js.PullSubscribe("bar", "foo", nats.DeliverSubject("baz"))
	if err != nats.ErrPullSubscribeToPushConsumer {
		t.Fatalf("Unexpected error: %v", err)
	}
	// If stream name is not specified, need the subject.
	_, err = js.PullSubscribe("", "rip")
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("Unexpected error: %v", err)
	}
	// If stream provided, it should be ok.
	sub, err = js.PullSubscribe("", "rip", nats.BindStream("TEST"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub.Unsubscribe()

	batch := 5
	sub, err = js.PullSubscribe("bar", "rip")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectConsumers(t, 1)

	// The first batch if available should be delivered and queued up.
	bmsgs, err := sub.Fetch(batch)
	if err != nil {
		t.Fatal(err)
	}

	if info, _ := sub.ConsumerInfo(); info.NumAckPending != batch || info.NumPending != uint64(batch) {
		t.Fatalf("Expected %d pending ack, and %d still waiting to be delivered, got %d and %d", batch, batch, info.NumAckPending, info.NumPending)
	}

	// Now go ahead and consume these and ack, but not ack+next.
	for i := 0; i < batch; i++ {
		m := bmsgs[i]
		err = m.AckSync()
		if err != nil {
			t.Fatal(err)
		}
	}
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if info, _ := sub.ConsumerInfo(); info.AckFloor.Consumer != uint64(batch) {
			return fmt.Errorf("Expected ack floor to be %d, got %d", batch, info.AckFloor.Consumer)
		}
		return nil
	})
	waitForPending(t, sub, 0)

	// Make a request for 10 but should only receive a few.
	bmsgs, err = sub.Fetch(10, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	got := len(bmsgs)
	expected := 5
	if got != expected {
		t.Errorf("Expected: %v, got: %v", expected, got)
	}

	for _, msg := range bmsgs {
		msg.AckSync()
	}

	// Now test attaching to a pull based durable.

	// Test that if we are attaching that the subjects will match up. rip from
	// above was created with a filtered subject of bar, so this should fail.
	_, err = js.PullSubscribe("baz", "rip")
	if err != nats.ErrSubjectMismatch {
		t.Fatalf("Expected a %q error but got %q", nats.ErrSubjectMismatch, err)
	}

	// Queue up 10 more messages.
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}

	sub, err = js.PullSubscribe("bar", "rip")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	// No new JS consumer was created.
	expectConsumers(t, 1)

	// Fetch messages a couple of times.
	expected = 5
	bmsgs, err = sub.Fetch(expected, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	got = len(bmsgs)
	if got != expected {
		t.Errorf("Expected: %v, got: %v", expected, got)
	}

	info, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.NumAckPending != batch || info.NumPending != uint64(toSend-batch) {
		t.Fatalf("Expected ack pending of %d and pending to be %d, got %d %d", batch, toSend-batch, info.NumAckPending, info.NumPending)
	}

	// Pull subscriptions can't use NextMsg variants.
	if _, err := sub.NextMsg(time.Second); err != nats.ErrTypeSubscription {
		t.Fatalf("Expected error %q, got %v", nats.ErrTypeSubscription, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := sub.NextMsgWithContext(ctx); err != nats.ErrTypeSubscription {
		t.Fatalf("Expected error %q, got %v", nats.ErrTypeSubscription, err)
	}
	cancel()

	// Prevent invalid durable names
	if _, err := js.SubscribeSync("baz", nats.Durable("test.durable")); err != nats.ErrInvalidConsumerName {
		t.Fatalf("Expected invalid durable name error")
	}

	ackWait := 1 * time.Millisecond
	sub, err = js.SubscribeSync("bar", nats.Durable("ack-wait"), nats.AckWait(ackWait))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectConsumers(t, 2)

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	info, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Config.AckWait != ackWait {
		t.Errorf("Expected %v, got %v", ackWait, info.Config.AckWait)
	}

	// Add Stream and Consumer name to metadata.
	sub, err = js.SubscribeSync("bar", nats.Durable("consumer-name"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectConsumers(t, 3)
	m, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	meta, err := m.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if meta.Stream != "TEST" {
		t.Fatalf("Unexpected stream name, got: %v", meta.Stream)
	}
	if meta.Consumer != "consumer-name" {
		t.Fatalf("Unexpected consumer name, got: %v", meta.Consumer)
	}

	qsubDurable := nats.Durable("qdur-chan")
	mch := make(chan *nats.Msg, 16536)
	sub, err = js.ChanQueueSubscribe("bar", "v1", mch, qsubDurable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectConsumers(t, 4)

	var a, b *nats.MsgMetadata
	select {
	case msg := <-mch:
		meta, err := msg.Metadata()
		if err != nil {
			t.Error(err)
		}
		a = meta
	case <-time.After(2 * time.Second):
		t.Errorf("Timeout waiting for message")
	}

	mch2 := make(chan *nats.Msg, 16536)
	sub, err = js.ChanQueueSubscribe("bar", "v1", mch2, qsubDurable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	// Not a new JS consumer
	expectConsumers(t, 4)

	// Publish more messages so that at least one is received by
	// the channel queue subscriber.
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}

	select {
	case msg := <-mch2:
		meta, err := msg.Metadata()
		if err != nil {
			t.Error(err)
		}
		b = meta
	case <-time.After(2 * time.Second):
		t.Errorf("Timeout waiting for message")
	}
	if reflect.DeepEqual(a, b) {
		t.Errorf("Expected to receive different messages in stream")
	}

	// Both ChanQueueSubscribers use the same consumer.
	expectConsumers(t, 4)

	sub, err = js.SubscribeSync("foo", nats.InactiveThreshold(-100*time.Millisecond))
	if err == nil || !strings.Contains(err.Error(), "invalid InactiveThreshold") {
		t.Fatalf("Expected error about invalid option, got %v", err)
	}

	// Create an ephemeral with a lower inactive threshold
	sub, err = js.SubscribeSync("foo", nats.InactiveThreshold(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Error on consumer info: %v", err)
	}
	name := ci.Name
	nc.Close()

	time.Sleep(150 * time.Millisecond)

	nc, js = jsClient(t, s)
	defer nc.Close()

	if ci, err := js.ConsumerInfo("TEST", name); err == nil {
		t.Fatalf("Expected no consumer to exist, got %+v", ci)
	}
}

func TestJetStreamSubscribe_SkipConsumerLookup(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:           "cons",
		DeliverSubject: "_INBOX.foo",
		AckPolicy:      nats.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// for checking whether subscribe looks up the consumer
	infoSub, err := nc.SubscribeSync("$JS.API.CONSUMER.INFO.TEST.*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer infoSub.Unsubscribe()

	// for checking whether subscribe creates the consumer
	createConsSub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer createConsSub.Unsubscribe()
	t.Run("use Bind to skip consumer lookup and create", func(t *testing.T) {
		sub, err := js.SubscribeSync("", nats.Bind("TEST", "cons"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()
		// we should get timeout waiting for msg on CONSUMER.INFO
		if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
			t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
		}

		// we should get timeout waiting for msg on CONSUMER.CREATE
		if msg, err := createConsSub.NextMsg(50 * time.Millisecond); err == nil {
			t.Fatalf("Expected to skip consumer create; got message on %q", msg.Subject)
		}
		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
			t.Fatalf("Expected to receive msg; got: %s", err)
		}
	})
	t.Run("use Durable, skip consumer lookup but overwrite the consumer", func(t *testing.T) {
		sub, err := js.SubscribeSync("foo", nats.Durable("cons"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// we should get timeout waiting for msg on CONSUMER.INFO
		if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
			t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
		}

		// we should get msg on CONSUMER.CREATE
		if _, err := createConsSub.NextMsg(50 * time.Millisecond); err != nil {
			t.Fatalf("Expected consumer create; got: %s", err)
		}
		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
			t.Fatalf("Expected to receive msg; got: %s", err)
		}
	})
	t.Run("create new consumer with Durable, skip lookup", func(t *testing.T) {
		sub, err := js.SubscribeSync("foo", nats.Durable("pp"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo1"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()
		// we should get timeout waiting for msg on CONSUMER.INFO
		if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
			t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
		}

		// we should get msg on CONSUMER.CREATE
		if _, err := createConsSub.NextMsg(50 * time.Millisecond); err != nil {
			t.Fatalf("Expected consumer create; got: %s", err)
		}
		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
			t.Fatalf("Expected to receive msg; got: %s", err)
		}
	})
	t.Run("create new consumer with ConsumerName, skip lookup", func(t *testing.T) {
		sub, err := js.SubscribeSync("foo", nats.ConsumerName("pp"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo1"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()
		// we should get timeout waiting for msg on CONSUMER.INFO
		if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
			t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
		}

		// we should get msg on CONSUMER.CREATE
		if _, err := createConsSub.NextMsg(50 * time.Millisecond); err != nil {
			t.Fatalf("Expected consumer create; got: %s", err)
		}
		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
			t.Fatalf("Expected to receive msg; got: %s", err)
		}
	})

	t.Run("create ephemeral consumer, SkipConsumerLookup has no effect", func(t *testing.T) {
		sub, err := js.SubscribeSync("foo", nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo2"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()
		// we should get timeout waiting for msg on CONSUMER.INFO
		if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
			t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
		}

		// we should get msg on CONSUMER.CREATE
		if _, err := createConsSub.NextMsg(50 * time.Millisecond); err != nil {
			t.Fatalf("Expected consumer create; got: %s", err)
		}
		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
			t.Fatalf("Expected to receive msg; got: %s", err)
		}
	})
	t.Run("attempt to update ack policy of existing consumer", func(t *testing.T) {
		_, err := js.SubscribeSync("foo", nats.Durable("cons"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo"), nats.AckAll())
		if err == nil || !strings.Contains(err.Error(), "ack policy can not be updated") {
			t.Fatalf("Expected update consumer error, got: %v", err)
		}
	})
}

func TestPullSubscribeFetchWithHeartbeat(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.PullSubscribe("foo", "")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	defer sub.Unsubscribe()
	for i := 0; i < 5; i++ {
		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
	}

	// fetch 5 messages, should finish immediately
	msgs, err := sub.Fetch(5, nats.PullHeartbeat(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
	}
	now := time.Now()
	// no messages available, should time out normally
	_, err = sub.Fetch(5, nats.PullHeartbeat(50*time.Millisecond), nats.MaxWait(300*time.Millisecond))
	elapsed := time.Since(now)
	if elapsed < 300*time.Millisecond {
		t.Fatalf("Expected timeout after 300ms; got: %v", elapsed)
	}
	if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("Expected timeout error; got: %v", err)
	}

	// delete consumer to verify heartbeats are not sent anymore
	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteConsumer("TEST", info.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = sub.Fetch(5, nats.PullHeartbeat(100*time.Millisecond), nats.MaxWait(1*time.Second))
	if !errors.Is(err, nats.ErrNoHeartbeat) {
		t.Fatalf("Expected no heartbeat error; got: %v", err)
	}

	// heartbeat value too large
	_, err = sub.Fetch(5, nats.PullHeartbeat(200*time.Millisecond), nats.MaxWait(300*time.Millisecond))
	if !errors.Is(err, nats.ErrInvalidArg) {
		t.Fatalf("Expected invalid arg error; got: %v", err)
	}

	// heartbeat value invalid
	_, err = sub.Fetch(5, nats.PullHeartbeat(-1))
	if !errors.Is(err, nats.ErrInvalidArg) {
		t.Fatalf("Expected invalid arg error; got: %v", err)
	}

	// set short timeout on JetStream context
	js, err = nc.JetStream(nats.MaxWait(100 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub1, err := js.PullSubscribe("foo", "")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	defer sub.Unsubscribe()

	// should produce invalid arg error based on default timeout from JetStream context
	_, err = sub1.Fetch(5, nats.PullHeartbeat(100*time.Millisecond))
	if !errors.Is(err, nats.ErrInvalidArg) {
		t.Fatalf("Expected invalid arg error; got: %v", err)
	}

	// overwrite default timeout with context timeout, fetch available messages
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	msgs, err = sub1.Fetch(10, nats.PullHeartbeat(100*time.Millisecond), nats.Context(ctx))
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
	}
	for _, msg := range msgs {
		msg.Ack()
	}

	// overwrite default timeout with max wait, should time out because no messages are available
	_, err = sub1.Fetch(5, nats.PullHeartbeat(100*time.Millisecond), nats.MaxWait(300*time.Millisecond))
	if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("Expected timeout error; got: %v", err)
	}
}

func TestPullSubscribeFetchDrain(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	defer js.PurgeStream("TEST")
	sub, err := js.PullSubscribe("foo", "")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	for i := 0; i < 100; i++ {
		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
	}
	// fill buffer with messages
	cinfo, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	nextSubject := fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.TEST.%s", cinfo.Name)
	replySubject := strings.Replace(sub.Subject, "*", "abc", 1)
	payload := `{"batch":10,"no_wait":true}`
	if err := nc.PublishRequest(nextSubject, replySubject, []byte(payload)); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	time.Sleep(100 * time.Millisecond)

	// now drain the subscription, messages should be in the buffer
	sub.Drain()
	msgs, err := sub.Fetch(100)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	for _, msg := range msgs {
		msg.Ack()
	}
	if len(msgs) != 10 {
		t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
	}

	// subsequent fetch should return error, subscription is already drained
	_, err = sub.Fetch(10, nats.MaxWait(100*time.Millisecond))
	if !errors.Is(err, nats.ErrSubscriptionClosed) {
		t.Fatalf("Expected error: %s; got: %s", nats.ErrSubscriptionClosed, err)
	}
}

func TestPullSubscribeFetchBatchWithHeartbeat(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.PullSubscribe("foo", "")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	defer sub.Unsubscribe()
	for i := 0; i < 5; i++ {
		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
	}

	// fetch 5 messages, should finish immediately
	msgs, err := sub.FetchBatch(5, nats.PullHeartbeat(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	var i int
	for msg := range msgs.Messages() {
		i++
		msg.Ack()
	}
	if i != 5 {
		t.Fatalf("Expected %d messages; got: %d", 5, i)
	}
	if msgs.Error() != nil {
		t.Fatalf("Unexpected error: %s", msgs.Error())
	}
	now := time.Now()
	// no messages available, should time out normally
	msgs, err = sub.FetchBatch(5, nats.PullHeartbeat(50*time.Millisecond), nats.MaxWait(300*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	i = 0
	for msg := range msgs.Messages() {
		i++
		msg.Ack()
	}
	elapsed := time.Since(now)
	if i != 0 {
		t.Fatalf("Expected %d messages; got: %d", 0, i)
	}
	if msgs.Error() != nil {
		t.Fatalf("Unexpected error: %s", msgs.Error())
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("Expected timeout after 300ms; got: %v", elapsed)
	}

	// delete consumer to verify heartbeats are not sent anymore
	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteConsumer("TEST", info.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	now = time.Now()
	msgs, err = sub.FetchBatch(5, nats.PullHeartbeat(100*time.Millisecond), nats.MaxWait(1*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for msg := range msgs.Messages() {
		msg.Ack()
	}
	elapsed = time.Since(now)
	if elapsed < 200*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Fatalf("Expected timeout after 200ms and before 300ms; got: %v", elapsed)
	}
	if !errors.Is(msgs.Error(), nats.ErrNoHeartbeat) {
		t.Fatalf("Expected no heartbeat error; got: %v", err)
	}

	// heartbeat value too large
	_, err = sub.FetchBatch(5, nats.PullHeartbeat(200*time.Millisecond), nats.MaxWait(300*time.Millisecond))
	if !errors.Is(err, nats.ErrInvalidArg) {
		t.Fatalf("Expected no heartbeat error; got: %v", err)
	}

	// heartbeat value invalid
	_, err = sub.FetchBatch(5, nats.PullHeartbeat(-1))
	if !errors.Is(err, nats.ErrInvalidArg) {
		t.Fatalf("Expected no heartbeat error; got: %v", err)
	}

	// set short timeout on JetStream context
	js, err = nc.JetStream(nats.MaxWait(100 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub1, err := js.PullSubscribe("foo", "")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	defer sub.Unsubscribe()

	// should produce invalid arg error based on default timeout from JetStream context
	_, err = sub1.Fetch(5, nats.PullHeartbeat(100*time.Millisecond))
	if !errors.Is(err, nats.ErrInvalidArg) {
		t.Fatalf("Expected invalid arg error; got: %v", err)
	}

	// overwrite default timeout with context timeout, fetch available messages
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	msgs, err = sub1.FetchBatch(10, nats.PullHeartbeat(100*time.Millisecond), nats.Context(ctx))
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	for msg := range msgs.Messages() {
		msg.Ack()
	}

	// overwrite default timeout with max wait, should time out because no messages are available
	msgs, err = sub1.FetchBatch(5, nats.PullHeartbeat(100*time.Millisecond), nats.MaxWait(300*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	<-msgs.Done()
	if msgs.Error() != nil {
		t.Fatalf("Unexpected error: %s", msgs.Error())
	}
}

func TestPullSubscribeFetchBatch(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	t.Run("basic fetch", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 5; i++ {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		res, err := sub.FetchBatch(10)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		go func() {
			time.Sleep(10 * time.Millisecond)
			for i := 0; i < 5; i++ {
				js.Publish("foo", []byte("msg"))
			}
		}()
		msgs := make([]*nats.Msg, 0)
		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", res.Error())
		}
		if len(msgs) != 10 {
			t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
		}
	})

	t.Run("multiple concurrent fetches", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 50; i++ {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		var r1, r2, r3 nats.MessageBatch
		started := &sync.WaitGroup{}
		started.Add(3)
		errs := make(chan error, 3)
		go func() {
			var err error
			r1, err = sub.FetchBatch(10)
			if err != nil {
				errs <- err
			}
			started.Done()
		}()
		go func() {
			var err error
			r2, err = sub.FetchBatch(10)
			if err != nil {
				errs <- err
			}
			started.Done()
		}()
		go func() {
			var err error
			r3, err = sub.FetchBatch(10)
			if err != nil {
				errs <- err
			}
			started.Done()
		}()
		// wait until batch results are available
		started.Wait()

		// check if any FetchBatch call returned an error
		select {
		case err := <-errs:
			t.Fatalf("Error initializing fetch: %s", err)
		default:
		}

		var msgsReceived int
		for msgsReceived < 30 {
			select {
			case <-r1.Messages():
				msgsReceived++
			case <-r2.Messages():
				msgsReceived++
			case <-r3.Messages():
				msgsReceived++
			case <-time.After(1 * time.Second):
				t.Fatalf("Timeout waiting for incoming messages")
			}
		}
		select {
		case <-r1.Done():
		case <-time.After(1 * time.Second):
			t.Fatalf("FetchBatch result channel should be closed after receiving all messages on r1")
		}
		select {
		case <-r2.Done():
		case <-time.After(1 * time.Second):
			t.Fatalf("FetchBatch result channel should be closed after receiving all messages on r2")
		}
		select {
		case <-r3.Done():
		case <-time.After(1 * time.Second):
			t.Fatalf("FetchBatch result channel should be closed after receiving all messages on r3")
		}
		if r1.Error() != nil {
			t.Fatalf("Unexpected error: %s", r1.Error())
		}
		if r2.Error() != nil {
			t.Fatalf("Unexpected error: %s", r2.Error())
		}
		if r3.Error() != nil {
			t.Fatalf("Unexpected error: %s", r3.Error())
		}
		if msgsReceived != 30 {
			t.Fatalf("Expected %d messages; got: %d", 30, msgsReceived)
		}
	})

	t.Run("deliver all, then consume", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 5; i++ {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		res, err := sub.FetchBatch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		time.Sleep(10 * time.Millisecond)
		msgs := make([]*nats.Msg, 0)
		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", res.Error())
		}
		if len(msgs) != 5 {
			t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
		}
	})

	t.Run("fetch with context", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 5; i++ {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		res, err := sub.FetchBatch(10, nats.Context(ctx))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		go func() {
			time.Sleep(10 * time.Millisecond)
			for i := 0; i < 5; i++ {
				js.Publish("foo", []byte("msg"))
			}
		}()
		msgs := make([]*nats.Msg, 0)
		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", res.Error())
		}
		if len(msgs) != 10 {
			t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
		}
	})

	t.Run("fetch subset of messages", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 10; i++ {
			js.Publish("foo", []byte("msg"))
		}
		res, err := sub.FetchBatch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		msgs := make([]*nats.Msg, 0)
		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", res.Error())
		}
		if len(msgs) != 5 {
			t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
		}
	})

	t.Run("context timeout, no error", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 5; i++ {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		res, err := sub.FetchBatch(10, nats.Context(ctx))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		msgs := make([]*nats.Msg, 0)
		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", res.Error())
		}
		if len(msgs) != 5 {
			t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
		}
	})

	t.Run("request expired", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 5; i++ {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		res, err := sub.FetchBatch(10, nats.MaxWait(50*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		msgs := make([]*nats.Msg, 0)
		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", res.Error())
		}
		if len(msgs) != 5 {
			t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
		}
	})

	t.Run("cancel context during fetch", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 5; i++ {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		res, err := sub.FetchBatch(10, nats.Context(ctx))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()
		msgs := make([]*nats.Msg, 0)
		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() == nil || !errors.Is(res.Error(), context.Canceled) {
			t.Fatalf("Expected error: %s; got: %s", nats.ErrConsumerDeleted, res.Error())
		}
		if len(msgs) != 5 {
			t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
		}
	})

	t.Run("remove durable consumer during fetch", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "cons")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 5; i++ {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		res, err := sub.FetchBatch(10)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		go func() {
			time.Sleep(10 * time.Millisecond)
			js.DeleteConsumer("TEST", "cons")
		}()
		msgs := make([]*nats.Msg, 0)
		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() == nil || !errors.Is(res.Error(), nats.ErrConsumerDeleted) {
			t.Fatalf("Expected error: %s; got: %s", nats.ErrConsumerDeleted, err)
		}
		if len(msgs) != 5 {
			t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
		}
	})

	t.Run("validation errors", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		// negative batch size
		_, err = sub.FetchBatch(-1)
		if !errors.Is(err, nats.ErrInvalidArg) {
			t.Errorf("Expected error: %s; got: %s", nats.ErrInvalidArg, err)
		}

		syncSub, err := js.SubscribeSync("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		// invalid subscription type
		_, err = syncSub.FetchBatch(10)
		if !errors.Is(err, nats.ErrTypeSubscription) {
			t.Errorf("Expected error: %s; got: %s", nats.ErrTypeSubscription, err)
		}

		// both context and max wait set
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_, err = sub.FetchBatch(10, nats.Context(ctx), nats.MaxWait(2*time.Second))
		if !errors.Is(err, nats.ErrContextAndTimeout) {
			t.Errorf("Expected error: %s; got: %s", nats.ErrContextAndTimeout, err)
		}

		// passing context.Background() to fetch
		_, err = sub.FetchBatch(10, nats.Context(context.Background()))
		if !errors.Is(err, nats.ErrNoDeadlineContext) {
			t.Errorf("Expected error: %s; got: %s", nats.ErrNoDeadlineContext, err)
		}
	})

	t.Run("close subscription", func(t *testing.T) {
		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for i := 0; i < 100; i++ {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		// fill buffer with messages
		cinfo, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		nextSubject := fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.TEST.%s", cinfo.Name)
		replySubject := strings.Replace(sub.Subject, "*", "abc", 1)
		payload := `{"batch":10,"no_wait":true}`
		if err := nc.PublishRequest(nextSubject, replySubject, []byte(payload)); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		time.Sleep(100 * time.Millisecond)

		// now drain the subscription, messages should be in the buffer
		sub.Drain()
		res, err := sub.FetchBatch(100)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		msgs := make([]*nats.Msg, 0)
		for msg := range res.Messages() {
			msgs = append(msgs, msg)
			msg.Ack()
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", res.Error())
		}
		if len(msgs) != 10 {
			t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
		}

		// subsequent fetch should return error, subscription is already drained
		_, err = sub.FetchBatch(10, nats.MaxWait(100*time.Millisecond))
		if !errors.Is(err, nats.ErrSubscriptionClosed) {
			t.Fatalf("Expected error: %s; got: %s", nats.ErrSubscriptionClosed, err)
		}
	})
}

func TestPullSubscribeConsumerDeleted(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.Publish("foo", []byte("msg")); err != nil {
		t.Fatal(err)
	}
	t.Run("delete consumer", func(t *testing.T) {
		sub, err := js.PullSubscribe("foo", "cons")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if err != nil {
			t.Fatal(err)
		}

		if _, err = sub.Fetch(1, nats.MaxWait(10*time.Millisecond)); err != nil {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrTimeout, err)
		}
		time.AfterFunc(50*time.Millisecond, func() { js.DeleteConsumer("TEST", "cons") })
		if _, err = sub.Fetch(1, nats.MaxWait(100*time.Millisecond)); !errors.Is(err, nats.ErrConsumerDeleted) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerDeleted, err)
		}
	})

	t.Run("delete stream", func(t *testing.T) {
		sub, err := js.PullSubscribe("foo", "cons")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if err != nil {
			t.Fatal(err)
		}

		if _, err = sub.Fetch(1, nats.MaxWait(10*time.Millisecond)); err != nil {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrTimeout, err)
		}
		time.AfterFunc(50*time.Millisecond, func() { js.DeleteStream("TEST") })
		if _, err = sub.Fetch(1, nats.MaxWait(100*time.Millisecond)); !errors.Is(err, nats.ErrConsumerDeleted) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerDeleted, err)
		}
	})

}

func TestJetStreamAckPending_Pull(t *testing.T) {
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

	const totalMsgs = 4
	for i := 0; i < totalMsgs; i++ {
		if _, err := js.Publish("foo", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	ackPendingLimit := 3
	sub, err := js.PullSubscribe("foo", "dname-pull-ack-wait", nats.MaxAckPending(ackPendingLimit))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	var msgs []*nats.Msg
	for i := 0; i < ackPendingLimit; i++ {
		ms, err := sub.Fetch(1)
		if err != nil {
			t.Fatalf("Error on fetch: %v", err)
		}
		msgs = append(msgs, ms...)
	}

	// Since we don't ack, the next fetch should time out because the server
	// won't send new ones until we ack some.
	if _, err := sub.Fetch(1, nats.MaxWait(250*time.Millisecond)); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout, got: %v", err)
	}
	// Ack one message, then we should be able to get the next
	msgs[0].Ack()
	if _, err := sub.Fetch(1); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamAckPending_Push(t *testing.T) {
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

	const totalMsgs = 3
	for i := 0; i < totalMsgs; i++ {
		if _, err := js.Publish("foo", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	sub, err := js.SubscribeSync("foo",
		nats.Durable("dname-wait"),
		nats.AckWait(100*time.Millisecond),
		nats.MaxDeliver(5),
		nats.MaxAckPending(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 3 messages delivered 5 times.
	expected := 15
	timeout := time.Now().Add(2 * time.Second)
	pending := 0
	for time.Now().Before(timeout) {
		if pending, _, _ = sub.Pending(); pending >= expected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pending < expected {
		t.Errorf("Expected %v, got %v", expected, pending)
	}

	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}

	got := info.NumRedelivered
	expected = 3
	if got < expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = info.NumAckPending
	expected = 3
	if got < expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = info.NumWaiting
	expected = 0
	if got != expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = int(info.NumPending)
	expected = 0
	if got != expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = info.Config.MaxAckPending
	expected = 3
	if got != expected {
		t.Errorf("Expected %v, got %v", expected, pending)
	}

	got = info.Config.MaxDeliver
	expected = 5
	if got != expected {
		t.Errorf("Expected %v, got %v", expected, pending)
	}

	acks := map[int]int{}

	ackPending := 3
	timeout = time.Now().Add(2 * time.Second)
	for time.Now().Before(timeout) {
		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := info.NumAckPending, ackPending; got > 0 && got != want {
			t.Fatalf("unexpected num ack pending: got=%d, want=%d", got, want)
		}

		// Continue to ack all messages until no more pending.
		pending, _, _ = sub.Pending()
		if pending == 0 {
			break
		}

		m, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("Error getting next message: %v", err)
		}

		if err := m.AckSync(); err != nil {
			t.Fatalf("Error on ack message: %v", err)
		}

		meta, err := m.Metadata()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		acks[int(meta.Sequence.Stream)]++

		if ackPending != 0 {
			ackPending--
		}
		if int(meta.NumPending) != ackPending {
			t.Errorf("Expected %v, got %v", ackPending, meta.NumPending)
		}
	}

	got = len(acks)
	expected = 3
	if got != expected {
		t.Errorf("Expected %v, got %v", expected, got)
	}

	expected = 5
	for _, got := range acks {
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
	}

	_, err = sub.NextMsg(100 * time.Millisecond)
	if err != nats.ErrTimeout {
		t.Errorf("Expected timeout, got: %v", err)
	}
}

func TestJetStream_Drain(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)

	nc, err := nats.Connect(s.ClientURL(), nats.ClosedHandler(func(_ *nats.Conn) {
		done()
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"drain"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	total := 500
	for i := 0; i < total; i++ {
		_, err := js.Publish("drain", []byte(fmt.Sprintf("i:%d", i)))
		if err != nil {
			t.Error(err)
		}
	}

	// Create some consumers and ensure that there are no timeouts.
	errCh := make(chan error, 2048)
	createSub := func(name string) (*nats.Subscription, error) {
		return js.Subscribe("drain", func(m *nats.Msg) {
			err := m.AckSync()
			if err != nil {
				errCh <- err
			}
		}, nats.Durable(name), nats.ManualAck())
	}

	subA, err := createSub("A")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	subB, err := createSub("B")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	subC, err := createSub("C")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	subD, err := createSub("D")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	waitForDelivered := func(t *testing.T, sub *nats.Subscription) {
		t.Helper()
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _ := sub.Delivered(); msgs != 0 {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitForDelivered(t, subA)
	waitForDelivered(t, subB)
	waitForDelivered(t, subC)
	waitForDelivered(t, subD)
	nc.Drain()

	select {
	case err := <-errCh:
		t.Fatalf("Error during drain: %+v", err)
	case <-ctx.Done():
		// OK!
	}
}

func TestAckForNonJetStream(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	nc.PublishRequest("foo", "_INBOX_", []byte("OK"))
	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := m.Ack(); err != nil {
		t.Fatalf("Expected no errors, got '%v'", err)
	}
}

func TestJetStreamManagement(t *testing.T) {
	conf := createConfFile(t, []byte(`
                listen: 127.0.0.1:-1
                jetstream: enabled
                accounts: {
                  A {
                    users: [{ user: "foo" }]
                    jetstream: { max_mem: 64MB, max_file: 64MB }
                  }
                }
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s, nats.UserInfo("foo", ""))
	defer nc.Close()

	// Create the stream using our client API.
	var si *nats.StreamInfo

	t.Run("create stream", func(t *testing.T) {
		consLimits := nats.StreamConsumerLimits{
			MaxAckPending:     100,
			InactiveThreshold: 10 * time.Second,
		}
		cfg := &nats.StreamConfig{
			Name:        "foo",
			Subjects:    []string{"foo", "bar", "baz"},
			Compression: nats.S2Compression,
			ConsumerLimits: nats.StreamConsumerLimits{
				MaxAckPending:     100,
				InactiveThreshold: 10 * time.Second,
			},
			FirstSeq: 22,
			Metadata: map[string]string{
				"foo": "bar",
				"baz": "quux",
			},
		}

		si, err := js.AddStream(cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si == nil || si.Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", si)
		}
		if v1, v2 := si.Config.Metadata["foo"], si.Config.Metadata["baz"]; v1 != "bar" || v2 != "quux" {
			t.Fatalf("Metadata is not correct %+v", si.Config.Metadata)
		}
		if si.Config.Compression != nats.S2Compression {
			t.Fatalf("Compression is not correct %+v", si.Config.Compression)
		}
		if si.Config.FirstSeq != 22 {
			t.Fatalf("FirstSeq is not correct %+v", si.Config.FirstSeq)
		}
		if si.Config.ConsumerLimits != consLimits {
			t.Fatalf("ConsumerLimits is not correct %+v", si.Config.ConsumerLimits)
		}
	})

	t.Run("stream with given name already exists", func(t *testing.T) {
		if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Description: "desc"}); !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrStreamNameAlreadyInUse, err)
		}
	})

	for i := 0; i < 25; i++ {
		js.Publish("foo", []byte("hi"))
	}

	var err error
	t.Run("stream not found", func(t *testing.T) {
		si, err = js.StreamInfo("bar")
		if !errors.Is(err, nats.ErrStreamNotFound) {
			t.Fatalf("Expected error: %v, got: %v", nats.ErrStreamNotFound, err)
		}
		if si != nil {
			t.Fatalf("StreamInfo should be nil %+v", si)
		}
	})

	t.Run("stream info", func(t *testing.T) {
		si, err = js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si == nil || si.Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", si)
		}
	})

	t.Run("create bad stream", func(t *testing.T) {
		if _, err := js.AddStream(nil); err != nats.ErrStreamConfigRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamConfigRequired, err)
		}
		if _, err := js.AddStream(&nats.StreamConfig{Name: ""}); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if _, err := js.AddStream(&nats.StreamConfig{Name: "bad.stream.name"}); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		if _, err := js.AddStream(&nats.StreamConfig{Name: "bad stream name"}); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
	})

	t.Run("bad stream info", func(t *testing.T) {
		if _, err := js.StreamInfo(""); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if _, err := js.StreamInfo("bad.stream.name"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
	})

	t.Run("stream update", func(t *testing.T) {
		if _, err := js.UpdateStream(nil); err != nats.ErrStreamConfigRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamConfigRequired, err)
		}
		if _, err := js.UpdateStream(&nats.StreamConfig{Name: ""}); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if _, err := js.UpdateStream(&nats.StreamConfig{Name: "bad.stream.name"}); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		prevMaxMsgs := si.Config.MaxMsgs
		si, err = js.UpdateStream(&nats.StreamConfig{Name: "foo", MaxMsgs: prevMaxMsgs + 100})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si == nil || si.Config.Name != "foo" || si.Config.MaxMsgs == prevMaxMsgs {
			t.Fatalf("StreamInfo is not correct %+v", si)
		}
	})

	t.Run("create consumer", func(t *testing.T) {
		t.Run("with durable set", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				Durable:   "dlc",
				AckPolicy: nats.AckExplicitPolicy,
				Metadata: map[string]string{
					"foo": "bar",
					"baz": "quux",
				},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
			if v1, v2 := ci.Config.Metadata["foo"], ci.Config.Metadata["baz"]; v1 != "bar" || v2 != "quux" {
				t.Fatalf("Metadata is not correct %+v", ci.Config.Metadata)
			}
		})
		t.Run("with name set", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-1")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc-1", AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-1"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc-1" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("with same Durable and Name set", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-2")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc-2", Name: "dlc-2", AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-2"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc-2" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("with name and filter subject", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-3.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				Durable:       "dlc-3",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "foo",
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-3"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc-3" || ci.Stream != "foo" || ci.Config.FilterSubject != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("legacy ephemeral consumer without name", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"stream_name":"foo"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Config.Durable != "" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("legacy durable with jetstream context option", func(t *testing.T) {
			jsLegacy, err := nc.JetStream(nats.UseLegacyDurableConsumers())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.DURABLE.CREATE.foo.dlc-4")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := jsLegacy.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc-4", AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-4"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Config.Durable != "dlc-4" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("durable consumer with multiple filter subjects", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-5")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				Durable:        "dlc-5",
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo", "bar"},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-5"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Config.Durable != "dlc-5" || !reflect.DeepEqual(ci.Config.FilterSubjects, []string{"foo", "bar"}) {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("ephemeral consumer with multiple filter subjects", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo", "bar"},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			_, err = sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci == nil || !reflect.DeepEqual(ci.Config.FilterSubjects, []string{"foo", "bar"}) {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("multiple filter subjects errors", func(t *testing.T) {
			// both filter subject and filter subjects provided
			_, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo", "bar"},
				FilterSubject:  "baz",
			})
			if !errors.Is(err, nats.ErrDuplicateFilterSubjects) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrDuplicateFilterSubjects, err)
			}
			// overlapping filter subjects
			_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo.*", "foo.A"},
			})
			if !errors.Is(err, nats.ErrOverlappingFilterSubjects) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrOverlappingFilterSubjects, err)
			}
			// empty filter subject in filter subjects
			_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo", ""},
			})
			if !errors.Is(err, nats.ErrEmptyFilter) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrEmptyFilter, err)
			}
		})

		t.Run("with invalid filter subject", func(t *testing.T) {
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Name: "tc", FilterSubject: ".foo"}); !errors.Is(err, nats.ErrInvalidFilterSubject) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidFilterSubject, err)
			}
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Name: "tc", FilterSubject: "foo."}); !errors.Is(err, nats.ErrInvalidFilterSubject) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidFilterSubject, err)
			}
		})

		t.Run("with invalid consumer name", func(t *testing.T) {
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "test.durable"}); err != nats.ErrInvalidConsumerName {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidConsumerName, err)
			}
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "test durable"}); err != nats.ErrInvalidConsumerName {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidConsumerName, err)
			}
		})

		t.Run("consumer with given name already exists, configs do not match", func(t *testing.T) {
			// configs do not match
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckAllPolicy}); !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerNameAlreadyInUse, err)
			}
		})

		t.Run("consumer with given name already exists, configs are the same", func(t *testing.T) {
			// configs are the same
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}); err != nil {
				t.Fatalf("Expected no error; got: %v", err)
			}
		})

		t.Run("stream does not exist", func(t *testing.T) {
			_, err = js.AddConsumer("missing", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrStreamNotFound {
				t.Fatalf("Expected stream not found error, got: %v", err)
			}
		})

		t.Run("params validation error", func(t *testing.T) {
			_, err = js.AddConsumer("", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrStreamNameRequired {
				t.Fatalf("Expected %v, got: %v", nats.ErrStreamNameRequired, err)
			}
			_, err = js.AddConsumer("bad.stream.name", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrInvalidStreamName {
				t.Fatalf("Expected %v, got: %v", nats.ErrInvalidStreamName, err)
			}
			_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "bad.consumer.name", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrInvalidConsumerName {
				t.Fatalf("Expected %v, got: %v", nats.ErrInvalidConsumerName, err)
			}
		})
	})

	t.Run("consumer info", func(t *testing.T) {
		if _, err := js.ConsumerInfo("", "dlc"); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if _, err := js.ConsumerInfo("bad.stream.name", "dlc"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		if _, err := js.ConsumerInfo("foo", ""); err != nats.ErrConsumerNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrConsumerNameRequired, err)
		}
		if _, err := js.ConsumerInfo("foo", "bad.consumer.name"); err != nats.ErrInvalidConsumerName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidConsumerName, err)
		}
		ci, err := js.ConsumerInfo("foo", "dlc")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci == nil || ci.Config.Durable != "dlc" {
			t.Fatalf("ConsumerInfo is not correct %+v", si)
		}
	})

	t.Run("consumer not found", func(t *testing.T) {
		ci, err := js.ConsumerInfo("foo", "cld")
		if !errors.Is(err, nats.ErrConsumerNotFound) {
			t.Fatalf("Expected error: %v, got: %v", nats.ErrConsumerNotFound, err)
		}
		if ci != nil {
			t.Fatalf("ConsumerInfo should be nil %+v", ci)
		}
	})

	t.Run("list streams", func(t *testing.T) {
		var infos []*nats.StreamInfo
		for info := range js.Streams() {
			infos = append(infos, info)
		}
		if len(infos) != 1 || infos[0].Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", infos)
		}
	})

	t.Run("list consumers", func(t *testing.T) {
		var infos []*nats.ConsumerInfo
		for info := range js.Consumers("") {
			infos = append(infos, info)
		}
		if len(infos) != 0 {
			t.Fatalf("ConsumerInfo is not correct %+v", infos)
		}
		for info := range js.Consumers("bad.stream.name") {
			infos = append(infos, info)
		}
		if len(infos) != 0 {
			t.Fatalf("ConsumerInfo is not correct %+v", infos)
		}
		infos = infos[:0]
		for info := range js.Consumers("foo") {
			infos = append(infos, info)
		}
		if len(infos) != 8 || infos[0].Stream != "foo" {
			t.Fatalf("ConsumerInfo is not correct %+v", infos)
		}
	})

	t.Run("list consumer names", func(t *testing.T) {
		var names []string
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for name := range js.ConsumerNames("foo", nats.Context(ctx)) {
			names = append(names, name)
		}
		if got, want := len(names), 8; got != want {
			t.Fatalf("Unexpected names, got=%d, want=%d", got, want)
		}
	})

	t.Run("delete consumers", func(t *testing.T) {
		if err := js.DeleteConsumer("", "dlc"); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if err := js.DeleteConsumer("bad.stream.name", "dlc"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		if err := js.DeleteConsumer("foo", ""); err != nats.ErrConsumerNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrConsumerNameRequired, err)
		}
		if err := js.DeleteConsumer("foo", "bad.consumer.name"); err != nats.ErrInvalidConsumerName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidConsumerName, err)
		}
		if err := js.DeleteConsumer("foo", "dlc"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("update consumer", func(t *testing.T) {
		ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:        "update_push_consumer",
			DeliverSubject: "bar",
			AckPolicy:      nats.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Currently, server supports these fields:
		// description, ack_wait, max_deliver, sample_freq, max_ack_pending, max_waiting and headers_only
		expected := ci.Config
		expected.Description = "my description"
		expected.AckWait = 2 * time.Second
		expected.MaxDeliver = 1
		expected.SampleFrequency = "30"
		expected.MaxAckPending = 10
		expected.HeadersOnly = true

		// Check that stream name is required
		_, err = js.UpdateConsumer("", &expected)
		if err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected stream name required error, got %v", err)
		}
		// Check that stream name is valid
		_, err = js.UpdateConsumer("bad.stream.name", &expected)
		if err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected stream name required error, got %v", err)
		}
		// Check that consumer name is required
		expected.Durable = ""
		expected.Name = ""
		_, err = js.UpdateConsumer("foo", &expected)
		if err != nats.ErrConsumerNameRequired {
			t.Fatalf("Expected consumer name required error, got %v", err)
		}
		// Check that durable name is valid
		expected.Durable = "bad.consumer.name"
		_, err = js.UpdateConsumer("foo", &expected)
		if err != nats.ErrInvalidConsumerName {
			t.Fatalf("Expected invalid consumer name error, got %v", err)
		}
		expected.Durable = "update_push_consumer"

		// Check that configuration is required
		_, err = js.UpdateConsumer("foo", nil)
		if err != nats.ErrConsumerConfigRequired {
			t.Fatalf("Expected consumer configuration required error, got %v", err)
		}

		// Now check that update works and expected fields have been updated
		ci, err = js.UpdateConsumer("foo", &expected)
		if err != nil {
			t.Fatalf("Error on update: %v", err)
		}
		expected.Name = "update_push_consumer"
		if !reflect.DeepEqual(ci.Config, expected) {
			t.Fatalf("Expected config to be %+v, got %+v", expected, ci.Config)
		}

		// Now check with pull consumer
		ci, err = js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:    "update_pull_consumer",
			AckPolicy:  nats.AckExplicitPolicy,
			MaxWaiting: 1,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Currently, server supports these fields:
		// description, ack_wait, max_deliver, sample_freq, max_ack_pending, max_waiting and headers_only
		expected = ci.Config
		expected.Description = "my description"
		expected.AckWait = 2 * time.Second
		expected.MaxDeliver = 1
		expected.SampleFrequency = "30"
		expected.MaxAckPending = 10
		expected.HeadersOnly = true
		expected.MaxRequestBatch = 10
		expected.MaxRequestExpires = 2 * time.Second
		expected.MaxRequestMaxBytes = 1024

		ci, err = js.UpdateConsumer("foo", &expected)
		if err != nil {
			t.Fatalf("Error on update: %v", err)
		}
		if !reflect.DeepEqual(ci.Config, expected) {
			t.Fatalf("Expected config to be %+v, got %+v", expected, ci.Config)
		}
	})

	t.Run("purge stream", func(t *testing.T) {
		if err := js.PurgeStream(""); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if err := js.PurgeStream("bad.stream.name"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		if err := js.PurgeStream("foo"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si, err := js.StreamInfo("foo"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		} else if si.State.Msgs != 0 {
			t.Fatalf("StreamInfo.Msgs is not correct")
		}
	})

	t.Run("list stream names", func(t *testing.T) {
		var names []string
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for name := range js.StreamNames(nats.Context(ctx)) {
			names = append(names, name)
		}
		if got, want := len(names), 1; got != want {
			t.Fatalf("Unexpected names, got=%d, want=%d", got, want)
		}
	})

	t.Run("delete stream", func(t *testing.T) {
		if err := js.DeleteStream(""); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if err := js.DeleteStream("bad.stream.name"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		if err := js.DeleteStream("foo"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.StreamInfo("foo"); err == nil {
			t.Fatalf("Unexpected success")
		}
	})

	t.Run("fetch account info", func(t *testing.T) {
		info, err := js.AccountInfo()
		if err != nil {
			t.Fatal(err)
		}
		if info.Limits.MaxMemory != 67108864 {
			t.Errorf("Expected to have memory limits, got: %v", info.Limits.MaxMemory)
		}
		if info.Limits.MaxStore != 67108864 {
			t.Errorf("Expected to have disk limits, got: %v", info.Limits.MaxMemory)
		}
		if info.Limits.MaxStreams != -1 {
			t.Errorf("Expected to not have stream limits, got: %v", info.Limits.MaxStreams)
		}
		if info.Limits.MaxConsumers != -1 {
			t.Errorf("Expected to not have consumer limits, got: %v", info.Limits.MaxConsumers)
		}
		if info.API.Total == 0 {
			t.Errorf("Expected some API calls, got: %v", info.API.Total)
		}
		if info.API.Errors == 0 {
			t.Errorf("Expected some API error, got: %v", info.API.Errors)
		}
	})
}

func TestStreamConfigMatches(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, js := jsClient(t, srv)
	defer nc.Close()

	cfg := nats.StreamConfig{
		Name:                 "stream",
		Description:          "desc",
		Subjects:             []string{"foo.*"},
		Retention:            nats.WorkQueuePolicy,
		MaxConsumers:         10,
		MaxMsgs:              100,
		MaxBytes:             1000,
		Discard:              nats.DiscardNew,
		DiscardNewPerSubject: true,
		MaxAge:               100 * time.Second,
		MaxMsgsPerSubject:    1000,
		MaxMsgSize:           10000,
		Storage:              nats.MemoryStorage,
		Replicas:             1,
		NoAck:                true,
		Duplicates:           10 * time.Second,
		Sealed:               false,
		DenyDelete:           true,
		DenyPurge:            false,
		AllowRollup:          true,
		Compression:          nats.S2Compression,
		FirstSeq:             5,
		SubjectTransform:     &nats.SubjectTransformConfig{Source: ">", Destination: "transformed.>"},
		RePublish: &nats.RePublish{
			Source:      ">",
			Destination: "RP.>",
			HeadersOnly: true,
		},
		AllowDirect: true,
		ConsumerLimits: nats.StreamConsumerLimits{
			InactiveThreshold: 10 * time.Second,
			MaxAckPending:     500,
		},
	}

	s, err := js.AddStream(&cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// server will set metadata values, so we need to clear them
	s.Config.Metadata = nil
	if !reflect.DeepEqual(s.Config, cfg) {
		t.Fatalf("StreamConfig doesn't match: %#v", s.Config)
	}

	cfgMirror := nats.StreamConfig{
		Name:              "mirror",
		MaxConsumers:      10,
		MaxMsgs:           100,
		MaxBytes:          1000,
		MaxAge:            100 * time.Second,
		MaxMsgsPerSubject: 1000,
		MaxMsgSize:        10000,
		Replicas:          1,
		Duplicates:        10 * time.Second,
		Mirror: &nats.StreamSource{
			Name:        "stream",
			OptStartSeq: 10,
			SubjectTransforms: []nats.SubjectTransformConfig{
				{Source: ">", Destination: "transformed.>"},
			},
		},
		MirrorDirect:     true,
		SubjectTransform: &nats.SubjectTransformConfig{Source: ">", Destination: "transformed.>"},
	}

	s, err = js.AddStream(&cfgMirror)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// server will set metadata values, so we need to clear them
	s.Config.Metadata = nil
	if !reflect.DeepEqual(s.Config, cfgMirror) {
		t.Fatalf("StreamConfig doesn't match: %#v", s.Config)
	}

	cfgSourcing := nats.StreamConfig{
		Name:              "sourcing",
		Subjects:          []string{"BAR"},
		MaxConsumers:      10,
		MaxMsgs:           100,
		MaxBytes:          1000,
		MaxAge:            100 * time.Second,
		MaxMsgsPerSubject: 1000,
		MaxMsgSize:        10000,
		Replicas:          1,
		Duplicates:        10 * time.Second,
		Sources: []*nats.StreamSource{
			{
				Name:        "stream",
				OptStartSeq: 10,
				SubjectTransforms: []nats.SubjectTransformConfig{
					{Source: ">", Destination: "transformed.>"},
				},
			},
		},
		SubjectTransform: &nats.SubjectTransformConfig{Source: ">", Destination: "transformed.>"},
	}

	s, err = js.AddStream(&cfgSourcing)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// server will set metadata values, so we need to clear them
	s.Config.Metadata = nil
	if !reflect.DeepEqual(s.Config, cfgSourcing) {
		t.Fatalf("StreamConfig doesn't match: %#v", s.Config)
	}
}

func TestStreamLister(t *testing.T) {
	tests := []struct {
		name       string
		streamsNum int
	}{
		{
			name:       "single page",
			streamsNum: 5,
		},
		{
			name:       "multi page",
			streamsNum: 1025,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			nc, js := jsClient(t, s)
			defer nc.Close()
			for i := 0; i < test.streamsNum; i++ {
				if _, err := js.AddStream(&nats.StreamConfig{Name: fmt.Sprintf("stream_%d", i)}); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			names := make([]string, 0)
			for name := range js.StreamNames() {
				names = append(names, name)
			}
			if len(names) != test.streamsNum {
				t.Fatalf("Invalid number of stream names; want: %d; got: %d", test.streamsNum, len(names))
			}
			infos := make([]*nats.StreamInfo, 0)
			for info := range js.Streams() {
				infos = append(infos, info)
			}
			if len(infos) != test.streamsNum {
				t.Fatalf("Invalid number of streams; want: %d; got: %d", test.streamsNum, len(infos))
			}
			// test the deprecated StreamsInfo()
			infos = make([]*nats.StreamInfo, 0)
			for info := range js.StreamsInfo() {
				infos = append(infos, info)
			}
			if len(infos) != test.streamsNum {
				t.Fatalf("Invalid number of streams; want: %d; got: %d", test.streamsNum, len(infos))
			}
		})
	}
}

func TestStreamLister_FilterSubject(t *testing.T) {
	streams := map[string][]string{
		"s1": {"foo"},
		"s2": {"bar"},
		"s3": {"foo.*", "bar.*"},
		"s4": {"foo-1.A"},
		"s5": {"foo.A.bar.B"},
		"s6": {"foo.C.bar.D.E"},
	}
	tests := []struct {
		filter   string
		expected []string
	}{
		{
			filter:   "foo",
			expected: []string{"s1"},
		},
		{
			filter:   "bar",
			expected: []string{"s2"},
		},
		{
			filter:   "*",
			expected: []string{"s1", "s2"},
		},
		{
			filter:   ">",
			expected: []string{"s1", "s2", "s3", "s4", "s5", "s6"},
		},
		{
			filter:   "*.A",
			expected: []string{"s3", "s4"},
		},
	}
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	for name, subjects := range streams {
		if _, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: subjects}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	for _, test := range tests {
		t.Run(test.filter, func(t *testing.T) {
			names := make([]string, 0)

			// list stream names
			for name := range js.StreamNames(nats.StreamListFilter(test.filter)) {
				names = append(names, name)
			}
			if !reflect.DeepEqual(names, test.expected) {
				t.Fatalf("Invalid result; want: %v; got: %v", test.expected, names)
			}

			// list streams
			names = make([]string, 0)
			for info := range js.Streams(nats.StreamListFilter(test.filter)) {
				names = append(names, info.Config.Name)
			}
			if !reflect.DeepEqual(names, test.expected) {
				t.Fatalf("Invalid result; want: %v; got: %v", test.expected, names)
			}
		})
	}
}

func TestConsumersLister(t *testing.T) {
	tests := []struct {
		name         string
		consumersNum int
	}{
		{
			name:         "single page",
			consumersNum: 5,
		},
		{
			name:         "multi page",
			consumersNum: 1025,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			nc, js := jsClient(t, s)
			defer nc.Close()
			js.AddStream(&nats.StreamConfig{Name: "foo"})
			for i := 0; i < test.consumersNum; i++ {
				if _, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: fmt.Sprintf("cons_%d", i), AckPolicy: nats.AckExplicitPolicy}); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			names := make([]string, 0)
			for name := range js.ConsumerNames("foo") {
				names = append(names, name)
			}
			if len(names) != test.consumersNum {
				t.Fatalf("Invalid number of consumer names; want: %d; got: %d", test.consumersNum, len(names))
			}
			infos := make([]*nats.ConsumerInfo, 0)
			for info := range js.Consumers("foo") {
				infos = append(infos, info)
			}
			if len(infos) != test.consumersNum {
				t.Fatalf("Invalid number of consumers; want: %d; got: %d", test.consumersNum, len(infos))
			}

			// test the deprecated ConsumersInfo()
			infos = make([]*nats.ConsumerInfo, 0)
			for info := range js.ConsumersInfo("foo") {
				infos = append(infos, info)
			}
			if len(infos) != test.consumersNum {
				t.Fatalf("Invalid number of consumers; want: %d; got: %d", test.consumersNum, len(infos))
			}
		})
	}
}

func TestAccountInfo(t *testing.T) {
	tests := []struct {
		name      string
		cfg       string
		expected  *nats.AccountInfo
		withError error
	}{
		{
			name: "server with default values",
			cfg: `
				listen: 127.0.0.1:-1
				jetstream: enabled
			`,
			expected: &nats.AccountInfo{
				Tier: nats.Tier{
					Memory:         0,
					Store:          0,
					Streams:        0,
					Consumers:      0,
					ReservedMemory: 0,
					ReservedStore:  0,
					Limits: nats.AccountLimits{
						MaxMemory:            -1,
						MaxStore:             -1,
						MaxStreams:           -1,
						MaxConsumers:         -1,
						MaxAckPending:        -1,
						MemoryMaxStreamBytes: -1,
						StoreMaxStreamBytes:  -1,
						MaxBytesRequired:     false,
					},
				},
				API: nats.APIStats{
					Total:  0,
					Errors: 0,
				},
			},
		},
		{
			name: "server with limits set",
			cfg: `
				listen: 127.0.0.1:-1
				jetstream: {domain: "test-domain"}
				accounts: {
				A {
					users: [{ user: "foo" }]
					jetstream: {
						max_mem: 64MB,
						max_file: 32MB,
						max_streams: 10,
						max_consumers: 20,
						max_ack_pending: 100,
						memory_max_stream_bytes: 2048,
						store_max_stream_bytes: 4096,
						max_stream_bytes: true
					}
				}
				}
			`,
			expected: &nats.AccountInfo{
				Tier: nats.Tier{
					Memory:         0,
					Store:          0,
					Streams:        0,
					Consumers:      0,
					ReservedMemory: 0,
					ReservedStore:  0,
					Limits: nats.AccountLimits{
						MaxMemory:            67108864,
						MaxStore:             33554432,
						MaxStreams:           10,
						MaxConsumers:         20,
						MaxAckPending:        100,
						MemoryMaxStreamBytes: 2048,
						StoreMaxStreamBytes:  4096,
						MaxBytesRequired:     true,
					},
				},
				Domain: "test-domain",
				API: nats.APIStats{
					Total:  0,
					Errors: 0,
				},
			},
		},
		{
			name: "jetstream not enabled",
			cfg: `
				listen: 127.0.0.1:-1
			`,
			withError: nats.ErrJetStreamNotEnabled,
		},
		{
			name: "jetstream not enabled for account",
			cfg: `
			listen: 127.0.0.1:-1
			no_auth_user: foo
			jetstream: enabled
			accounts: {
				A: {
					users: [ {user: foo} ]
				},
			}
			`,
			withError: nats.ErrJetStreamNotEnabledForAccount,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(test.cfg))
			defer os.Remove(conf)
			s, _ := RunServerWithConfig(conf)
			defer shutdownJSServerAndRemoveStorage(t, s)
			nc, js := jsClient(t, s, nats.UserInfo("foo", ""))
			defer nc.Close()
			info, err := js.AccountInfo()
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: '%s'; got '%s'", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !reflect.DeepEqual(test.expected, info) {
				t.Fatalf("Account info does not match; expected: %+v; got: %+v", test.expected, info)
			}
			_, err = js.AddStream(&nats.StreamConfig{Name: "FOO", MaxBytes: 1024})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			_, err = js.AddConsumer("FOO", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// a total of 3 API calls is expected - get account info, create stream, create consumer
			test.expected.API.Total = 3
			test.expected.Streams = 1
			test.expected.Consumers = 1

			info, err = js.AccountInfo()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// ignore reserved store in comparison since this is dynamically
			// assigned by the server
			info.ReservedStore = test.expected.ReservedStore

			if !reflect.DeepEqual(test.expected, info) {
				t.Fatalf("Account info does not match; expected: %+v; got: %+v", test.expected, info)
			}
		})
	}
}

func TestPurgeStream(t *testing.T) {
	testData := []nats.Msg{
		{
			Subject: "foo.A",
			Data:    []byte("first on A"),
		},
		{
			Subject: "foo.C",
			Data:    []byte("first on C"),
		},
		{
			Subject: "foo.B",
			Data:    []byte("first on B"),
		},
		{
			Subject: "foo.C",
			Data:    []byte("second on C"),
		},
	}

	tests := []struct {
		name      string
		stream    string
		req       *nats.StreamPurgeRequest
		withError error
		expected  []nats.Msg
	}{
		{
			name:     "purge all messages",
			stream:   "foo",
			expected: []nats.Msg{},
		},
		{
			name:   "with filter subject",
			stream: "foo",
			req: &nats.StreamPurgeRequest{
				Subject: "foo.C",
			},
			expected: []nats.Msg{
				{
					Subject: "foo.A",
					Data:    []byte("first on A"),
				},
				{
					Subject: "foo.B",
					Data:    []byte("first on B"),
				},
			},
		},
		{
			name:   "with sequence",
			stream: "foo",
			req: &nats.StreamPurgeRequest{
				Sequence: 3,
			},
			expected: []nats.Msg{
				{
					Subject: "foo.B",
					Data:    []byte("first on B"),
				},
				{
					Subject: "foo.C",
					Data:    []byte("second on C"),
				},
			},
		},
		{
			name:   "with keep",
			stream: "foo",
			req: &nats.StreamPurgeRequest{
				Keep: 1,
			},
			expected: []nats.Msg{
				{
					Subject: "foo.C",
					Data:    []byte("second on C"),
				},
			},
		},
		{
			name:   "with filter and sequence",
			stream: "foo",
			req: &nats.StreamPurgeRequest{
				Subject:  "foo.C",
				Sequence: 3,
			},
			expected: []nats.Msg{
				{
					Subject: "foo.A",
					Data:    []byte("first on A"),
				},
				{
					Subject: "foo.B",
					Data:    []byte("first on B"),
				},
				{
					Subject: "foo.C",
					Data:    []byte("second on C"),
				},
			},
		},
		{
			name:   "with filter and keep",
			stream: "foo",
			req: &nats.StreamPurgeRequest{
				Subject: "foo.C",
				Keep:    1,
			},
			expected: []nats.Msg{
				{
					Subject: "foo.A",
					Data:    []byte("first on A"),
				},
				{
					Subject: "foo.B",
					Data:    []byte("first on B"),
				},
				{
					Subject: "foo.C",
					Data:    []byte("second on C"),
				},
			},
		},
		{
			name:   "empty stream name",
			stream: "",
			req: &nats.StreamPurgeRequest{
				Subject: "foo.C",
				Keep:    1,
			},
			withError: nats.ErrStreamNameRequired,
		},
		{
			name:   "invalid stream name",
			stream: "bad.stream.name",
			req: &nats.StreamPurgeRequest{
				Subject: "foo.C",
				Keep:    1,
			},
			withError: nats.ErrInvalidStreamName,
		},
		{
			name:   "invalid request - both sequence and keep provided",
			stream: "foo",
			req: &nats.StreamPurgeRequest{
				Sequence: 3,
				Keep:     1,
			},
			withError: nats.ErrBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			nc, js := jsClient(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "foo",
				Subjects: []string{"foo.A", "foo.B", "foo.C"},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			for _, msg := range testData {
				if _, err := js.PublishMsg(&msg); err != nil {
					t.Fatalf("Unexpected error during publish: %v", err)
				}
			}

			err = js.PurgeStream(test.stream, test.req)
			if test.withError != nil {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: '%s'; got '%s'", test.withError, err)
				}
				return
			}

			streamInfo, err := js.StreamInfo("foo", test.req)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if streamInfo.State.Msgs != uint64(len(test.expected)) {
				t.Fatalf("Unexpected message count: expected %d; got: %d", len(test.expected), streamInfo.State.Msgs)
			}
			sub, err := js.SubscribeSync("foo.*", nats.BindStream("foo"))
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for i := 0; i < int(streamInfo.State.Msgs); i++ {
				msg, err := sub.NextMsg(1 * time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
				if msg.Subject != test.expected[i].Subject {
					t.Fatalf("Unexpected message; subject is different than expected: want %s; got: %s", test.expected[i].Subject, msg.Subject)
				}
				if string(msg.Data) != string(test.expected[i].Data) {
					t.Fatalf("Unexpected message; data is different than expected: want %s; got: %s", test.expected[i].Data, msg.Data)
				}
			}
		})
	}
}

func TestStreamInfoSubjectInfo(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.Publish("foo.A", []byte("")); err != nil {
		t.Fatalf("Unexpected error during publish: %v", err)
	}
	if _, err := js.Publish("foo.B", []byte("")); err != nil {
		t.Fatalf("Unexpected error during publish: %v", err)
	}

	si, err := js.StreamInfo("foo", &nats.StreamInfoRequest{
		SubjectsFilter: "foo.A",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.NumSubjects != 2 {
		t.Fatal("Expected NumSubjects to be 1")
	}
	if len(si.State.Subjects) != 1 {
		t.Fatal("Expected Subjects len to be 1")
	}
	if si.State.Subjects["foo.A"] != 1 {
		t.Fatal("Expected Subjects to have an entry for foo.A with a count of 1")
	}
}

func TestStreamInfoDeletedDetails(t *testing.T) {
	testData := []string{"one", "two", "three", "four"}

	tests := []struct {
		name                   string
		stream                 string
		req                    *nats.StreamInfoRequest
		withError              error
		expectedDeletedDetails []uint64
	}{
		{
			name:   "empty request body",
			stream: "foo",
		},
		{
			name:   "with deleted details",
			stream: "foo",
			req: &nats.StreamInfoRequest{
				DeletedDetails: true,
			},
			expectedDeletedDetails: []uint64{2, 4},
		},
		{
			name:   "with deleted details set to false",
			stream: "foo",
			req: &nats.StreamInfoRequest{
				DeletedDetails: false,
			},
		},
		{
			name:      "empty stream name",
			stream:    "",
			withError: nats.ErrStreamNameRequired,
		},
		{
			name:      "invalid stream name",
			stream:    "bad.stream.name",
			withError: nats.ErrInvalidStreamName,
		},
		{
			name:      "stream not found",
			stream:    "bar",
			withError: nats.ErrStreamNotFound,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			nc, js := jsClient(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "foo",
				Subjects: []string{"foo.A"},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			for _, msg := range testData {
				if _, err := js.Publish("foo.A", []byte(msg)); err != nil {
					t.Fatalf("Unexpected error during publish: %v", err)
				}
			}
			if err := js.DeleteMsg("foo", 2); err != nil {
				t.Fatalf("Unexpected error while deleting message from stream: %v", err)
			}
			if err := js.DeleteMsg("foo", 4); err != nil {
				t.Fatalf("Unexpected error while deleting message from stream: %v", err)
			}

			var streamInfo *nats.StreamInfo
			if test.req != nil {
				streamInfo, err = js.StreamInfo(test.stream, test.req)
			} else {
				streamInfo, err = js.StreamInfo(test.stream)
			}
			if test.withError != nil {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: '%s'; got '%s'", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if streamInfo.Config.Name != "foo" {
				t.Fatalf("Invalid stream name in StreamInfo response: want: 'foo'; got: '%s'", streamInfo.Config.Name)
			}
			if streamInfo.State.NumDeleted != 2 {
				t.Fatalf("Invalid value for num_deleted in state: want: 2; got: %d", streamInfo.State.NumDeleted)
			}
			if !reflect.DeepEqual(test.expectedDeletedDetails, streamInfo.State.Deleted) {
				t.Fatalf("Invalid value for deleted msgs in state: want: %v; got: %v", test.expectedDeletedDetails, streamInfo.State.Deleted)
			}
		})
	}
}

func TestJetStreamManagement_GetMsg(t *testing.T) {
	t.Run("1-node", func(t *testing.T) {
		withJSServer(t, testJetStreamManagement_GetMsg)
	})
	t.Run("3-node", func(t *testing.T) {
		withJSCluster(t, "GET", 3, testJetStreamManagement_GetMsg)
	})
}

func testJetStreamManagement_GetMsg(t *testing.T, srvs ...*jsServer) {
	s := srvs[0]
	nc, js := jsClient(t, s.Server)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 5; i++ {
		msg := nats.NewMsg("foo.A")
		data := fmt.Sprintf("A:%d", i)
		msg.Data = []byte(data)
		msg.Header = nats.Header{
			"X-NATS-Key": []string{"123"},
		}
		msg.Header.Add("X-Nats-Test-Data", data)
		js.PublishMsg(msg)
		js.Publish("foo.B", []byte(fmt.Sprintf("B:%d", i)))
		js.Publish("foo.C", []byte(fmt.Sprintf("C:%d", i)))
	}

	var originalSeq uint64
	t.Run("get message", func(t *testing.T) {
		expected := 5
		msgs := make([]*nats.Msg, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		sub, err := js.Subscribe("foo.C", func(msg *nats.Msg) {
			msgs = append(msgs, msg)
			if len(msgs) == expected {
				cancel()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		<-ctx.Done()
		sub.Unsubscribe()

		got := len(msgs)
		if got != expected {
			t.Fatalf("Expected: %d, got: %d", expected, got)
		}

		msg := msgs[3]
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatal(err)
		}
		originalSeq = meta.Sequence.Stream

		// Get the same message using JSM.
		fetchedMsg, err := js.GetMsg("foo", originalSeq)
		if err != nil {
			t.Fatal(err)
		}

		expectedData := "C:3"
		if string(fetchedMsg.Data) != expectedData {
			t.Errorf("Expected: %v, got: %v", expectedData, string(fetchedMsg.Data))
		}
	})

	t.Run("get deleted message", func(t *testing.T) {
		err := js.DeleteMsg("foo", originalSeq)
		if err != nil {
			t.Fatal(err)
		}

		si, err := js.StreamInfo("foo")
		if err != nil {
			t.Fatal(err)
		}
		expected := 14
		if int(si.State.Msgs) != expected {
			t.Errorf("Expected %d msgs, got: %d", expected, si.State.Msgs)
		}

		// There should be only 4 messages since one deleted.
		expected = 4
		msgs := make([]*nats.Msg, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		sub, err := js.Subscribe("foo.C", func(msg *nats.Msg) {
			msgs = append(msgs, msg)

			if len(msgs) == expected {
				cancel()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		<-ctx.Done()
		sub.Unsubscribe()

		msg := msgs[3]
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatal(err)
		}
		newSeq := meta.Sequence.Stream

		// First message removed
		if newSeq <= originalSeq {
			t.Errorf("Expected %d to be higher sequence than %d",
				newSeq, originalSeq)
		}

		// Try to fetch the same message which should be gone.
		_, err = js.GetMsg("foo", originalSeq)
		if err == nil || err != nats.ErrMsgNotFound {
			t.Errorf("Expected no message found error, got: %v", err)
		}
	})

	t.Run("get message with headers", func(t *testing.T) {
		streamMsg, err := js.GetMsg("foo", 4)
		if err != nil {
			t.Fatal(err)
		}
		if streamMsg.Sequence != 4 {
			t.Errorf("Expected %v, got: %v", 4, streamMsg.Sequence)
		}
		expectedMap := map[string][]string{
			"X-Nats-Test-Data": {"A:1"},
			"X-NATS-Key":       {"123"},
		}
		if !reflect.DeepEqual(streamMsg.Header, nats.Header(expectedMap)) {
			t.Errorf("Expected %v, got: %v", expectedMap, streamMsg.Header)
		}

		sub, err := js.SubscribeSync("foo.A", nats.StartSequence(4))
		if err != nil {
			t.Fatal(err)
		}
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(msg.Header, nats.Header(expectedMap)) {
			t.Errorf("Expected %v, got: %v", expectedMap, msg.Header)
		}
	})
}

func TestJetStreamManagement_DeleteMsg(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 5; i++ {
		js.Publish("foo.A", []byte("A"))
		js.Publish("foo.B", []byte("B"))
		js.Publish("foo.C", []byte("C"))
	}

	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatal(err)
	}
	var total uint64 = 15
	if si.State.Msgs != total {
		t.Errorf("Expected %d msgs, got: %d", total, si.State.Msgs)
	}

	expected := 5
	msgs := make([]*nats.Msg, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err := js.Subscribe("foo.C", func(msg *nats.Msg) {
		msgs = append(msgs, msg)
		if len(msgs) == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	sub.Unsubscribe()

	got := len(msgs)
	if got != expected {
		t.Fatalf("Expected %d, got %d", expected, got)
	}

	msg := msgs[0]
	meta, err := msg.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	originalSeq := meta.Sequence.Stream

	// create a subscription on delete message API subject to verify the content of delete operation
	apiSub, err := nc.SubscribeSync("$JS.API.STREAM.MSG.DELETE.foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	err = js.DeleteMsg("foo", originalSeq)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = apiSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if str := string(msg.Data); !strings.Contains(str, "no_erase\":true") {
		t.Fatalf("Request should not have no_erase field set: %s", str)
	}

	si, err = js.StreamInfo("foo")
	if err != nil {
		t.Fatal(err)
	}
	total = 14
	if si.State.Msgs != total {
		t.Errorf("Expected %d msgs, got: %d", total, si.State.Msgs)
	}

	// There should be only 4 messages since one deleted.
	expected = 4
	msgs = make([]*nats.Msg, 0)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err = js.Subscribe("foo.C", func(msg *nats.Msg) {
		msgs = append(msgs, msg)

		if len(msgs) == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	sub.Unsubscribe()

	msg = msgs[0]
	meta, err = msg.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	newSeq := meta.Sequence.Stream

	// First message removed
	if newSeq <= originalSeq {
		t.Errorf("Expected %d to be higher sequence than %d", newSeq, originalSeq)
	}
}

func TestJetStreamManagement_SecureDeleteMsg(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 5; i++ {
		js.Publish("foo.A", []byte("A"))
		js.Publish("foo.B", []byte("B"))
		js.Publish("foo.C", []byte("C"))
	}

	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatal(err)
	}
	var total uint64 = 15
	if si.State.Msgs != total {
		t.Errorf("Expected %d msgs, got: %d", total, si.State.Msgs)
	}

	expected := 5
	msgs := make([]*nats.Msg, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err := js.Subscribe("foo.C", func(msg *nats.Msg) {
		msgs = append(msgs, msg)
		if len(msgs) == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	sub.Unsubscribe()

	got := len(msgs)
	if got != expected {
		t.Fatalf("Expected %d, got %d", expected, got)
	}

	msg := msgs[0]
	meta, err := msg.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	originalSeq := meta.Sequence.Stream

	// create a subscription on delete message API subject to verify the content of delete operation
	apiSub, err := nc.SubscribeSync("$JS.API.STREAM.MSG.DELETE.foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	err = js.SecureDeleteMsg("foo", originalSeq)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = apiSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if str := string(msg.Data); strings.Contains(str, "no_erase\":true") {
		t.Fatalf("Request should not have no_erase field set: %s", str)
	}

	si, err = js.StreamInfo("foo")
	if err != nil {
		t.Fatal(err)
	}
	total = 14
	if si.State.Msgs != total {
		t.Errorf("Expected %d msgs, got: %d", total, si.State.Msgs)
	}

	// There should be only 4 messages since one deleted.
	expected = 4
	msgs = make([]*nats.Msg, 0)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err = js.Subscribe("foo.C", func(msg *nats.Msg) {
		msgs = append(msgs, msg)

		if len(msgs) == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	sub.Unsubscribe()

	msg = msgs[0]
	meta, err = msg.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	newSeq := meta.Sequence.Stream

	// First message removed
	if newSeq <= originalSeq {
		t.Errorf("Expected %d to be higher sequence than %d", newSeq, originalSeq)
	}
}

func TestJetStreamImport(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.>" },  { service: "foo" }]
			},
			U: {
				users: [ {user: rip, password: bar} ]
				imports [
					{ service: { subject: "$JS.API.>", account: JS } , to: "dlc.>" }
					{ service: { subject: "foo", account: JS } }
				]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	// Create a stream using JSM.
	ncm, jsm := jsClient(t, s, nats.UserInfo("dlc", "foo"))
	defer ncm.Close()

	var err error

	_, err = jsm.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	// Client with the imports.
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// Since we import with a prefix from above we can use that when creating our JS context.
	js, err := nc.JetStream(nats.APIPrefix("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg := []byte("Hello JS Import!")

	if _, err = js.Publish("foo", msg); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
}

func TestJetStreamImportDirectOnly(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [
					# For now have to expose the API to enable JS context across account.
					{ service: "$JS.API.INFO" }
					# For the stream publish.
					{ service: "ORDERS" }
					# For the pull based consumer. Response type needed for batchsize > 1
					{ service: "$JS.API.CONSUMER.INFO.ORDERS.d1", response: stream }
					{ service: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", response: stream }
					# For the push based consumer delivery and ack.
					{ stream: "p.d" }
					{ stream: "p.d3" }
					# For the acks. Service in case we want an ack to our ack.
					{ service: "$JS.ACK.ORDERS.*.>" }

					# Allow lookup of stream to be able to bind from another account.
					{ service: "$JS.API.CONSUMER.INFO.ORDERS.d4", response: stream }
					{ stream: "p.d4" }
				]
			},
			U: {
				users: [ { user: rip, password: bar } ]
				imports [
					{ service: { subject: "$JS.API.INFO", account: JS } }
					{ service: { subject: "ORDERS", account: JS } , to: "orders" }
					# { service: { subject: "$JS.API.CONSUMER.INFO.ORDERS.d1", account: JS } }
					{ service: { subject: "$JS.API.CONSUMER.INFO.ORDERS.d4", account: JS } }
					{ service: { subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", account: JS } }
					{ stream:  { subject: "p.d", account: JS } }
					{ stream:  { subject: "p.d3", account: JS } }
					{ stream:  { subject: "p.d4", account: JS } }
					{ service: { subject: "$JS.ACK.ORDERS.*.>", account: JS } }
				]
			},
			V: {
				users: [ {
					user: v,
					password: quux,
					permissions: { publish: {deny: ["$JS.API.CONSUMER.INFO.ORDERS.d1"]} }
				} ]
				imports [
					{ service: { subject: "$JS.API.INFO", account: JS } }
					{ service: { subject: "ORDERS", account: JS } , to: "orders" }
					{ service: { subject: "$JS.API.CONSUMER.INFO.ORDERS.d1", account: JS } }
					{ service: { subject: "$JS.API.CONSUMER.INFO.ORDERS.d4", account: JS } }
					{ service: { subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", account: JS } }
					{ stream:  { subject: "p.d", account: JS } }
					{ stream:  { subject: "p.d3", account: JS } }
					{ stream:  { subject: "p.d4", account: JS } }
					{ service: { subject: "$JS.ACK.ORDERS.*.>", account: JS } }
				]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	// Create a stream using JSM.
	ncm, jsm := jsClient(t, s, nats.UserInfo("dlc", "foo"))
	defer ncm.Close()

	var err error

	// Create a stream using the server directly.
	_, err = jsm.AddStream(&nats.StreamConfig{Name: "ORDERS"})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	// Create a pull based consumer.
	_, err = jsm.AddConsumer("ORDERS", &nats.ConsumerConfig{Durable: "d1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("pull consumer create failed: %v", err)
	}

	// Create a push based consumers.
	_, err = jsm.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:        "d2",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "p.d",
	})
	if err != nil {
		t.Fatalf("push consumer create failed: %v", err)
	}

	_, err = jsm.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:        "d3",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "p.d3",
	})
	if err != nil {
		t.Fatalf("push consumer create failed: %v", err)
	}

	_, err = jsm.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:        "d4",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "p.d4",
	})
	if err != nil {
		t.Fatalf("push consumer create failed: %v", err)
	}

	nc, js := jsClient(t, s)
	defer nc.Close()

	// Now make sure we can send to the stream from another account.
	toSend := 100
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("orders", []byte(fmt.Sprintf("ORDER-%d", i+1))); err != nil {
			t.Fatalf("Unexpected error publishing message %d: %v", i+1, err)
		}
	}

	var sub *nats.Subscription

	waitForPending := func(t *testing.T, n int) {
		t.Helper()
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _, _ := sub.Pending(); msgs == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		msgs, _, _ := sub.Pending()
		t.Fatalf("Expected to receive %d messages, but got %d", n, msgs)
	}

	// Do push based consumer using a regular NATS subscription on the import subject.
	sub, err = nc.SubscribeSync("p.d3")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	waitForPending(t, toSend)

	// Can also ack from the regular NATS subscription via the imported subject.
	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Test that can expect an ack of the ack.
		err = m.AckSync()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	// Can attach to the consumer from another JS account if there is a durable name.
	sub, err = js.SubscribeSync("ORDERS", nats.Durable("d4"), nats.BindStream("ORDERS"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	waitForPending(t, toSend)

	// Even if there are no permissions or import to check that a consumer exists,
	// it is still possible to bind subscription to it.
	sub, err = js.PullSubscribe("ORDERS", "d1", nats.Bind("ORDERS", "d1"))
	if err != nil {
		t.Fatal(err)
	}
	expected := 10
	msgs, err := sub.Fetch(expected)
	if err != nil {
		t.Fatal(err)
	}
	got := len(msgs)
	if got != expected {
		t.Fatalf("Expected %d, got %d", expected, got)
	}

	// Account without permissions to lookup should be able to bind as well.
	eh := func(_ *nats.Conn, _ *nats.Subscription, err error) {}
	nc, err = nats.Connect(s.ClientURL(), nats.UserInfo("v", "quux"), nats.ErrorHandler(eh))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// Since we know that the lookup will fail, we use a smaller timeout than the 5s default.
	js, err = nc.JetStream(nats.MaxWait(500 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	sub, err = js.PullSubscribe("ORDERS", "d1", nats.Bind("ORDERS", "d1"))
	if err != nil {
		t.Fatal(err)
	}
	expected = 10
	msgs, err = sub.Fetch(expected)
	if err != nil {
		t.Fatal(err)
	}
	got = len(msgs)
	if got != expected {
		t.Fatalf("Expected %d, got %d", expected, got)
	}
}

func TestJetStreamCrossAccountMirrorsAndSources(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts {
			JS {
				jetstream: enabled
				users = [ { user: "rip", pass: "pass" } ]
				exports [
					{ service: "$JS.API.CONSUMER.>" } # To create internal consumers to mirror/source.
					{ stream: "RI.DELIVER.SYNC.>" }   # For the mirror/source consumers sending to IA via delivery subject.
				]
			}
			IA {
				jetstream: enabled
				users = [ { user: "dlc", pass: "pass" } ]
				imports [
					{ service: { account: JS, subject: "$JS.API.CONSUMER.>"}, to: "RI.JS.API.CONSUMER.>" }
					{ stream: { account: JS, subject: "RI.DELIVER.SYNC.>"} }
				]
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc1, js1 := jsClient(t, s, nats.UserInfo("rip", "pass"))
	defer nc1.Close()

	var err error

	_, err = js1.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	const (
		toSend      = 100
		publishSubj = "TEST"
		sourceName  = "MY_SOURCE_TEST"
		mirrorName  = "MY_MIRROR_TEST"
	)
	for i := 0; i < toSend; i++ {
		data := []byte(fmt.Sprintf("OK %d", i))
		if _, err := js1.Publish(publishSubj, data); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nc2, js2 := jsClient(t, s, nats.UserInfo("dlc", "pass"))
	defer nc2.Close()

	checkMsgCount := func(t *testing.T, stream string) {
		t.Helper()
		checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
			si, err := js2.StreamInfo(stream)
			if err != nil {
				return err
			}
			if si.State.Msgs != uint64(toSend) {
				return fmt.Errorf("Expected %d msgs, got state: %+v", toSend, si.State)
			}
			return nil
		})
	}
	checkConsume := func(t *testing.T, js nats.JetStream, subject, stream string, want int) {
		t.Helper()
		sub, err := js.SubscribeSync(subject, nats.BindStream(stream))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		checkSubsPending(t, sub, want)

		for i := 0; i < want; i++ {
			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			meta, err := msg.Metadata()
			if err != nil {
				t.Fatal(err)
			}
			if got, want := meta.Stream, stream; got != want {
				t.Fatalf("unexpected stream name, got=%q, want=%q", got, want)
			}
		}
	}

	_, err = js2.AddStream(&nats.StreamConfig{
		Name:    mirrorName,
		Storage: nats.FileStorage,
		Mirror: &nats.StreamSource{
			Name: publishSubj,
			External: &nats.ExternalStream{
				APIPrefix:     "RI.JS.API",
				DeliverPrefix: "RI.DELIVER.SYNC.MIRRORS",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkMsgCount(t, mirrorName)
	checkConsume(t, js2, publishSubj, mirrorName, toSend)

	_, err = js2.AddStream(&nats.StreamConfig{
		Name:    sourceName,
		Storage: nats.FileStorage,
		Sources: []*nats.StreamSource{
			{
				Name: publishSubj,
				External: &nats.ExternalStream{
					APIPrefix:     "RI.JS.API",
					DeliverPrefix: "RI.DELIVER.SYNC.SOURCES",
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkMsgCount(t, sourceName)
	checkConsume(t, js2, publishSubj, sourceName, toSend)
}

func TestJetStreamAutoMaxAckPending(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s, nats.SyncQueueLen(500))
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10_000

	msg := []byte("Hello")
	for i := 0; i < toSend; i++ {
		// Use plain NATS here for speed.
		nc.Publish("foo", msg)
	}
	nc.Flush()

	// Create a consumer.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectedMaxAck, _, _ := sub.PendingLimits()

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Config.MaxAckPending != expectedMaxAck {
		t.Fatalf("Expected MaxAckPending to be set to %d, got %d", expectedMaxAck, ci.Config.MaxAckPending)
	}

	waitForPending := func(n int) {
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _, _ := sub.Pending(); msgs == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		msgs, _, _ := sub.Pending()
		t.Fatalf("Expected to receive %d messages, but got %d", n, msgs)
	}

	waitForPending(expectedMaxAck)
	// We do it twice to make sure it does not go over.
	waitForPending(expectedMaxAck)

	// Now make sure we can consume them all with no slow consumers etc.
	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error receiving %d: %v", i+1, err)
		}
		m.Ack()
	}
}

func TestJetStreamInterfaces(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	var jsm nats.JetStreamManager
	var jsctx nats.JetStreamContext

	// JetStream that can publish/subscribe but cannot manage streams.
	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	js.Publish("foo", []byte("hello"))

	// JetStream context that can manage streams/consumers but cannot produce messages.
	jsm, err = nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsm.AddStream(&nats.StreamConfig{Name: "FOO"})

	// JetStream context that can both manage streams/consumers
	// as well as publish/subscribe.
	jsctx, err = nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsctx.AddStream(&nats.StreamConfig{Name: "BAR"})
	jsctx.Publish("bar", []byte("hello world"))

	publishMsg := func(js nats.JetStream, payload []byte) {
		js.Publish("foo", payload)
	}
	publishMsg(js, []byte("hello world"))
}

func TestJetStreamSubscribe_DeliverPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var publishTime time.Time

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		if i == 5 {
			publishTime = time.Now()
		}
		js.Publish("foo", []byte(payload))
		time.Sleep(15 * time.Millisecond)
	}

	for _, test := range []struct {
		name     string
		subopt   nats.SubOpt
		expected int
	}{
		{
			"deliver.all", nats.DeliverAll(), 10,
		},
		{
			"deliver.last", nats.DeliverLast(), 1,
		},
		{
			"deliver.new", nats.DeliverNew(), 0,
		},
		{
			"deliver.starttime", nats.StartTime(publishTime), 5,
		},
		{
			"deliver.startseq", nats.StartSequence(6), 5,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			timeout := 2 * time.Second
			if test.expected == 0 {
				timeout = 250 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			got := 0
			sub, err := js.Subscribe("foo", func(m *nats.Msg) {
				got++
				if got == test.expected {
					cancel()
				}
			}, test.subopt)

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			<-ctx.Done()
			sub.Drain()

			if got != test.expected {
				t.Fatalf("Expected %d, got %d", test.expected, got)
			}
		})
	}

	js.Publish("bar", []byte("bar msg 1"))
	js.Publish("bar", []byte("bar msg 2"))

	sub, err := js.SubscribeSync("bar", nats.BindStream("TEST"), nats.DeliverLastPerSubject())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error on next msg: %v", err)
	}
	if string(msg.Data) != "bar msg 2" {
		t.Fatalf("Unexpected last message: %q", msg.Data)
	}
}

func TestJetStreamSubscribe_AckPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish("foo", []byte(payload))
	}

	for _, test := range []struct {
		name     string
		subopt   nats.SubOpt
		expected nats.AckPolicy
	}{
		{
			"ack-none", nats.AckNone(), nats.AckNonePolicy,
		},
		{
			"ack-all", nats.AckAll(), nats.AckAllPolicy,
		},
		{
			"ack-explicit", nats.AckExplicit(), nats.AckExplicitPolicy,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			got := 0
			totalMsgs := 10
			sub, err := js.Subscribe("foo", func(m *nats.Msg) {
				got++
				if got == totalMsgs {
					cancel()
				}
			}, test.subopt, nats.Durable(test.name))

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			<-ctx.Done()
			if got != totalMsgs {
				t.Fatalf("Expected %d, got %d", totalMsgs, got)
			}

			// check if consumer is configured properly
			ci, err := js.ConsumerInfo("TEST", test.name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci.Config.AckPolicy != test.expected {
				t.Fatalf("Expected %v, got %v", test.expected, ci.Config.AckPolicy)
			}

			// drain the subscription. This will remove the consumer
			sub.Drain()
		})
	}

	checkAcks := func(t *testing.T, sub *nats.Subscription) {
		// Normal Ack
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		meta, err := msg.Metadata()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if meta.Sequence.Consumer != 1 || meta.Sequence.Stream != 1 || meta.NumDelivered != 1 {
			t.Errorf("Unexpected metadata: %v", meta)
		}

		got := string(msg.Data)
		expected := "i:0"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.Ack()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		// AckSync
		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:1"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}

		// Give an already canceled context.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = msg.AckSync(nats.Context(ctx))
		if err != context.Canceled {
			t.Errorf("Unexpected error: %v", err)
		}

		// Context that not yet canceled.
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Prevent double context and ack wait options.
		err = msg.AckSync(nats.Context(ctx), nats.AckWait(1*time.Second))
		if err != nats.ErrContextAndTimeout {
			t.Errorf("Unexpected error: %v", err)
		}

		err = msg.AckSync(nats.Context(ctx))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		err = msg.AckSync(nats.AckWait(2 * time.Second))
		if err != nats.ErrMsgAlreadyAckd {
			t.Errorf("Unexpected error: %v", err)
		}

		// AckSync default
		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:2"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.AckSync()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		// Nak
		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:3"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}

		// Prevent double context and ack wait options.
		err = msg.Nak(nats.Context(ctx), nats.AckWait(1*time.Second))
		if err != nats.ErrContextAndTimeout {
			t.Errorf("Unexpected error: %v", err)
		}

		// Skip the message.
		err = msg.Nak()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:4"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.Nak(nats.AckWait(2 * time.Second))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:5"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}

		// Prevent double context and ack wait options.
		err = msg.Term(nats.Context(ctx), nats.AckWait(1*time.Second))
		if err != nats.ErrContextAndTimeout {
			t.Errorf("Unexpected error: %v", err)
		}

		err = msg.Term()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()

		// Convert context into nats option.
		nctx := nats.Context(ctx)
		msg, err = sub.NextMsgWithContext(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:6"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.Term(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		msg, err = sub.NextMsgWithContext(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:7"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}

		// Prevent double context and ack wait options.
		err = msg.InProgress(nats.Context(ctx), nats.AckWait(1*time.Second))
		if err != nats.ErrContextAndTimeout {
			t.Errorf("Unexpected error: %v", err)
		}

		err = msg.InProgress(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		err = msg.InProgress(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		err = msg.Ack(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	t.Run("js sub ack", func(t *testing.T) {
		sub, err := js.SubscribeSync("foo", nats.Durable("wq2"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		checkAcks(t, sub)
	})

	t.Run("non js sub ack", func(t *testing.T) {
		inbox := nats.NewInbox()
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:        "wq",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverPolicy:  nats.DeliverAllPolicy,
			DeliverSubject: inbox,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		checkAcks(t, sub)
	})

	t.Run("Nak with delay", func(t *testing.T) {
		js.Publish("bar", []byte("msg"))
		sub, err := js.SubscribeSync("bar", nats.Durable("nak_dur"))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on NextMsg: %v", err)
		}
		if err := msg.NakWithDelay(500 * time.Millisecond); err != nil {
			t.Fatalf("Error on Nak: %v", err)
		}
		// We should not get redelivery before 500ms+
		if _, err = sub.NextMsg(250 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Expected timeout, got %v", err)
		}
		msg, err = sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on NextMsg: %v", err)
		}
		if err := msg.NakWithDelay(0); err != nil {
			t.Fatalf("Error on Nak: %v", err)
		}
		msg, err = sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			t.Fatalf("Expected timeout, got %v", err)
		}
		msg.Ack()
	})

	t.Run("BackOff redeliveries", func(t *testing.T) {
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		defer sub.Unsubscribe()
		cc := nats.ConsumerConfig{
			Durable:        "backoff",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverPolicy:  nats.DeliverAllPolicy,
			FilterSubject:  "bar",
			DeliverSubject: inbox,
			BackOff:        []time.Duration{50 * time.Millisecond, 250 * time.Millisecond},
		}
		// First, try with a MaxDeliver that is < len(BackOff), which the
		// server should reject.
		cc.MaxDeliver = 1
		_, err = js.AddConsumer("TEST", &cc)
		if err == nil || !strings.Contains(err.Error(), "max deliver is required to be > length of backoff values") {
			t.Fatalf("Expected backoff/max deliver error, got %v", err)
		}
		// Now put a valid value
		cc.MaxDeliver = 4
		ci, err := js.AddConsumer("TEST", &cc)
		if err != nil {
			t.Fatalf("Error on add consumer: %v", err)
		}
		if !reflect.DeepEqual(ci.Config.BackOff, cc.BackOff) {
			t.Fatalf("Expected backoff to be %v, got %v", cc.BackOff, ci.Config.BackOff)
		}
		// Consume the first delivery
		_, err = sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on nextMsg: %v", err)
		}
		// We should get a redelivery at around 50ms
		start := time.Now()
		_, err = sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on nextMsg: %v", err)
		}
		if dur := time.Since(start); dur < 25*time.Millisecond || dur > 100*time.Millisecond {
			t.Fatalf("Expected to be redelivered at around 50ms, took %v", dur)
		}
		// Now it should be every 250ms or so
		for i := 0; i < 2; i++ {
			start = time.Now()
			_, err = sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Error on nextMsg for iter=%v: %v", i+1, err)
			}
			if dur := time.Since(start); dur < 200*time.Millisecond || dur > 300*time.Millisecond {
				t.Fatalf("Expected to be redelivered at around 250ms, took %v", dur)
			}
		}
		// At this point, we should have go reach MaxDeliver
		_, err = sub.NextMsg(300 * time.Millisecond)
		if err != nats.ErrTimeout {
			t.Fatalf("Expected timeout, got %v", err)
		}
	})
}

func TestJetStreamPullSubscribe_AckPending(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	const totalMsgs = 10
	for i := 0; i < totalMsgs; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish("foo", []byte(payload))
	}

	sub, err := js.PullSubscribe("foo", "wq",
		nats.AckWait(200*time.Millisecond),
		nats.MaxAckPending(5),
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nextMsg := func() *nats.Msg {
		t.Helper()
		msgs, err := sub.Fetch(1)
		if err != nil {
			t.Fatal(err)
		}
		return msgs[0]
	}

	getPending := func() (int, int) {
		t.Helper()
		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		return info.NumAckPending, int(info.NumPending)
	}

	getMetadata := func(msg *nats.Msg) *nats.MsgMetadata {
		t.Helper()
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return meta
	}

	expectedPending := func(inflight int, pending int) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			i, p := getPending()
			if i != inflight || p != pending {
				return fmt.Errorf("Unexpected inflight/pending msgs: %v/%v", i, p)
			}
			return nil
		})
	}

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		inflight, pending := getPending()
		if inflight != 0 || pending != totalMsgs {
			return fmt.Errorf("Unexpected inflight/pending msgs: %v/%v", inflight, pending)
		}
		return nil
	})

	// Normal Ack should decrease pending
	msg := nextMsg()
	err = msg.Ack()
	if err != nil {
		t.Fatal(err)
	}

	expectedPending(0, 9)
	meta := getMetadata(msg)

	if meta.Sequence.Consumer != 1 || meta.Sequence.Stream != 1 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// AckSync
	msg = nextMsg()
	err = msg.AckSync()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(0, 8)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 2 || meta.Sequence.Stream != 2 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// Nak the message so that it is redelivered.
	msg = nextMsg()
	err = msg.Nak()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(1, 7)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 3 || meta.Sequence.Stream != 3 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}
	prevSeq := meta.Sequence.Stream
	prevPayload := string(msg.Data)

	// Nak same sequence again, sequence number should not change.
	msg = nextMsg()
	err = msg.Nak()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(1, 7)
	meta = getMetadata(msg)
	if meta.Sequence.Stream != prevSeq {
		t.Errorf("Expected to get message at seq=%v, got seq=%v", prevSeq, meta.Sequence.Stream)
	}
	if string(msg.Data) != prevPayload {
		t.Errorf("Expected: %q, got: %q", string(prevPayload), string(msg.Data))
	}
	if meta.Sequence.Consumer != 4 || meta.NumDelivered != 2 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// Terminate message so it is no longer pending.
	msg = nextMsg()
	err = msg.Term()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(0, 7)
	meta = getMetadata(msg)
	if meta.Sequence.Stream != prevSeq {
		t.Errorf("Expected to get message at seq=%v, got seq=%v", prevSeq, meta.Sequence.Stream)
	}
	if string(msg.Data) != prevPayload {
		t.Errorf("Expected: %q, got: %q", string(prevPayload), string(msg.Data))
	}
	if meta.Sequence.Consumer != 5 || meta.Sequence.Stream != 3 || meta.NumDelivered != 3 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// Get next message and ack in progress a few times
	msg = nextMsg()
	expected := "i:3"
	if string(msg.Data) != expected {
		t.Errorf("Expected: %q, got: %q", string(msg.Data), expected)
	}
	err = msg.InProgress()
	if err != nil {
		t.Fatal(err)
	}
	err = msg.InProgress()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(1, 6)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 6 || meta.Sequence.Stream != 4 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// Now ack the message to mark it as done.
	err = msg.AckSync()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(0, 6)

	// Fetch next message, but do not ack and wait for redelivery.
	msg = nextMsg()
	expectedPending(1, 5)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 7 || meta.Sequence.Stream != 5 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}
	prevSeq = meta.Sequence.Stream
	time.Sleep(500 * time.Millisecond)
	expectedPending(1, 5)

	// Next message should be a redelivery.
	msg = nextMsg()
	expectedPending(1, 5)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 8 || meta.Sequence.Stream != prevSeq || meta.NumDelivered != 2 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}
	err = msg.AckSync()
	if err != nil {
		t.Fatal(err)
	}

	// Get rest of messages.
	count := 5
	for count > 0 {
		msgs, err := sub.Fetch(count)
		if err != nil {
			t.Fatal(err)
		}
		for _, msg := range msgs {
			count--
			getMetadata(msg)
			msg.Ack()
		}
	}
	expectedPending(0, 0)
}

func TestJetStreamSubscribe_AckDup(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("hello"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	pings := make(chan struct{}, 6)
	nc.Subscribe("$JS.ACK.TEST.>", func(msg *nats.Msg) {
		pings <- struct{}{}
	})
	nc.Flush()

	ch := make(chan error, 6)
	_, err = js.Subscribe("foo", func(m *nats.Msg) {
		// Only first ack will be sent, auto ack that will occur after
		// this won't be sent either.
		ch <- m.Ack()

		// Any following acks should fail.
		ch <- m.Ack()
		ch <- m.Nak()
		ch <- m.AckSync()
		ch <- m.Term()
		ch <- m.InProgress()
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	<-ctx.Done()
	ackErr1 := <-ch
	if ackErr1 != nil {
		t.Errorf("Unexpected error: %v", ackErr1)
	}

	for i := 0; i < 5; i++ {
		e := <-ch
		if e != nats.ErrMsgAlreadyAckd {
			t.Errorf("Expected error: %v", e)
		}
	}
	if len(pings) != 1 {
		t.Logf("Expected to receive a single ack, got: %v", len(pings))
	}
}

func TestJetStreamSubscribe_AutoAck(t *testing.T) {
	tests := []struct {
		name        string
		opt         nats.SubOpt
		expectedAck bool
	}{
		{
			name:        "with ack explicit",
			opt:         nats.AckExplicit(),
			expectedAck: true,
		},
		{
			name:        "with ack all",
			opt:         nats.AckAll(),
			expectedAck: true,
		},
		{
			name:        "with ack none",
			opt:         nats.AckNone(),
			expectedAck: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			nc, js := jsClient(t, s)
			defer nc.Close()

			var err error

			// Create the stream using our client API.
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			js.Publish("foo", []byte("hello"))

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			acks := make(chan struct{}, 2)
			nc.Subscribe("$JS.ACK.TEST.>", func(msg *nats.Msg) {
				acks <- struct{}{}
			})
			nc.Flush()

			_, err = js.Subscribe("foo", func(m *nats.Msg) {
			}, test.opt)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			<-ctx.Done()

			if test.expectedAck {
				if len(acks) != 1 {
					t.Fatalf("Expected to receive a single ack, got: %v", len(acks))
				}
				return
			}
			if len(acks) != 0 {
				t.Fatalf("Expected no acks, got: %v", len(acks))
			}
		})
	}
}

func TestJetStreamSubscribe_AckDupInProgress(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("hello"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	pings := make(chan struct{}, 3)
	nc.Subscribe("$JS.ACK.TEST.>", func(msg *nats.Msg) {
		pings <- struct{}{}
	})
	nc.Flush()

	ch := make(chan error, 3)
	_, err = js.Subscribe("foo", func(m *nats.Msg) {
		// InProgress ACK can be sent any number of times.
		ch <- m.InProgress()
		ch <- m.InProgress()
		ch <- m.Ack()
	}, nats.Durable("WQ"), nats.ManualAck())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	<-ctx.Done()
	ackErr1 := <-ch
	ackErr2 := <-ch
	ackErr3 := <-ch
	if ackErr1 != nil {
		t.Errorf("Unexpected error: %v", ackErr1)
	}
	if ackErr2 != nil {
		t.Errorf("Unexpected error: %v", ackErr2)
	}
	if ackErr3 != nil {
		t.Errorf("Unexpected error: %v", ackErr3)
	}
	if len(pings) != 3 {
		t.Logf("Expected to receive multiple acks, got: %v", len(pings))
	}
}

func TestJetStream_Unsubscribe(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchConsumers := func(t *testing.T, expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			var infos []*nats.ConsumerInfo
			for info := range js.Consumers("foo") {
				infos = append(infos, info)
			}
			if len(infos) != expected {
				return fmt.Errorf("Expected %d consumers, got: %d", expected, len(infos))
			}
			return nil
		})
	}

	deleteAllConsumers := func(t *testing.T) {
		t.Helper()
		for cn := range js.ConsumerNames("foo") {
			js.DeleteConsumer("foo", cn)
		}
	}

	js.Publish("foo.A", []byte("A"))
	js.Publish("foo.B", []byte("B"))
	js.Publish("foo.C", []byte("C"))

	t.Run("consumers deleted on unsubscribe", func(t *testing.T) {
		sub, err := js.SubscribeSync("foo.A")
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		sub, err = js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		sub, err = js.Subscribe("foo.C", func(_ *nats.Msg) {})
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		sub, err = js.Subscribe("foo.C", func(_ *nats.Msg) {}, nats.Durable("C"))
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 0)
	})

	t.Run("not deleted on unsubscribe if consumer created externally", func(t *testing.T) {
		// Created by JetStreamManagement
		if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:   "wq",
			AckPolicy: nats.AckExplicitPolicy,
			// Need to specify filter subject here otherwise
			// would get messages from foo.A as well.
			FilterSubject: "foo.C",
		}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		subC, err := js.PullSubscribe("foo.C", "wq")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 1)

		msgs, err := subC.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Errorf("Unexpected error getting message: %v", err)
		}
		msg := msgs[0]
		got := string(msg.Data)
		expected := "C"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		subC.Unsubscribe()
		fetchConsumers(t, 1)
		deleteAllConsumers(t)
	})

	t.Run("consumers deleted on drain", func(t *testing.T) {
		subA, err := js.Subscribe("foo.A", func(_ *nats.Msg) {})
		if err != nil {
			t.Fatal(err)
		}
		fetchConsumers(t, 1)
		err = subA.Drain()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 0)
		deleteAllConsumers(t)
	})

	t.Run("durable consumers deleted on drain", func(t *testing.T) {
		subB, err := js.Subscribe("foo.B", func(_ *nats.Msg) {}, nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}
		fetchConsumers(t, 1)
		err = subB.Drain()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 0)
	})
}

func TestJetStream_UnsubscribeCloseDrain(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	serverURL := s.ClientURL()
	mc, jsm := jsClient(t, s)
	defer mc.Close()

	var err error

	_, err = jsm.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchConsumers := func(t *testing.T, expected int) []*nats.ConsumerInfo {
		t.Helper()
		var infos []*nats.ConsumerInfo
		for info := range jsm.Consumers("foo") {
			infos = append(infos, info)
		}
		if len(infos) != expected {
			t.Fatalf("Expected %d consumers, got: %d", expected, len(infos))
		}

		return infos
	}

	t.Run("conn drain deletes ephemeral consumers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		nc, err := nats.Connect(serverURL, nats.ClosedHandler(func(_ *nats.Conn) {
			cancel()
		}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.SubscribeSync("foo.C")
		if err != nil {
			t.Fatal(err)
		}

		// sub.Drain() or nc.Drain() delete JS consumer, same than Unsubscribe()
		nc.Drain()
		<-ctx.Done()
		fetchConsumers(t, 0)
	})

	jsm.Publish("foo.A", []byte("A.1"))
	jsm.Publish("foo.B", []byte("B.1"))
	jsm.Publish("foo.C", []byte("C.1"))

	t.Run("conn close does not delete any consumer", func(t *testing.T) {
		nc, js := jsClient(t, s)
		defer nc.Close()

		if _, err := js.SubscribeSync("foo.A"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		subB, err := js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		resp, err := subB.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		got := string(resp.Data)
		expected := "B.1"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
		fetchConsumers(t, 2)

		// There will be still all consumers since nc.Close
		// does not delete ephemeral consumers.
		nc.Close()
		fetchConsumers(t, 2)
	})

	jsm.Publish("foo.A", []byte("A.2"))
	jsm.Publish("foo.B", []byte("B.2"))
	jsm.Publish("foo.C", []byte("C.2"))

	t.Run("reattached durables consumers cannot be deleted with unsubscribe", func(t *testing.T) {
		nc, js := jsClient(t, s)
		defer nc.Close()

		fetchConsumers(t, 2)

		// The durable interest remains so have to attach now,
		// otherwise would get a stream already used error.
		subB, err := js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}

		// No new consumers created since reattached to the same one.
		fetchConsumers(t, 2)

		resp, err := subB.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		got := string(resp.Data)
		expected := "B.2"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}

		jsm.Publish("foo.B", []byte("B.3"))

		// Sub can still receive the same message.
		resp, err = subB.NextMsg(1 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		got = string(resp.Data)
		expected = "B.3"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
		// Delete durable consumer.
		err = subB.Unsubscribe()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		// Since library did not create, the JS consumers remain.
		fetchConsumers(t, 2)
	})
}

func TestJetStream_UnsubscribeDeleteNoPermissions(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		no_auth_user: guest
		accounts: {
			JS: {   # User should not be able to delete consumer.
				jetstream: enabled
				users: [ {user: guest, password: "", permissions: {
					publish: { deny: "$JS.API.CONSUMER.DELETE.>" }
				}}]
			}
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	errCh := make(chan error, 2)
	nc, err := nats.Connect(s.ClientURL(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errCh <- err
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	js.AddStream(&nats.StreamConfig{
		Name: "foo",
	})
	js.Publish("foo", []byte("test"))

	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}

	_, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Should fail due to lack of permissions.
	err = sub.Unsubscribe()
	if err == nil {
		t.Errorf("Unexpected success attempting to delete consumer without permissions")
	}

	select {
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for permissions error")
	case err = <-errCh:
		if !strings.Contains(err.Error(), `Permissions Violation for Publish to "$JS.API.CONSUMER.DELETE`) {
			t.Error("Expected permissions violation error")
		}
	}
}

func TestJetStreamSubscribe_ReplayPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	i := 0
	totalMsgs := 10
	for range time.NewTicker(100 * time.Millisecond).C {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish("foo", []byte(payload))
		i++

		if i == totalMsgs {
			break
		}
	}

	// By default it is ReplayInstant playback policy.
	isub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, err := isub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Config.ReplayPolicy != nats.ReplayInstantPolicy {
		t.Fatalf("Expected original replay policy, got: %v", ci.Config.ReplayPolicy)
	}

	// Change into original playback.
	sub, err := js.SubscribeSync("foo", nats.ReplayOriginal())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Config.ReplayPolicy != nats.ReplayOriginalPolicy {
		t.Fatalf("Expected original replay policy, got: %v", ci.Config.ReplayPolicy)
	}

	// There should already be a message delivered.
	_, err = sub.NextMsg(10 * time.Millisecond)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We should timeout faster since too soon for the original playback.
	_, err = sub.NextMsg(10 * time.Millisecond)
	if err != nats.ErrTimeout {
		t.Fatalf("Expected timeout error replaying the stream, got: %v", err)
	}

	// Enough time to get the next message according to the original playback.
	_, err = sub.NextMsg(110 * time.Millisecond)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamSubscribe_RateLimit(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	totalMsgs := 2048
	for i := 0; i < totalMsgs; i++ {
		payload := strings.Repeat("A", 1024)
		js.Publish("foo", []byte(payload))
	}

	// By default there is no RateLimit
	isub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, err := isub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Config.RateLimit != 0 {
		t.Fatalf("Expected no rate limit, got: %v", ci.Config.RateLimit)
	}

	// Change rate limit.
	// Make the receive channel able to possibly hold ALL messages, but
	// we expect it to hold less due to rate limiting.
	recvd := make(chan *nats.Msg, totalMsgs)
	duration := 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var rl uint64 = 1024
	sub, err := js.Subscribe("foo", func(m *nats.Msg) {
		recvd <- m

		if len(recvd) == totalMsgs {
			cancel()
		}

	}, nats.RateLimit(rl))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Config.RateLimit != rl {
		t.Fatalf("Expected %v, got: %v", rl, ci.Config.RateLimit)
	}
	<-ctx.Done()

	if len(recvd) >= int(rl) {
		t.Errorf("Expected applied rate limit to push consumer, got %v msgs in %v", recvd, duration)
	}
}

func TestJetStreamSubscribe_FilterSubjects(t *testing.T) {
	tests := []struct {
		name    string
		durable string
	}{
		{
			name: "ephemeral consumer",
		},
		{
			name:    "durable consumer",
			durable: "cons",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			nc, js := jsClient(t, s)
			defer nc.Close()

			var err error

			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo", "bar", "baz"},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for i := 0; i < 5; i++ {
				js.Publish("foo", []byte("msg"))
			}
			for i := 0; i < 5; i++ {
				js.Publish("bar", []byte("msg"))
			}
			for i := 0; i < 5; i++ {
				js.Publish("baz", []byte("msg"))
			}

			opts := []nats.SubOpt{nats.BindStream("TEST"), nats.ConsumerFilterSubjects("foo", "baz")}
			if test.durable != "" {
				opts = append(opts, nats.Durable(test.durable))
			}
			sub, err := js.SubscribeSync("", opts...)
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}

			for i := 0; i < 10; i++ {
				msg, err := sub.NextMsg(500 * time.Millisecond)
				if err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
				if msg.Subject != "foo" && msg.Subject != "baz" {
					t.Fatalf("Unexpected message subject: %s", msg.Subject)
				}
			}
		})
	}

}

func TestJetStreamSubscribe_ConfigCantChange(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range []struct {
		name   string
		first  nats.SubOpt
		second nats.SubOpt
	}{
		{"description", nats.Description("a"), nats.Description("b")},
		{"deliver policy", nats.DeliverAll(), nats.DeliverLast()},
		{"optional start sequence", nats.StartSequence(1), nats.StartSequence(10)},
		{"optional start time", nats.StartTime(time.Now()), nats.StartTime(time.Now().Add(-2 * time.Hour))},
		{"ack wait", nats.AckWait(10 * time.Second), nats.AckWait(15 * time.Second)},
		{"max deliver", nats.MaxDeliver(3), nats.MaxDeliver(5)},
		{"replay policy", nats.ReplayOriginal(), nats.ReplayInstant()},
		{"max waiting", nats.PullMaxWaiting(10), nats.PullMaxWaiting(20)},
		{"max ack pending", nats.MaxAckPending(10), nats.MaxAckPending(20)},
	} {
		t.Run(test.name, func(t *testing.T) {
			durName := nuid.Next()
			sub, err := js.PullSubscribe("foo", durName, test.first)
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			// Once it is created, options can't be changed.
			_, err = js.PullSubscribe("foo", durName, test.second)
			if err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub.Unsubscribe()
		})
	}

	for _, test := range []struct {
		name string
		cc   *nats.ConsumerConfig
		opt  nats.SubOpt
	}{
		{"ack policy", &nats.ConsumerConfig{AckPolicy: nats.AckAllPolicy}, nats.AckNone()},
		{"rate limit", &nats.ConsumerConfig{RateLimit: 10}, nats.RateLimit(100)},
		{"flow control", &nats.ConsumerConfig{FlowControl: false}, nats.EnableFlowControl()},
		{"heartbeat", &nats.ConsumerConfig{Heartbeat: 10 * time.Second}, nats.IdleHeartbeat(20 * time.Second)},
	} {
		t.Run(test.name, func(t *testing.T) {
			durName := nuid.Next()

			cc := test.cc
			cc.Durable = durName
			cc.DeliverSubject = nuid.Next()
			if _, err := js.AddConsumer("TEST", cc); err != nil {
				t.Fatalf("Error creating consumer: %v", err)
			}

			sub, err := js.SubscribeSync("foo", nats.Durable(durName), test.opt)
			if err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub.Unsubscribe()
		})
	}

	// Verify that we don't fail if user did not set it.
	for _, test := range []struct {
		name string
		opt  nats.SubOpt
	}{
		{"description", nats.Description("a")},
		{"deliver policy", nats.DeliverAll()},
		{"optional start sequence", nats.StartSequence(10)},
		{"optional start time", nats.StartTime(time.Now())},
		{"ack wait", nats.AckWait(10 * time.Second)},
		{"max deliver", nats.MaxDeliver(3)},
		{"replay policy", nats.ReplayOriginal()},
		{"max waiting", nats.PullMaxWaiting(10)},
		{"max ack pending", nats.MaxAckPending(10)},
	} {
		t.Run(test.name+" not set", func(t *testing.T) {
			durName := nuid.Next()
			sub, err := js.PullSubscribe("foo", durName, test.opt)
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			// If not explicitly asked by the user, we are ok
			_, err = js.PullSubscribe("foo", durName)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub.Unsubscribe()
		})
	}

	for _, test := range []struct {
		name string
		opt  nats.SubOpt
	}{
		{"default deliver policy", nats.DeliverAll()},
		{"default ack wait", nats.AckWait(30 * time.Second)},
		{"default replay policy", nats.ReplayInstant()},
		{"default max waiting", nats.PullMaxWaiting(512)},
		{"default ack pending", nats.MaxAckPending(65536)},
	} {
		t.Run(test.name, func(t *testing.T) {
			durName := nuid.Next()
			sub, err := js.PullSubscribe("foo", durName)
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			// If the option is the same as the server default, it is not an error either.
			_, err = js.PullSubscribe("foo", durName, test.opt)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub.Unsubscribe()
		})
	}

	for _, test := range []struct {
		name string
		opt  nats.SubOpt
	}{
		{"policy", nats.DeliverNew()},
		{"ack wait", nats.AckWait(31 * time.Second)},
		{"replay policy", nats.ReplayOriginal()},
		{"max waiting", nats.PullMaxWaiting(513)},
		{"ack pending", nats.MaxAckPending(2)},
	} {
		t.Run(test.name+" changed from default", func(t *testing.T) {
			durName := nuid.Next()
			sub, err := js.PullSubscribe("foo", durName)
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			// First time it was created with defaults and the
			// second time a change is attempted, so it is an error.
			_, err = js.PullSubscribe("foo", durName, test.opt)
			if err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub.Unsubscribe()
		})
	}

	// Check that binding to a durable (without specifying durable option) works
	if _, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "BindDurable",
		DeliverSubject: "bar",
	}); err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	if _, err := js.SubscribeSync("foo", nats.Bind("TEST", "BindDurable")); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
}

type jsServer struct {
	*server.Server
	myopts  *server.Options
	restart sync.Mutex
}

// Restart can be used to start again a server
// using the same listen address as before.
func (srv *jsServer) Restart() {
	srv.restart.Lock()
	defer srv.restart.Unlock()
	srv.Server = RunServerWithOptions(srv.myopts)
}

func setupJSClusterWithSize(t *testing.T, clusterName string, size int) []*jsServer {
	t.Helper()
	nodes := make([]*jsServer, size)
	opts := make([]*server.Options, 0)

	var activeListeners []net.Listener
	getAddr := func(t *testing.T) (string, string, int) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		addr := l.Addr()
		host := addr.(*net.TCPAddr).IP.String()
		port := addr.(*net.TCPAddr).Port
		time.Sleep(100 * time.Millisecond)

		// we cannot close the listener immediately to avoid duplicate port binding
		// the returned net.Listener has to be closed after all ports are drawn
		activeListeners = append(activeListeners, l)
		return addr.String(), host, port
	}

	routes := []string{}
	for i := 0; i < size; i++ {
		o := natsserver.DefaultTestOptions
		o.JetStream = true
		o.ServerName = fmt.Sprintf("NODE_%d", i)
		tdir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("%s_%s-", o.ServerName, clusterName))
		if err != nil {
			t.Fatal(err)
		}
		o.StoreDir = tdir

		if size > 1 {
			o.Cluster.Name = clusterName
			_, host1, port1 := getAddr(t)
			o.Host = host1
			o.Port = port1

			addr2, host2, port2 := getAddr(t)
			o.Cluster.Host = host2
			o.Cluster.Port = port2
			o.Tags = []string{o.ServerName}
			routes = append(routes, fmt.Sprintf("nats://%s", addr2))
		}
		opts = append(opts, &o)
	}
	// close all connections used to randomize ports
	for _, l := range activeListeners {
		l.Close()
	}

	if size > 1 {
		routesStr := server.RoutesFromStr(strings.Join(routes, ","))

		for i, o := range opts {
			o.Routes = routesStr
			nodes[i] = &jsServer{Server: RunServerWithOptions(o), myopts: o}
		}
	} else {
		o := opts[0]
		nodes[0] = &jsServer{Server: RunServerWithOptions(o), myopts: o}
	}

	// Wait until JS is ready.
	srvA := nodes[0]
	nc, err := nats.Connect(srvA.ClientURL())
	if err != nil {
		t.Error(err)
	}
	waitForJSReady(t, nc)
	nc.Close()

	return nodes
}

func withJSServer(t *testing.T, tfn func(t *testing.T, srvs ...*jsServer)) {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.LameDuckDuration = 3 * time.Second
	opts.LameDuckGracePeriod = 2 * time.Second
	s := &jsServer{Server: RunServerWithOptions(&opts), myopts: &opts}
	defer shutdownJSServerAndRemoveStorage(t, s.Server)
	tfn(t, s)
}

func withJSCluster(t *testing.T, clusterName string, size int, tfn func(t *testing.T, srvs ...*jsServer)) {
	t.Helper()

	nodes := setupJSClusterWithSize(t, clusterName, size)
	defer func() {
		// Ensure that they get shutdown and remove their state.
		for _, node := range nodes {
			node.restart.Lock()
			shutdownJSServerAndRemoveStorage(t, node.Server)
			node.restart.Unlock()
		}
	}()
	tfn(t, nodes...)
}

func withJSClusterAndStream(t *testing.T, clusterName string, size int, stream *nats.StreamConfig, tfn func(t *testing.T, subject string, srvs ...*jsServer)) {
	t.Helper()

	withJSCluster(t, clusterName, size, func(t *testing.T, nodes ...*jsServer) {
		srvA := nodes[0]
		nc, err := nats.Connect(srvA.ClientURL())
		if err != nil {
			t.Error(err)
		}
		defer nc.Close()

		timeout := time.Now().Add(10 * time.Second)
		for time.Now().Before(timeout) {
			jsm, err := nc.JetStream()
			if err != nil {
				t.Fatal(err)
			}
			_, err = jsm.AccountInfo()
			if err != nil {
				// Backoff for a bit until cluster and resources are ready.
				time.Sleep(500 * time.Millisecond)
			}

			_, err = jsm.AddStream(stream)
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		tfn(t, stream.Name, nodes...)
	})
}

func waitForJSReady(t *testing.T, nc *nats.Conn) {
	var err error
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		// Use a smaller MaxWait here since if it fails, we don't want
		// to wait for too long since we are going to try again.
		js, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		_, err = js.AccountInfo()
		if err != nil {
			continue
		}
		return
	}
	t.Fatalf("Timeout waiting for JS to be ready: %v", err)
}

func checkFor(t *testing.T, totalWait, sleepDur time.Duration, f func() error) {
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

func checkSubsPending(t *testing.T, sub *nats.Subscription, numExpected int) {
	t.Helper()
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
		}
		return nil
	})
}

func TestJetStreamStreamMirror(t *testing.T) {
	withJSServer(t, testJetStreamMirror_Source)
}

func testJetStreamMirror_Source(t *testing.T, nodes ...*jsServer) {
	srvA := nodes[0]
	nc, js := jsClient(t, srvA.Server)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "origin",
		Placement: &nats.Placement{
			Tags: []string{"NODE_0"},
		},
		Storage:  nats.MemoryStorage,
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	totalMsgs := 10
	for i := 0; i < totalMsgs; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish("origin", []byte(payload))
	}

	t.Run("create mirrors", func(t *testing.T) {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "m1",
			Mirror:   &nats.StreamSource{Name: "origin"},
			Storage:  nats.FileStorage,
			Replicas: 1,
		})
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "m2",
			Mirror:   &nats.StreamSource{Name: "origin"},
			Storage:  nats.MemoryStorage,
			Replicas: 1,
		})
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}
		msgs := make([]*nats.RawStreamMsg, 0)

		// Stored message sequences start at 1
		startSequence := 1

	GetNextMsg:
		for i := startSequence; i < totalMsgs+1; i++ {
			var (
				err       error
				seq       = uint64(i)
				msgA      *nats.RawStreamMsg
				msgB      *nats.RawStreamMsg
				sourceMsg *nats.RawStreamMsg
				timeout   = time.Now().Add(2 * time.Second)
			)

			for time.Now().Before(timeout) {
				sourceMsg, err = js.GetMsg("origin", seq)
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				msgA, err = js.GetMsg("m1", seq)
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if !reflect.DeepEqual(sourceMsg, msgA) {
					t.Errorf("Expected %+v, got: %+v", sourceMsg, msgA)
				}

				msgB, err = js.GetMsg("m2", seq)
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if !reflect.DeepEqual(sourceMsg, msgB) {
					t.Errorf("Expected %+v, got: %+v", sourceMsg, msgB)
				}

				msgs = append(msgs, msgA)
				continue GetNextMsg
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		got := len(msgs)
		if got < totalMsgs {
			t.Errorf("Expected %v, got: %v", totalMsgs, got)
		}

		t.Run("consume from mirror", func(t *testing.T) {
			sub, err := js.SubscribeSync("origin", nats.BindStream("m1"))
			if err != nil {
				t.Fatal(err)
			}

			mmsgs := make([]*nats.Msg, 0)
			for i := 0; i < totalMsgs; i++ {
				msg, err := sub.NextMsg(2 * time.Second)
				if err != nil {
					t.Error(err)
				}
				meta, err := msg.Metadata()
				if err != nil {
					t.Error(err)
				}
				if meta.Stream != "m1" {
					t.Errorf("Expected m1, got: %v", meta.Stream)
				}
				mmsgs = append(mmsgs, msg)
			}
			if len(mmsgs) != totalMsgs {
				t.Errorf("Expected to consume %v msgs, got: %v", totalMsgs, len(mmsgs))
			}
		})
	})

	t.Run("consume from original source", func(t *testing.T) {
		sub, err := js.SubscribeSync("origin")
		defer sub.Unsubscribe()
		if err != nil {
			t.Fatal(err)
		}
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Error(err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			t.Error(err)
		}
		if meta.Stream != "origin" {
			t.Errorf("Expected m1, got: %v", meta.Stream)
		}
	})

	t.Run("bind to non existing stream fails", func(t *testing.T) {
		_, err := js.SubscribeSync("origin", nats.BindStream("foo"))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if !errors.Is(err, nats.ErrStreamNotFound) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrStreamNotFound, err)
		}
	})

	t.Run("bind to origin stream", func(t *testing.T) {
		// This would only avoid the stream names lookup.
		sub, err := js.SubscribeSync("origin", nats.BindStream("origin"))
		if err != nil {
			t.Fatal(err)
		}
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Error(err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			t.Error(err)
		}
		if meta.Stream != "origin" {
			t.Errorf("Expected m1, got: %v", meta.Stream)
		}
	})

	t.Run("get mirror info", func(t *testing.T) {
		m1, err := js.StreamInfo("m1")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got := m1.Mirror.Name
		expected := "origin"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}

		m2, err := js.StreamInfo("m2")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got = m2.Mirror.Name
		expected = "origin"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
	})

	t.Run("create stream from sources", func(t *testing.T) {
		sources := make([]*nats.StreamSource, 0)
		sources = append(sources, &nats.StreamSource{Name: "m1"})
		sources = append(sources, &nats.StreamSource{Name: "m2"})
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "s1",
			Sources:  sources,
			Storage:  nats.FileStorage,
			Replicas: 1,
		})
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		msgs := make([]*nats.RawStreamMsg, 0)

		// Stored message sequences start at 1
		startSequence := 1
		expectedTotal := totalMsgs * 2

	GetNextMsg:
		for i := startSequence; i < expectedTotal+1; i++ {
			var (
				err     error
				seq     = uint64(i)
				msg     *nats.RawStreamMsg
				timeout = time.Now().Add(5 * time.Second)
			)

		Retry:
			for time.Now().Before(timeout) {
				msg, err = js.GetMsg("s1", seq)
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					continue Retry
				}
				msgs = append(msgs, msg)
				continue GetNextMsg
			}
			if err != nil {
				t.Fatalf("Unexpected error fetching seq=%v: %v", seq, err)
			}
		}

		got := len(msgs)
		if got < expectedTotal {
			t.Errorf("Expected %v, got: %v", expectedTotal, got)
		}

		si, err := js.StreamInfo("s1")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got = int(si.State.Msgs)
		if got != expectedTotal {
			t.Errorf("Expected %v, got: %v", expectedTotal, got)
		}

		got = len(si.Sources)
		expected := 2
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}

		t.Run("consume from sourced stream", func(t *testing.T) {
			sub, err := js.SubscribeSync("origin", nats.BindStream("s1"))
			if err != nil {
				t.Error(err)
			}
			_, err = sub.NextMsg(2 * time.Second)
			if err != nil {
				t.Error(err)
			}
		})
	})

	t.Run("update stream with sources", func(t *testing.T) {
		si, err := js.StreamInfo("s1")
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}
		got := len(si.Config.Sources)
		expected := 2
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}

		got = len(si.Sources)
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}

		// Make an update
		config := si.Config
		config.MaxMsgs = 128
		updated, err := js.UpdateStream(&config)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		got = len(updated.Config.Sources)
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}

		got = int(updated.Config.MaxMsgs)
		expected = int(config.MaxMsgs)
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
	})

	t.Run("bind to stream with subject not in stream", func(t *testing.T) {
		// The Stream does not have a subject called 'nothing' but client is still able
		// to bind to the origin stream even though it cannot consume messages.
		// After updating the stream with the subject this consumer will be able to
		// match and receive messages.
		sub, err := js.SubscribeSync("nothing", nats.BindStream("origin"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = sub.NextMsg(1 * time.Second)
		if !errors.Is(err, nats.ErrTimeout) {
			t.Fatal("Expected timeout error")
		}

		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		got := info.Stream
		expected := "origin"
		if got != expected {
			t.Fatalf("Expected %v, got %v", expected, got)
		}

		got = info.Config.FilterSubject
		expected = "nothing"
		if got != expected {
			t.Fatalf("Expected %v, got %v", expected, got)
		}

		t.Run("can consume after stream update", func(t *testing.T) {
			_, err = js.UpdateStream(&nats.StreamConfig{
				Name: "origin",
				Placement: &nats.Placement{
					Tags: []string{"NODE_0"},
				},
				Storage:  nats.MemoryStorage,
				Replicas: 1,
				Subjects: []string{"origin", "nothing"},
			})
			js.Publish("nothing", []byte("hello world"))

			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Error(err)
			}
			got = msg.Subject
			expected = "nothing"
			if got != expected {
				t.Fatalf("Expected %v, got %v", expected, got)
			}

		})
	})

	t.Run("create sourced stream with a cycle", func(t *testing.T) {
		// Since v2.8.0, this test would fail with a "detected cycle" error.
		sources := make([]*nats.StreamSource, 0)
		sources = append(sources, &nats.StreamSource{Name: "origin"})
		sources = append(sources, &nats.StreamSource{Name: "m1"})
		streamName := "s2"
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Sources:  sources,
			Storage:  nats.FileStorage,
			Replicas: 1,
		})
		var aerr *nats.APIError
		if ok := errors.As(err, &aerr); !ok || aerr.ErrorCode != nats.JSStreamInvalidConfig {
			t.Fatalf("Expected nats.APIError, got %v", err)
		}
	})
}

func TestJetStream_ClusterMultipleSubscribe(t *testing.T) {
	nodes := []int{1, 3}
	replicas := []int{1}

	for _, n := range nodes {
		for _, r := range replicas {
			if r > 1 && n == 1 {
				continue
			}

			t.Run(fmt.Sprintf("qsub n=%d r=%d", n, r), func(t *testing.T) {
				name := fmt.Sprintf("MSUB%d%d", n, r)
				stream := &nats.StreamConfig{
					Name:     name,
					Replicas: r,
				}
				withJSClusterAndStream(t, name, n, stream, testJetStream_ClusterMultipleQueueSubscribe)
			})

			t.Run(fmt.Sprintf("psub n=%d r=%d", n, r), func(t *testing.T) {
				name := fmt.Sprintf("PSUBN%d%d", n, r)
				stream := &nats.StreamConfig{
					Name:     name,
					Replicas: n,
				}
				withJSClusterAndStream(t, name, n, stream, testJetStream_ClusterMultiplePullSubscribe)
			})

			t.Run(fmt.Sprintf("psub n=%d r=%d multi fetch", n, r), func(t *testing.T) {
				name := fmt.Sprintf("PFSUBN%d%d", n, r)
				stream := &nats.StreamConfig{
					Name:     name,
					Replicas: n,
				}
				withJSClusterAndStream(t, name, n, stream, testJetStream_ClusterMultipleFetchPullSubscribe)
			})
		}
	}
}

func testJetStream_ClusterMultipleQueueSubscribe(t *testing.T, subject string, srvs ...*jsServer) {
	srv := srvs[0]
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	var wg sync.WaitGroup
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	size := 5
	subs := make([]*nats.Subscription, size)
	errCh := make(chan error, size)

	// We are testing auto-bind here so create one first and expect others to bind to it.
	sub, err := js.QueueSubscribeSync(subject, "wq", nats.Durable("shared"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	subs[0] = sub

	for i := 1; i < size; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var sub *nats.Subscription
			var err error
			for attempt := 0; attempt < 5; attempt++ {
				sub, err = js.QueueSubscribeSync(subject, "wq", nats.Durable("shared"))
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}
				break
			}
			if err != nil {
				errCh <- err
			} else {
				subs[n] = sub
			}
		}(i)
	}

	go func() {
		// Unblock the main context when done.
		wg.Wait()
		done()
	}()

	wg.Wait()
	for i := 0; i < size*2; i++ {
		js.Publish(subject, []byte("test"))
	}

	delivered := 0
	for _, sub := range subs {
		if sub == nil {
			continue
		}
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs > 0 {
			delivered++
		}
	}
	if delivered < 2 {
		t.Fatalf("Expected more than one subscriber to receive a message, got: %v", delivered)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Unexpected error with multiple queue subscribers: %v", err)
		}
	}
}

func testJetStream_ClusterMultiplePullSubscribe(t *testing.T, subject string, srvs ...*jsServer) {
	srv := srvs[0]
	nc, js := jsClient(t, srv.Server)
	defer nc.Close()

	var wg sync.WaitGroup
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()

	size := 5
	subs := make([]*nats.Subscription, size)
	errCh := make(chan error, size)
	for i := 0; i < size; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var sub *nats.Subscription
			var err error
			for attempt := 0; attempt < 5; attempt++ {
				sub, err = js.PullSubscribe(subject, "shared")
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}
				break
			}
			if err != nil {
				errCh <- err
			} else {
				subs[n] = sub
			}
		}(i)
	}

	go func() {
		// Unblock the main context when done.
		wg.Wait()
		done()
	}()

	wg.Wait()
	for i := 0; i < size*2; i++ {
		js.Publish(subject, []byte("test"))
	}

	delivered := 0
	for i, sub := range subs {
		if sub == nil {
			continue
		}
		for attempt := 0; attempt < 4; attempt++ {
			_, err := sub.Fetch(1, nats.MaxWait(250*time.Millisecond))
			if err != nil {
				t.Logf("%v WARN: Timeout waiting for next message: %v", i, err)
				continue
			}
			delivered++
			break
		}
	}

	if delivered < 2 {
		t.Fatalf("Expected more than one subscriber to receive a message, got: %v", delivered)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Unexpected error with multiple pull subscribers: %v", err)
		}
	}
}

func testJetStream_ClusterMultipleFetchPullSubscribe(t *testing.T, subject string, srvs ...*jsServer) {
	srv := srvs[0]
	nc, js := jsClient(t, srv.Server)
	defer nc.Close()

	var wg sync.WaitGroup
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()

	// Setup a number of subscriptions with different inboxes that will be
	// fetching the messages in parallel.
	nsubs := 4
	subs := make([]*nats.Subscription, nsubs)
	errCh := make(chan error, nsubs)
	var queues sync.Map
	for i := 0; i < nsubs; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var sub *nats.Subscription
			var err error
			sub, err = js.PullSubscribe(subject, "shared")
			if err != nil {
				errCh <- err
			} else {
				subs[n] = sub
				queues.Store(sub.Subject, make([]*nats.Msg, 0))
			}
		}(i)
	}

	// Publishing of messages happens after the subscriptions are ready.
	// The subscribers will be fetching messages while these are being
	// produced so sometimes there are not going to be messages available.
	wg.Wait()
	var (
		total     uint64 = 100
		delivered uint64
		batchSize = 2
	)
	go func() {
		for i := 0; i < int(total); i++ {
			js.Publish(subject, []byte(fmt.Sprintf("n:%v", i)))
			time.Sleep(1 * time.Millisecond)
		}
	}()

	ctx2, done2 := context.WithTimeout(ctx, 3*time.Second)
	defer done2()

	for _, psub := range subs {
		if psub == nil {
			continue
		}
		sub := psub
		subject := sub.Subject
		v, _ := queues.Load(sub.Subject)
		queue := v.([]*nats.Msg)
		go func() {
			for {
				select {
				case <-ctx2.Done():
					return
				default:
				}

				if current := atomic.LoadUint64(&delivered); current >= total {
					done2()
					return
				}

				// Wait until all messages have been consumed.
				for attempt := 0; attempt < 4; attempt++ {
					recvd, err := sub.Fetch(batchSize, nats.MaxWait(1*time.Second))
					if err != nil {
						if err == nats.ErrConnectionClosed {
							return
						}
						current := atomic.LoadUint64(&delivered)
						if current >= total {
							done2()
							return
						} else {
							t.Logf("WARN: Timeout waiting for next message: %v", err)
						}
						continue
					}
					for _, msg := range recvd {
						queue = append(queue, msg)
						queues.Store(subject, queue)
					}
					atomic.AddUint64(&delivered, uint64(len(recvd)))
					break
				}
			}
		}()
	}

	// Wait until context is canceled after receiving all messages.
	<-ctx2.Done()

	if delivered < total {
		t.Fatalf("Expected %v, got: %v", total, delivered)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Unexpected error with multiple pull subscribers: %v", err)
		}
	}

	var (
		gotNoMessages bool
		count         = 0
	)
	queues.Range(func(k, v any) bool {
		msgs := v.([]*nats.Msg)
		count += len(msgs)

		if len(msgs) == 0 {
			gotNoMessages = true
			return false
		}
		return true
	})

	if gotNoMessages {
		t.Error("Expected all pull subscribers to receive some messages")
	}
}

func TestJetStream_ClusterReconnect(t *testing.T) {
	t.Skip("This test need to be revisited")
	n := 3
	replicas := []int{1, 3}

	t.Run("pull sub", func(t *testing.T) {
		for _, r := range replicas {
			t.Run(fmt.Sprintf("n=%d r=%d", n, r), func(t *testing.T) {
				stream := &nats.StreamConfig{
					Name:     fmt.Sprintf("foo-qr%d", r),
					Replicas: r,
				}
				withJSClusterAndStream(t, fmt.Sprintf("QPULLR%d", r), n, stream, testJetStream_ClusterReconnectPullQueueSubscriber)
			})
		}
	})

	t.Run("sub durable", func(t *testing.T) {
		for _, r := range replicas {
			t.Run(fmt.Sprintf("n=%d r=%d", n, r), func(t *testing.T) {
				stream := &nats.StreamConfig{
					Name:     fmt.Sprintf("quux-r%d", r),
					Replicas: r,
				}
				withJSClusterAndStream(t, fmt.Sprintf("SUBR%d", r), n, stream, testJetStream_ClusterReconnectDurablePushSubscriber)
			})
		}
	})

	t.Run("qsub durable", func(t *testing.T) {
		for _, r := range replicas {
			t.Run(fmt.Sprintf("n=%d r=%d", n, r), func(t *testing.T) {
				stream := &nats.StreamConfig{
					Name:     fmt.Sprintf("bar-r%d", r),
					Replicas: r,
				}
				withJSClusterAndStream(t, fmt.Sprintf("QSUBR%d", r), n, stream, testJetStream_ClusterReconnectDurableQueueSubscriber)
			})
		}
	})
}

func testJetStream_ClusterReconnectDurableQueueSubscriber(t *testing.T, subject string, srvs ...*jsServer) {
	var (
		srvA          = srvs[0]
		totalMsgs     = 20
		reconnected   = make(chan struct{})
		reconnectDone bool
	)
	nc, err := nats.Connect(srvA.ClientURL(),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			reconnected <- struct{}{}

			// Bring back the server after the reconnect event.
			if !reconnectDone {
				reconnectDone = true
				srvA.Restart()
			}
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			t.Logf("WARN: Got error %v", err)
			if info, ok := err.(*nats.ErrConsumerSequenceMismatch); ok {
				t.Logf("WARN: %+v", info)
			}
			// Take out this QueueSubscriber from the group.
			sub.Drain()
		}),
	)
	if err != nil {
		t.Error(err)
	}

	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish(subject, []byte(payload))
	}

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	msgs := make(chan *nats.Msg, totalMsgs)

	// Create some queue subscribers.
	for i := 0; i < 5; i++ {
		expected := totalMsgs
		dname := "dur"
		_, err = js.QueueSubscribe(subject, "wg", func(m *nats.Msg) {
			msgs <- m

			count := len(msgs)
			switch {
			case count == 2:
				// Do not ack and wait for redelivery on reconnect.
				srvA.Shutdown()
				srvA.WaitForShutdown()
				return
			case count == expected:
				done()
			}

			err := m.AckSync()
			if err != nil {
				// During the reconnection, both of these errors can occur.
				if err == nats.ErrNoResponders || err == nats.ErrTimeout {
					// Wait for reconnection event to occur to continue.
					select {
					case <-reconnected:
						return
					case <-time.After(1 * time.Second):
						return
					case <-ctx.Done():
						return
					}
				}
			}
		}, nats.Durable(dname), nats.AckWait(5*time.Second), nats.ManualAck(), nats.IdleHeartbeat(100*time.Millisecond))

		if err != nil && (err != nats.ErrTimeout && err != context.DeadlineExceeded) {
			t.Error(err)
		}
	}

	// Check for persisted messages, this could fail a few times.
	var stream *nats.StreamInfo
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		stream, err = js.StreamInfo(subject)
		if err == nats.ErrTimeout {
			time.Sleep(100 * time.Millisecond)
			continue
		} else if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		break
	}
	if stream == nil {
		t.Logf("WARN: Failed to get stream info: %v", err)
	}

	var failedPubs int
	for i := 10; i < totalMsgs; i++ {
		var published bool
		payload := fmt.Sprintf("i:%d", i)
		timeout = time.Now().Add(5 * time.Second)

	Retry:
		for time.Now().Before(timeout) {
			_, err = js.Publish(subject, []byte(payload))

			// Skip temporary errors.
			if err != nil && (err == nats.ErrNoStreamResponse || err == nats.ErrTimeout || err.Error() == `raft: not leader`) {
				time.Sleep(100 * time.Millisecond)
				continue Retry
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			published = true
			break Retry
		}

		if !published {
			failedPubs++
		}
	}

	<-ctx.Done()

	// Wait a bit to get heartbeats.
	time.Sleep(2 * time.Second)

	// Drain to allow AckSync response to be received.
	nc.Drain()

	got := len(msgs)
	if got != totalMsgs {
		t.Logf("WARN: Expected %v, got: %v (failed publishes: %v)", totalMsgs, got, failedPubs)
	}
	if got < totalMsgs-failedPubs {
		t.Errorf("Expected %v, got: %v", totalMsgs-failedPubs, got)
	}
}

func testJetStream_ClusterReconnectDurablePushSubscriber(t *testing.T, subject string, srvs ...*jsServer) {
	var (
		srvA          = srvs[0]
		srvB          = srvs[1]
		srvC          = srvs[2]
		totalMsgs     = 20
		reconnected   = make(chan struct{})
		reconnectDone bool
	)
	nc, err := nats.Connect(srvA.ClientURL(),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			reconnected <- struct{}{}

			// Bring back the server after the reconnect event.
			if !reconnectDone {
				reconnectDone = true
				srvA.Restart()
			}
		}),
	)
	if err != nil {
		t.Error(err)
	}

	// Drain to allow Ack responses to be published.
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		t.Error(err)
	}

	// Initial burst of messages.
	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish(subject, []byte(payload))
	}

	// For now just confirm that do receive all messages across restarts.
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	recvd := make(chan *nats.Msg, totalMsgs)
	expected := totalMsgs
	_, err = js.Subscribe(subject, func(m *nats.Msg) {
		recvd <- m

		if len(recvd) == expected {
			done()
		}
	}, nats.Durable("sd1"))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	timeout := time.Now().Add(3 * time.Second)
	for time.Now().Before(timeout) {
		if len(recvd) >= 2 {
			// Restart the first server.
			srvA.Shutdown()
			break
		}
	}

	// Wait for reconnect or timeout.
	select {
	case <-reconnected:
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for reconnect")
	}

	for i := 10; i < totalMsgs; i++ {
		payload := fmt.Sprintf("i:%d", i)
		timeout := time.Now().Add(5 * time.Second)
	Retry:
		for time.Now().Before(timeout) {
			_, err = js.Publish(subject, []byte(payload))
			if err == nats.ErrNoStreamResponse || err == nats.ErrTimeout {
				// Temporary error.
				time.Sleep(100 * time.Millisecond)
				continue Retry
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			break Retry
		}
	}

	srvBClientURL := srvB.ClientURL()
	srvCClientURL := srvC.ClientURL()
	timeout = time.Now().Add(3 * time.Second)
	for time.Now().Before(timeout) {
		if len(recvd) >= 5 {
			// Do another Shutdown of the server we are connected with.
			switch nc.ConnectedUrl() {
			case srvBClientURL:
				srvB.Shutdown()
			case srvCClientURL:
				srvC.Shutdown()
			default:
			}

			break
		}
	}
	<-ctx.Done()

	got := len(recvd)
	if got != totalMsgs {
		t.Logf("WARN: Expected %v, got: %v", totalMsgs, got)
	}
}

func testJetStream_ClusterReconnectPullQueueSubscriber(t *testing.T, subject string, srvs ...*jsServer) {
	var (
		recvd         = make(map[string]int)
		recvdQ        = make(map[int][]*nats.Msg)
		srvA          = srvs[0]
		totalMsgs     = 20
		reconnected   = make(chan struct{}, 2)
		reconnectDone bool
	)
	nc, err := nats.Connect(srvA.ClientURL(),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			reconnected <- struct{}{}

			// Bring back the server after the reconnect event.
			if !reconnectDone {
				reconnectDone = true
				srvA.Restart()
			}
		}),
	)
	if err != nil {
		t.Error(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		_, err := js.Publish(subject, []byte(payload))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	subs := make([]*nats.Subscription, 0)

	for i := 0; i < 5; i++ {
		sub, err := js.PullSubscribe(subject, "d1", nats.PullMaxWaiting(5))
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	for i := 10; i < totalMsgs; i++ {
		payload := fmt.Sprintf("i:%d", i)
		_, err := js.Publish(subject, []byte(payload))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

NextMsg:
	for len(recvd) < totalMsgs {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for messages, expected: %d, got: %d", totalMsgs, len(recvd))
		default:
		}

		for qsub, sub := range subs {
			// Server will shutdown after a couple of messages which will result
			// in empty messages with an status unavailable error.
			msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
			if err == nats.ErrNoResponders || err == nats.ErrTimeout {
				// Backoff before asking for more messages.
				time.Sleep(100 * time.Millisecond)
				continue NextMsg
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
				continue NextMsg
			}
			msg := msgs[0]
			if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
				t.Fatal("Got 503 JetStream API message!")
			}

			recvd[string(msg.Data)]++
			recvdQ[qsub] = append(recvdQ[qsub], msg)

			// Add a few retries since there can be errors during the reconnect.
			timeout := time.Now().Add(5 * time.Second)
		RetryAck:
			for time.Now().Before(timeout) {
				err = msg.AckSync()
				if err != nil {
					// During the reconnection, both of these errors can occur.
					if err == nats.ErrNoResponders || err == nats.ErrTimeout {
						// Wait for reconnection event to occur to continue.
						select {
						case <-reconnected:
							continue RetryAck
						case <-time.After(100 * time.Millisecond):
							continue RetryAck
						case <-ctx.Done():
							t.Fatal("Timed out waiting for reconnect")
						}
					}

					t.Errorf("Unexpected error: %v", err)
					continue RetryAck
				}
				break RetryAck
			}

			// Shutdown the server after a couple of messages.
			if len(recvd) == 2 {
				srvA.Shutdown()
			}
		}
	}

	// Confirm the number of messages.
	for i := 0; i < totalMsgs; i++ {
		msg := fmt.Sprintf("i:%d", i)
		count, ok := recvd[msg]
		if !ok {
			t.Errorf("Missing message %v", msg)
		} else if count != 1 {
			t.Logf("WARN: Expected to receive a single message, got: %v", count)
		}
	}

	// Expect all qsubs to receive at least a message.
	for _, msgs := range recvdQ {
		if len(msgs) < 1 {
			t.Errorf("Expected queue sub to receive at least one message")
		}
	}
}

func TestJetStreamPullSubscribeOptions(t *testing.T) {
	withJSCluster(t, "FOPTS", 3, testJetStreamFetchOptions)
}

func testJetStreamFetchOptions(t *testing.T, srvs ...*jsServer) {
	srv := srvs[0]
	nc, js := jsClient(t, srv.Server)
	defer nc.Close()

	var err error

	subject := "WQ"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     subject,
		Replicas: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	sendMsgs := func(t *testing.T, totalMsgs int) {
		t.Helper()
		for i := 0; i < totalMsgs; i++ {
			payload := fmt.Sprintf("i:%d", i)
			_, err := js.Publish(subject, []byte(payload))
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		}
	}

	t.Run("max request batch", func(t *testing.T) {
		defer js.PurgeStream(subject)

		sub, err := js.PullSubscribe(subject, "max-request-batch", nats.MaxRequestBatch(2))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if _, err := sub.Fetch(10); err == nil || !strings.Contains(err.Error(), "MaxRequestBatch of 2") {
			t.Fatalf("Expected error about max request batch size, got %v", err)
		}
	})

	t.Run("max request max bytes", func(t *testing.T) {
		defer js.PurgeStream(subject)

		sub, err := js.PullSubscribe(subject, "max-request-max-bytes", nats.MaxRequestMaxBytes(100))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		if _, err := sub.Fetch(10, nats.PullMaxBytes(200)); err == nil || !strings.Contains(err.Error(), "MaxRequestMaxBytes of 100") {
			t.Fatalf("Expected error about max request max bytes, got %v", err)
		}
	})

	t.Run("max request expires", func(t *testing.T) {
		defer js.PurgeStream(subject)

		sub, err := js.PullSubscribe(subject, "max-request-expires", nats.MaxRequestExpires(50*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if _, err := sub.Fetch(10); err == nil || !strings.Contains(err.Error(), "MaxRequestExpires of 50ms") {
			t.Fatalf("Expected error about max request expiration, got %v", err)
		}
	})

	t.Run("batch size", func(t *testing.T) {
		defer js.PurgeStream(subject)

		expected := 10
		sendMsgs(t, expected)
		sub, err := js.PullSubscribe(subject, "batch-size")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		msgs, err := sub.Fetch(expected, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}

		for _, msg := range msgs {
			msg.AckSync()
		}

		got := len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}

		// Next fetch will timeout since no more messages.
		_, err = sub.Fetch(1, nats.MaxWait(250*time.Millisecond))
		if err != nats.ErrTimeout {
			t.Errorf("Expected timeout fetching next message, got: %v", err)
		}

		expected = 5
		sendMsgs(t, expected)
		msgs, err = sub.Fetch(expected, nats.MaxWait(1*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got = len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}

		for _, msg := range msgs {
			msg.Ack()
		}
	})

	t.Run("sub drain is no op", func(t *testing.T) {
		defer js.PurgeStream(subject)

		expected := 10
		sendMsgs(t, expected)
		sub, err := js.PullSubscribe(subject, "batch-ctx")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		msgs, err := sub.Fetch(expected, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got := len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}
		err = sub.Drain()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("fetch after unsubscribe", func(t *testing.T) {
		defer js.PurgeStream(subject)

		expected := 10
		sendMsgs(t, expected)
		sub, err := js.PullSubscribe(subject, "fetch-unsub")
		if err != nil {
			t.Fatal(err)
		}

		err = sub.Unsubscribe()
		if err != nil {
			t.Fatal(err)
		}

		_, err = sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if !errors.Is(err, nats.ErrBadSubscription) {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("max waiting exceeded", func(t *testing.T) {
		defer js.PurgeStream(subject)

		_, err := js.AddConsumer(subject, &nats.ConsumerConfig{
			Durable:    "max-waiting",
			MaxWaiting: 2,
			AckPolicy:  nats.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				defer wg.Done()

				sub, err := js.PullSubscribe(subject, "max-waiting")
				if err != nil {
					return
				}
				sub.Fetch(1, nats.MaxWait(time.Second))
			}()
		}

		// Give time to those 2 above to fill the MaxWaiting
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			ci, err := js.ConsumerInfo(subject, "max-waiting")
			if err != nil {
				return err
			}
			if n := ci.NumWaiting; n != 2 {
				return fmt.Errorf("NumWaiting should be 2, was %v", n)
			}
			return nil
		})

		// Now this request should get a 409. Currently, we do not re-fetch
		// on that error, so would be visible in the error returned by Fetch()
		sub, err := js.PullSubscribe(subject, "max-waiting")
		if err != nil {
			t.Fatal(err)
		}
		_, err = sub.Fetch(1, nats.MaxWait(time.Second))
		if err == nil || !strings.Contains(err.Error(), "MaxWaiting") {
			t.Fatalf("Unexpected error: %v", err)
		}
		wg.Wait()
	})

	t.Run("no wait", func(t *testing.T) {
		defer js.PurgeStream(subject)

		expected := 10
		sendMsgs(t, expected)
		sub, err := js.PullSubscribe(subject, "no-wait")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		recvd := make([]*nats.Msg, 0)

	Loop:
		for range time.NewTicker(100 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				break Loop
			default:
			}

			msgs, err := sub.Fetch(1, nats.MaxWait(250*time.Millisecond))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			recvd = append(recvd, msgs[0])

			for _, msg := range msgs {
				err = msg.AckSync()
				if err != nil {
					t.Error(err)
				}
			}

			if len(recvd) == expected {
				done()
				break
			}
		}

		got := len(recvd)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}

		// There should only be timeout errors since no more messages.
		msgs, err := sub.Fetch(expected, nats.MaxWait(2*time.Second))
		if err == nil {
			t.Fatal("Unexpected success", len(msgs))
		}
		if err != nats.ErrTimeout {
			t.Fatalf("Expected timeout error, got: %v", err)
		}
	})
}

func TestJetStreamPublishAsync(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	// Make sure we get a proper failure when no stream is present.
	paf, err := js.PublishAsync("foo", []byte("Hello JS"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case <-paf.Ok():
		t.Fatalf("Did not expect to get PubAck")
	case err := <-paf.Err():
		if err != nats.ErrNoResponders {
			t.Fatalf("Expected a ErrNoResponders error, got %v", err)
		}
		// Should be able to get the message back to resend, etc.
		m := paf.Msg()
		if m == nil {
			t.Fatalf("Expected to be able to retrieve the message")
		}
		if m.Subject != "foo" || string(m.Data) != "Hello JS" {
			t.Fatalf("Wrong message: %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not receive an error in time")
	}

	// Now create a stream and expect a PubAck from <-OK().
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	paf, err = js.PublishAsync("TEST", []byte("Hello JS ASYNC PUB"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case pa := <-paf.Ok():
		if pa.Stream != "TEST" || pa.Sequence != 1 {
			t.Fatalf("Bad PubAck: %+v", pa)
		}
	case err := <-paf.Err():
		t.Fatalf("Did not expect to get an error: %v", err)
	case <-time.After(time.Second):
		t.Fatalf("Did not receive a PubAck in time")
	}

	errCh := make(chan error, 1)

	// Make sure we can register an async err handler for these.
	errHandler := func(js nats.JetStream, originalMsg *nats.Msg, err error) {
		if originalMsg == nil {
			t.Fatalf("Expected non-nil original message")
		}
		errCh <- err
	}

	js, err = nc.JetStream(nats.PublishAsyncErrHandler(errHandler))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nats.ErrNoResponders {
			t.Fatalf("Expected a ErrNoResponders error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not receive an async err in time")
	}

	// Now test that we can set our window for the JetStream context to limit number of outstanding messages.
	js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 100; i++ {
		if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if np := js.PublishAsyncPending(); np > 10 {
			t.Fatalf("Expected num pending to not exceed 10, got %d", np)
		}
	}

	// Now test that we can wait on all prior outstanding if we want.
	js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 500; i++ {
		if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Check invalid options
	_, err = js.PublishAsync("foo", []byte("Bad"), nats.StallWait(0))
	expectedErr := "nats: stall wait should be more than 0"
	if err == nil || err.Error() != expectedErr {
		t.Errorf("Expected %v, got: %v", expectedErr, err)
	}

	_, err = js.Publish("foo", []byte("Also bad"), nats.StallWait(200*time.Millisecond))
	expectedErr = "nats: stall wait cannot be set to sync publish"
	if err == nil || err.Error() != expectedErr {
		t.Errorf("Expected %v, got: %v", expectedErr, err)
	}

	nc, js = jsClient(t, s, nats.CustomInboxPrefix("_BOX"))
	defer nc.Close()

	paf, err = js.PublishAsync("foo", []byte("Hello JS with Custom Inbox"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case <-paf.Ok():
		t.Fatalf("Did not expect to get PubAck")
	case err := <-paf.Err():
		if err != nats.ErrNoResponders {
			t.Fatalf("Expected a ErrNoResponders error, got %v", err)
		}
		m := paf.Msg()
		if m == nil {
			t.Fatalf("Expected to be able to retrieve the message")
		}
		if m.Subject != "foo" || string(m.Data) != "Hello JS with Custom Inbox" {
			t.Fatalf("Wrong message: %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not receive an error in time")
	}
}

func TestPublishAsyncResetPendingOnReconnect(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	// Now create a stream and expect a PubAck from <-OK().
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"FOO"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	errs := make(chan error, 1)
	done := make(chan struct{}, 1)
	acks := make(chan nats.PubAckFuture, 100)
	go func() {
		for i := 0; i < 100; i++ {
			if ack, err := js.PublishAsync("FOO", []byte("hello")); err != nil {
				errs <- err
				return
			} else {
				acks <- ack
			}
		}
		close(acks)
		done <- struct{}{}
	}()
	select {
	case <-done:
	case err := <-errs:
		t.Fatalf("Unexpected error during publish: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	s.Shutdown()
	time.Sleep(100 * time.Millisecond)
	if pending := js.PublishAsyncPending(); pending != 0 {
		t.Fatalf("Expected no pending messages after server shutdown; got: %d", pending)
	}
	s = RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	for ack := range acks {
		select {
		case <-ack.Ok():
		case err := <-ack.Err():
			if !errors.Is(err, nats.ErrDisconnected) && !errors.Is(err, nats.ErrNoResponders) {
				t.Fatalf("Expected error: %v or %v; got: %v", nats.ErrDisconnected, nats.ErrNoResponders, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}
	}
}

func TestPublishAsyncRetryInErrHandler(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	streamCreated := make(chan struct{})
	errCB := func(js nats.JetStream, m *nats.Msg, e error) {
		<-streamCreated
		_, err := js.PublishMsgAsync(m)
		if err != nil {
			t.Fatalf("Unexpected error when republishing: %v", err)
		}
	}

	js, err := nc.JetStream(nats.PublishAsyncErrHandler(errCB))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	errs := make(chan error, 1)
	done := make(chan struct{}, 1)
	go func() {
		for i := 0; i < 10; i++ {
			if _, err := js.PublishAsync("FOO.A", []byte("hello")); err != nil {
				errs <- err
				return
			}
		}
		done <- struct{}{}
	}()
	select {
	case <-done:
	case err := <-errs:
		t.Fatalf("Unexpected error during publish: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	close(streamCreated)
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	info, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if info.State.Msgs != 10 {
		t.Fatalf("Expected 10 messages in the stream; got: %d", info.State.Msgs)
	}
}

func TestJetStreamPublishAsyncPerf(t *testing.T) {
	// Comment out below to run this benchmark.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// 64 byte payload.
	msg := make([]byte, 64)
	rand.Read(msg)

	// Setup error handler.
	var errors uint32
	errHandler := func(js nats.JetStream, originalMsg *nats.Msg, err error) {
		t.Logf("Got an async err: %v", err)
		atomic.AddUint32(&errors, 1)
	}

	js, err := nc.JetStream(
		nats.PublishAsyncErrHandler(errHandler),
		nats.PublishAsyncMaxPending(256),
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "B"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 1000000
	start := time.Now()
	for i := 0; i < toSend; i++ {
		if _, err = js.PublishAsync("B", msg); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	select {
	case <-js.PublishAsyncComplete():
		if ne := atomic.LoadUint32(&errors); ne > 0 {
			t.Fatalf("Got unexpected errors publishing")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	tt := time.Since(start)
	fmt.Printf("Took %v to send %d msgs\n", tt, toSend)
	fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
}

func TestPublishAsyncRetry(t *testing.T) {
	tests := []struct {
		name     string
		pubOpts  []nats.PubOpt
		ackError error
		pubErr   error
	}{
		{
			name: "retry until stream is ready",
			pubOpts: []nats.PubOpt{
				nats.RetryAttempts(10),
				nats.RetryWait(100 * time.Millisecond),
			},
		},
		{
			name: "fail after max retries",
			pubOpts: []nats.PubOpt{
				nats.RetryAttempts(2),
				nats.RetryWait(50 * time.Millisecond),
			},
			ackError: nats.ErrNoResponders,
		},
		{
			name:     "no retries",
			pubOpts:  nil,
			ackError: nats.ErrNoResponders,
		},
		{
			name: "invalid retry attempts",
			pubOpts: []nats.PubOpt{
				nats.RetryAttempts(-1),
			},
			pubErr: nats.ErrInvalidArg,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			nc, err := nats.Connect(s.ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// set max pending to 1 so that we can test if retries don't cause stall
			js, err := nc.JetStream(nats.PublishAsyncMaxPending(1))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()

			test.pubOpts = append(test.pubOpts, nats.StallWait(1*time.Nanosecond))
			ack, err := js.PublishAsync("foo", []byte("hello"), test.pubOpts...)
			if !errors.Is(err, test.pubErr) {
				t.Fatalf("Expected error: %v; got: %v", test.pubErr, err)
			}
			if err != nil {
				return
			}
			errs := make(chan error, 1)
			go func() {
				// create stream with delay so that publish will receive no responders
				time.Sleep(300 * time.Millisecond)
				if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}}); err != nil {
					errs <- err
				}
			}()
			select {
			case <-ack.Ok():
			case err := <-ack.Err():
				if test.ackError != nil {
					if !errors.Is(err, test.ackError) {
						t.Fatalf("Expected error: %v; got: %v", test.ackError, err)
					}
				} else {
					t.Fatalf("Unexpected ack error: %v", err)
				}
			case err := <-errs:
				t.Fatalf("Error creating stream: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatalf("Timeout waiting for ack")
			}
		})
	}
}
func TestJetStreamCleanupPublisher(t *testing.T) {

	t.Run("cleanup js publisher", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		// Create a stream.
		if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"FOO"}}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		numSubs := nc.NumSubscriptions()
		if _, err := js.PublishAsync("FOO", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		if numSubs+1 != nc.NumSubscriptions() {
			t.Fatalf("Expected an additional subscription after publish, got %d", nc.NumSubscriptions())
		}

		js.CleanupPublisher()

		if numSubs != nc.NumSubscriptions() {
			t.Fatalf("Expected subscriptions to be back to original count")
		}
	})

	t.Run("cleanup js publisher, cancel pending acks", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		cbErr := make(chan error, 10)
		js, err := nc.JetStream(nats.PublishAsyncErrHandler(func(js nats.JetStream, m *nats.Msg, err error) {
			cbErr <- err
		}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Create a stream with NoAck so that we can test that we cancel ack futures.
		if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"FOO"}, NoAck: true}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		numSubs := nc.NumSubscriptions()

		var acks []nats.PubAckFuture
		for i := 0; i < 10; i++ {
			ack, err := js.PublishAsync("FOO", []byte("hello"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			acks = append(acks, ack)
		}

		asyncComplete := js.PublishAsyncComplete()
		select {
		case <-asyncComplete:
			t.Fatalf("Should not complete, NoAck is set")
		case <-time.After(200 * time.Millisecond):
		}

		if numSubs+1 != nc.NumSubscriptions() {
			t.Fatalf("Expected an additional subscription after publish, got %d", nc.NumSubscriptions())
		}

		js.CleanupPublisher()

		if numSubs != nc.NumSubscriptions() {
			t.Fatalf("Expected subscriptions to be back to original count")
		}

		// check that PublishAsyncComplete channel is closed
		select {
		case <-asyncComplete:
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		// check that all ack futures are canceled
		for _, ack := range acks {
			select {
			case err := <-ack.Err():
				if !errors.Is(err, nats.ErrJetStreamPublisherClosed) {
					t.Fatalf("Expected JetStreamContextClosed error, got %v", err)
				}
			case <-ack.Ok():
				t.Fatalf("Expected error on the ack future")
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("Expected an error on the ack future")
			}
		}

		// check that async error handler is called for each pending ack
		for i := 0; i < 10; i++ {
			select {
			case err := <-cbErr:
				if !errors.Is(err, nats.ErrJetStreamPublisherClosed) {
					t.Fatalf("Expected JetStreamContextClosed error, got %v", err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("Expected errors to be passed from the async handler")
			}
		}
	})

}

func TestJetStreamPublishExpectZero(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test", "foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	// Explicitly set the header to zero.
	_, err = js.Publish("foo", []byte("bar"),
		nats.ExpectLastSequence(0),
		nats.ExpectLastSequencePerSubject(0),
	)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	rawMsg, err := js.GetMsg("TEST", 1)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	hdr, ok := rawMsg.Header["Nats-Expected-Last-Sequence"]
	if !ok {
		t.Fatal("Missing header")
	}
	got := hdr[0]
	expected := "0"
	if got != expected {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}
	hdr, ok = rawMsg.Header["Nats-Expected-Last-Subject-Sequence"]
	if !ok {
		t.Fatal("Missing header")
	}
	got = hdr[0]
	expected = "0"
	if got != expected {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}

	msg, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	hdr, ok = msg.Header["Nats-Expected-Last-Sequence"]
	if !ok {
		t.Fatal("Missing header")
	}
	got = hdr[0]
	expected = "0"
	if got != expected {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}
	hdr, ok = msg.Header["Nats-Expected-Last-Subject-Sequence"]
	if !ok {
		t.Fatal("Missing header")
	}
	got = hdr[0]
	expected = "0"
	if got != expected {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}
}

func TestJetStreamBindConsumer(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AddStream(nil); err == nil {
		t.Fatalf("Unexpected success")
	}
	si, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "foo" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	for i := 0; i < 25; i++ {
		js.Publish("foo", []byte("hi"))
	}

	// Both stream and consumer names are required for bind only.
	_, err = js.SubscribeSync("foo", nats.Bind("", ""))
	if err != nats.ErrStreamNameRequired {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.SubscribeSync("foo", nats.Bind("foo", ""))
	if err != nats.ErrConsumerNameRequired {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.SubscribeSync("foo", nats.Bind("foo", "push"))
	if err == nil || !errors.Is(err, nats.ErrConsumerNotFound) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Pull consumer
	_, err = js.PullSubscribe("foo", "pull", nats.Bind("foo", "pull"))
	if err == nil || !errors.Is(err, nats.ErrConsumerNotFound) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Push consumer
	_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
		Durable:        "push",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: nats.NewInbox(),
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Push Consumer Bind Only
	sub, err := js.SubscribeSync("foo", nats.Bind("foo", "push"))
	if err != nil {
		t.Fatal(err)
	}
	// Ambiguous declaration should not be allowed.
	_, err = js.SubscribeSync("foo", nats.Durable("push2"), nats.Bind("foo", "push"))
	if err == nil || !strings.Contains(err.Error(), `nats: duplicate consumer names (push2 and push)`) {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.SubscribeSync("foo", nats.BindStream("foo"), nats.Bind("foo2", "push"))
	if err == nil || !strings.Contains(err.Error(), `nats: duplicate stream name (foo and foo2)`) {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub.Unsubscribe()

	checkConsInactive := func() {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			ci, _ := js.ConsumerInfo("foo", "push")
			if ci != nil && !ci.PushBound {
				return nil
			}
			return fmt.Errorf("Consumer %q still active", "push")
		})
	}
	checkConsInactive()

	// Duplicate stream name is fine.
	sub, err = js.SubscribeSync("foo", nats.BindStream("foo"), nats.Bind("foo", "push"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Cannot have 2 instances for same durable
	_, err = js.SubscribeSync("foo", nats.Durable("push"))
	if err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Cannot start a queue sub since plain durable is active
	_, err = js.QueueSubscribeSync("foo", "wq", nats.Durable("push"))
	if err == nil || !strings.Contains(err.Error(), "without a deliver group") {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub.Unsubscribe()
	checkConsInactive()

	// Create a queue sub
	_, err = js.QueueSubscribeSync("foo", "wq1", nats.Durable("qpush"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Can't create a plain sub on that durable
	_, err = js.SubscribeSync("foo", nats.Durable("qpush"))
	if err == nil || !strings.Contains(err.Error(), "cannot create a subscription for a consumer with a deliver group") {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Try to attach different queue group
	_, err = js.QueueSubscribeSync("foo", "wq2", nats.Durable("qpush"))
	if err == nil || !strings.Contains(err.Error(), "cannot create a queue subscription") {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Pull consumer
	_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
		Durable:   "pull",
		AckPolicy: nats.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Pull consumer can bind without create using only the stream name (since durable is required argument).
	_, err = js.PullSubscribe("foo", "pull", nats.Bind("foo", "pull"))
	if err != nil {
		t.Fatal(err)
	}

	// Prevent binding to durable that is from a wrong type.
	_, err = js.PullSubscribe("foo", "push", nats.Bind("foo", "push"))
	if err != nats.ErrPullSubscribeToPushConsumer {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.SubscribeSync("foo", nats.Bind("foo", "pull"))
	if err != nats.ErrPullSubscribeRequired {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create ephemeral consumer
	sub1, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	cinfo, err := sub1.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}

	// Cannot bind to ephemeral consumer because it is active.
	_, err = js.SubscribeSync("foo", nats.Bind("foo", cinfo.Name))
	if err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("Unexpected error: %v", err)
	}

	// However, one can create an ephemeral Queue subscription and bind several members to it.
	sub2, err := js.QueueSubscribeSync("foo", "wq3")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Consumer all
	for i := 0; i < 25; i++ {
		msg, err := sub2.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on NextMsg: %v", err)
		}
		msg.AckSync()
	}
	cinfo, _ = sub2.ConsumerInfo()
	sub3, err := js.QueueSubscribeSync("foo", "wq3", nats.Bind("foo", cinfo.Name))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 0; i < 100; i++ {
		js.Publish("foo", []byte("new"))
	}
	// We expect sub3 to at least get a message
	if _, err := sub3.NextMsg(time.Second); err != nil {
		t.Fatalf("Second member failed to get a message: %v", err)
	}
}

func TestJetStreamDomain(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: { domain: ABC }
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// JS with custom domain
	jsd, err := nc.JetStream(nats.Domain("ABC"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	info, err := jsd.AccountInfo()
	if err != nil {
		t.Error(err)
	}
	got := info.Domain
	expected := "ABC"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	if _, err = jsd.AddStream(&nats.StreamConfig{Name: "foo"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsd.Publish("foo", []byte("first"))

	sub, err := jsd.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got = string(msg.Data)
	expected = "first"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	// JS without explicit bound domain should also work.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	info, err = js.AccountInfo()
	if err != nil {
		t.Error(err)
	}
	got = info.Domain
	expected = "ABC"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	js.Publish("foo", []byte("second"))

	sub2, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	msg, err = sub2.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got = string(msg.Data)
	expected = "first"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	msg, err = sub2.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got = string(msg.Data)
	expected = "second"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	// Using different domain not configured is an error.
	jsb, err := nc.JetStream(nats.Domain("XYZ"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = jsb.AccountInfo()
	if err != nats.ErrJetStreamNotEnabled {
		t.Errorf("Unexpected error: %v", err)
	}
}

// Test that we properly enforce per subject msg limits.
func TestJetStreamMaxMsgsPerSubject(t *testing.T) {
	const subjectMax = 5
	msc := nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo", "bar", "baz.*"},
		Storage:           nats.MemoryStorage,
		MaxMsgsPerSubject: subjectMax,
	}
	fsc := msc
	fsc.Storage = nats.FileStorage

	cases := []struct {
		name    string
		mconfig *nats.StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			// Client for API requests.
			nc, js := jsClient(t, s)
			defer nc.Close()

			var err error

			_, err = js.AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer js.DeleteStream(c.mconfig.Name)

			pubAndCheck := func(subj string, num int, expectedNumMsgs uint64) {
				t.Helper()
				for i := 0; i < num; i++ {
					if _, err = js.Publish(subj, []byte("TSLA")); err != nil {
						t.Fatalf("Unexpected publish error: %v", err)
					}
				}
				si, err := js.StreamInfo(c.mconfig.Name)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if si.State.Msgs != expectedNumMsgs {
					t.Fatalf("Expected %d msgs, got %d", expectedNumMsgs, si.State.Msgs)
				}
			}

			pubAndCheck("foo", 1, 1)
			pubAndCheck("foo", 4, 5)
			// Now make sure our per subject limits kick in..
			pubAndCheck("foo", 2, 5)
			pubAndCheck("baz.22", 5, 10)
			pubAndCheck("baz.33", 5, 15)
			// We are maxed so totals should be same no matter what we add here.
			pubAndCheck("baz.22", 5, 15)
			pubAndCheck("baz.33", 5, 15)

			// Now purge and make sure all is still good.
			if err := js.PurgeStream(c.mconfig.Name); err != nil {
				t.Fatalf("Unexpected purge error: %v", err)
			}
			pubAndCheck("foo", 1, 1)
			pubAndCheck("foo", 4, 5)
			pubAndCheck("baz.22", 5, 10)
			pubAndCheck("baz.33", 5, 15)
		})
	}
}

func TestJetStreamDrainFailsToDeleteConsumer(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	errCh := make(chan error, 1)
	nc, err := nats.Connect(s.ClientURL(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		select {
		case errCh <- err:
		default:
		}
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("hi"))

	blockCh := make(chan struct{})
	sub, err := js.Subscribe("foo", func(m *nats.Msg) {
		<-blockCh
	}, nats.Durable("dur"))
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}

	// Initiate the drain... it won't complete because we have blocked the
	// message callback.
	sub.Drain()

	// Now delete the JS consumer
	if err := js.DeleteConsumer("TEST", "dur"); err != nil {
		t.Fatalf("Error deleting consumer: %v", err)
	}

	// Now unblock and make sure we get the async error
	close(blockCh)

	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), "consumer not found") {
			t.Fatalf("Unexpected async error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get async error")
	}
}

func TestJetStreamDomainInPubAck(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB"}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	pa, err := js.Publish("foo", []byte("msg"))
	if err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if pa.Domain != "HUB" {
		t.Fatalf("Expected PubAck to have domain of %q, got %q", "HUB", pa.Domain)
	}
}

func TestJetStreamStreamAndConsumerDescription(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	streamDesc := "stream description"
	si, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Description: streamDesc,
		Subjects:    []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}
	if si.Config.Description != streamDesc {
		t.Fatalf("Invalid description: %q vs %q", streamDesc, si.Config.Description)
	}

	consDesc := "consumer description"
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Description:    consDesc,
		Durable:        "dur",
		DeliverSubject: "bar",
	})
	if err != nil {
		t.Fatalf("Error adding consumer: %v", err)
	}
	if ci.Config.Description != consDesc {
		t.Fatalf("Invalid description: %q vs %q", consDesc, ci.Config.Description)
	}
}

func TestJetStreamMsgSubjectRewrite(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	sub, err := nc.SubscribeSync(nats.NewInbox())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if _, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: sub.Subject,
		DeliverPolicy:  nats.DeliverAllPolicy,
	}); err != nil {
		t.Fatalf("Error adding consumer: %v", err)
	}

	if _, err := js.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Did not get message: %v", err)
	}
	if msg.Subject != "foo" {
		t.Fatalf("Subject should be %q, got %q", "foo", msg.Subject)
	}
	if string(msg.Data) != "msg" {
		t.Fatalf("Unexpected data: %q", msg.Data)
	}
}

func TestJetStreamPullSubscribeFetchContext(t *testing.T) {
	withJSCluster(t, "PULLCTX", 3, testJetStreamFetchContext)
}

func testJetStreamFetchContext(t *testing.T, srvs ...*jsServer) {
	srv := srvs[0]
	nc, js := jsClient(t, srv.Server)
	defer nc.Close()

	var err error

	subject := "WQ"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     subject,
		Replicas: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	sendMsgs := func(t *testing.T, totalMsgs int) {
		t.Helper()
		for i := 0; i < totalMsgs; i++ {
			payload := fmt.Sprintf("i:%d", i)
			_, err := js.Publish(subject, []byte(payload))
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		}
	}
	expected := 10
	sendMsgs(t, expected)

	sub, err := js.PullSubscribe(subject, "batch-ctx")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	t.Run("ctx background", func(t *testing.T) {
		_, err = sub.Fetch(expected, nats.Context(context.Background()))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != nats.ErrNoDeadlineContext {
			t.Errorf("Expected context deadline error, got: %v", err)
		}
	})

	t.Run("ctx canceled", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		cancel()

		_, err = sub.Fetch(expected, nats.Context(ctx))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != context.Canceled {
			t.Errorf("Expected context deadline error, got: %v", err)
		}

		ctx, cancel = context.WithCancel(context.Background())
		cancel()

		_, err = sub.Fetch(expected, nats.Context(ctx))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != context.Canceled {
			t.Errorf("Expected context deadline error, got: %v", err)
		}
	})

	t.Run("ctx timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		msgs, err := sub.Fetch(expected, nats.Context(ctx))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		got := len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}
		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Error(err)
		}
		if info.NumAckPending != expected {
			t.Errorf("Expected %d pending acks, got: %d", expected, info.NumAckPending)
		}

		for _, msg := range msgs {
			msg.AckSync()
		}

		info, err = sub.ConsumerInfo()
		if err != nil {
			t.Error(err)
		}
		if info.NumAckPending > 0 {
			t.Errorf("Expected no pending acks, got: %d", info.NumAckPending)
		}

		// No messages at this point.
		ctx, cancel = context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()

		_, err = sub.Fetch(1, nats.Context(ctx))
		if err != context.DeadlineExceeded {
			t.Errorf("Expected deadline exceeded fetching next message, got: %v", err)
		}

		// Send more messages then pull them with a new context
		expected = 5
		sendMsgs(t, expected)

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Single message fetch.
		msgs, err = sub.Fetch(1, nats.Context(ctx))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Expected to receive a single message, got: %d", len(msgs))
		}
		for _, msg := range msgs {
			msg.Ack()
		}

		// Fetch multiple messages.
		expected = 4
		msgs, err = sub.Fetch(expected, nats.Context(ctx))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got = len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}
		for _, msg := range msgs {
			msg.AckSync()
		}

		info, err = sub.ConsumerInfo()
		if err != nil {
			t.Error(err)
		}
		if info.NumAckPending > 0 {
			t.Errorf("Expected no pending acks, got: %d", info.NumAckPending)
		}
	})

	t.Run("ctx with cancel", func(t *testing.T) {
		// New JS context with slightly shorter timeout than default.
		js, err = nc.JetStream(nats.MaxWait(2 * time.Second))
		if err != nil {
			t.Fatal(err)
		}

		sub, err := js.PullSubscribe(subject, "batch-cancel-ctx")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		// Parent context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Fetch all the messages as needed.
		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		total := info.NumPending

		// Child context with timeout with the same duration as JS context timeout
		// will be created to fetch next message.
		msgs, err := sub.Fetch(1, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Expected a message, got: %d", len(msgs))
		}
		for _, msg := range msgs {
			msg.AckSync()
		}

		// Fetch the rest using same cancellation context.
		expected := int(total - 1)
		msgs, err = sub.Fetch(expected, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != expected {
			t.Fatalf("Expected %d messages, got: %d", expected, len(msgs))
		}
		for _, msg := range msgs {
			msg.AckSync()
		}

		// Fetch more messages and wait for timeout since there are none.
		_, err = sub.Fetch(expected, nats.Context(ctx))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != context.DeadlineExceeded {
			t.Fatalf("Expected deadline exceeded fetching next message, got: %v", err)
		}

		// Original cancellation context is not yet canceled, it should still work.
		if ctx.Err() != nil {
			t.Fatalf("Expected no errors in original cancellation context, got: %v", ctx.Err())
		}

		// Should be possible to use the same context again.
		sendMsgs(t, 5)

		// Get the next message to leave 4 pending.
		var pending uint64 = 4
		msgs, err = sub.Fetch(1, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Expected a message, got: %d", len(msgs))
		}
		for _, msg := range msgs {
			msg.AckSync()
		}

		// Cancel finally.
		cancel()

		_, err = sub.Fetch(1, nats.Context(ctx))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != context.Canceled {
			t.Fatalf("Expected deadline exceeded fetching next message, got: %v", err)
		}

		info, err = sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		total = info.NumPending
		if total != pending {
			t.Errorf("Expected %d pending messages, got: %d", pending, total)
		}
	})

	t.Run("MaxWait timeout should return nats error", func(t *testing.T) {
		_, err := sub.Fetch(1, nats.MaxWait(1*time.Nanosecond))
		if !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("Expect ErrTimeout, got err=%#v", err)
		}
	})

	t.Run("Context timeout should return context error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		_, err := sub.Fetch(1, nats.Context(ctx))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expect context.DeadlineExceeded, got err=%#v", err)
		}
	})
}

func TestJetStreamSubscribeContextCancel(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz", "foo.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 100
	for i := 0; i < toSend; i++ {
		js.Publish("bar", []byte("foo"))
	}

	t.Run("cancel unsubscribes and deletes ephemeral", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan *nats.Msg, 100)
		sub, err := js.Subscribe("bar", func(msg *nats.Msg) {
			ch <- msg

			// Cancel will unsubscribe and remove the subscription
			// of the consumer.
			if len(ch) >= 50 {
				cancel()
			}
		}, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}

		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
			t.Fatal("Timed out waiting for context to be canceled")
		}

		// Consumer should not be present since unsubscribe already called.
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			info, err := sub.ConsumerInfo()
			if err != nil && err == nats.ErrConsumerNotFound {
				return nil
			}
			return fmt.Errorf("Consumer still active, got: %v (info=%+v)", err, info)
		})

		got := len(ch)
		expected := 50
		if got < expected {
			t.Errorf("Expected to receive at least %d messages, got: %d", expected, got)
		}
	})

	t.Run("unsubscribe cancels child context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sub, err := js.Subscribe("bar", func(msg *nats.Msg) {}, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		err = sub.Unsubscribe()
		if err != nil {
			t.Fatal(err)
		}

		// Consumer should not be present since unsubscribe already called.
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			info, err := sub.ConsumerInfo()
			if err != nil && err == nats.ErrConsumerNotFound {
				return nil
			}
			return fmt.Errorf("Consumer still active, got: %v (info=%+v)", err, info)
		})
	})
}

func TestJetStreamClusterStreamLeaderChangeClientErr(t *testing.T) {
	t.Skip("The 2.9 server changed behavior making this test fail now")

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}

	withJSClusterAndStream(t, "R3S", 3, cfg, func(t *testing.T, stream string, servers ...*jsServer) {
		// We want to make sure the worse thing seen by the lower levels during a leadership change is NoResponders.
		// We will have three concurrent contexts going on.
		// 1. Leadership Changes every 500ms.
		// 2. Publishing messages to the stream every 10ms.
		// 3. StreamInfo calls every 15ms.
		expires := time.Now().Add(5 * time.Second)
		var wg sync.WaitGroup
		wg.Add(3)

		randServer := func() *server.Server {
			return servers[mrand.Intn(len(servers))].Server
		}

		// Leadership changes.
		go func() {
			defer wg.Done()
			nc, js := jsClient(t, randServer())
			defer nc.Close()

			sds := fmt.Sprintf(server.JSApiStreamLeaderStepDownT, "TEST")
			for time.Now().Before(expires) {
				time.Sleep(500 * time.Millisecond)
				si, err := js.StreamInfo("TEST")
				expectOk(t, err)
				_, err = nc.Request(sds, nil, time.Second)
				expectOk(t, err)

				// Wait on new leader.
				checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
					si, err = js.StreamInfo("TEST")
					if err != nil {
						return err
					}
					if si.Cluster.Leader == "" {
						return errors.New("No leader yet")
					}
					return nil
				})
			}
		}()

		// Published every 10ms
		toc := 0
		go func() {
			defer wg.Done()
			nc, js := jsClient(t, randServer())
			defer nc.Close()

			for time.Now().Before(expires) {
				time.Sleep(10 * time.Millisecond)
				_, err := js.Publish("foo", []byte("OK"))
				if err == nats.ErrTimeout {
					toc++
					continue
				}
				expectOk(t, err)
			}
		}()

		// StreamInfo calls.
		go func() {
			defer wg.Done()
			nc, js := jsClient(t, randServer())
			defer nc.Close()

			for time.Now().Before(expires) {
				time.Sleep(15 * time.Millisecond)
				_, err := js.StreamInfo("TEST")
				expectOk(t, err)
			}
		}()

		wg.Wait()

		// An occasional timeout can occur, but should be 0 or maybe 1 with ~10 leadership changes per test run.
		if toc > 1 {
			t.Fatalf("Got too many timeout errors from publish: %d", toc)
		}
	})
}

func TestJetStreamConsumerConfigReplicasAndMemStorage(t *testing.T) {
	withJSCluster(t, "CR", 3, func(t *testing.T, nodes ...*jsServer) {
		nc, js := jsClient(t, nodes[0].Server)
		defer nc.Close()

		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     "CR",
			Subjects: []string{"foo"},
			Replicas: 3,
		}); err != nil {
			t.Fatalf("Error adding stream: %v", err)
		}

		// We can't really check if the consumer ends-up with memory storage or not.
		// We are simply going to create a NATS subscription on the request subject
		// and make sure that the request contains "mem_storage:true".
		sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.CR.dur")
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		ci, err := js.AddConsumer("CR", &nats.ConsumerConfig{
			Durable:        "dur",
			DeliverSubject: "bar",
			Replicas:       1,
			MemoryStorage:  true,
		})
		if err != nil {
			t.Fatalf("Error adding consumer: %v", err)
		}
		if n := len(ci.Cluster.Replicas); n > 0 {
			t.Fatalf("Expected replicas to be 1, got %+v", ci.Cluster)
		}
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on next msg: %v", err)
		}
		if str := string(msg.Data); !strings.Contains(str, "mem_storage\":true") {
			t.Fatalf("Does not look like the request asked for memory storage: %s", str)
		}
	})
}

func TestJetStreamRePublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "RP",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo", "bar", "baz"},
		RePublish: &nats.RePublish{
			Source:      ">",
			Destination: "RP.>",
		},
	}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	sub, err := nc.SubscribeSync("RP.>")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	msg, toSend := []byte("OK TO REPUBLISH?"), 100
	for i := 0; i < toSend; i++ {
		js.Publish("foo", msg)
		js.Publish("bar", msg)
		js.Publish("baz", msg)
	}

	lseq := map[string]int{
		"foo": 0,
		"bar": 0,
		"baz": 0,
	}

	for i := 1; i <= toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on next msg: %v", err)
		}
		// Grab info from Header
		stream := m.Header.Get(nats.JSStream)
		if stream != "RP" {
			t.Fatalf("Unexpected header: %+v", m.Header)
		}
		// Make sure sequence is correct.
		seq, err := strconv.Atoi(m.Header.Get(nats.JSSequence))
		if err != nil {
			t.Fatalf("Error decoding sequence for %s", m.Header.Get(nats.JSSequence))
		}
		if seq != i {
			t.Fatalf("Expected sequence to be %v, got %v", i, seq)
		}
		// Make sure last sequence matches last seq we received on this subject.
		last, err := strconv.Atoi(m.Header.Get(nats.JSLastSequence))
		if err != nil {
			t.Fatalf("Error decoding last sequence for %s", m.Header.Get(nats.JSLastSequence))
		}
		if last != lseq[m.Subject] {
			t.Fatalf("Expected last sequence to be %v, got %v", lseq[m.Subject], last)
		}
		lseq[m.Subject] = seq
	}
}

func TestJetStreamDirectGetMsg(t *testing.T) {
	// Using standlone server here, we are testing the client side API, not
	// the server feature, which has tests checking it works in cluster mode.
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "DGM",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	send := func(subj, body string) {
		t.Helper()
		if _, err := js.Publish(subj, []byte(body)); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	send("foo", "a")
	send("foo", "b")
	send("foo", "c")
	send("bar", "d")
	send("foo", "e")

	// Without AllowDirect, we should get a timeout (so reduce the timeout for this call)
	if _, err := js.GetMsg("DGM", 1, nats.DirectGet(), nats.MaxWait(200*time.Millisecond)); err != context.DeadlineExceeded {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Update stream:
	si.Config.AllowDirect = true
	si, err = js.UpdateStream(&si.Config)
	if err != nil {
		t.Fatalf("Error updating stream: %v", err)
	}
	if !si.Config.AllowDirect {
		t.Fatalf("AllowDirect should be true: %+v", si)
	}

	check := func(seq uint64, opt nats.JSOpt, useGetLast bool, expectedSubj string, expectedSeq uint64, expectedBody string) {
		t.Helper()

		var msg *nats.RawStreamMsg
		var err error
		if useGetLast {
			msg, err = js.GetLastMsg("DGM", expectedSubj, []nats.JSOpt{opt}...)
		} else {
			msg, err = js.GetMsg("DGM", seq, []nats.JSOpt{opt}...)
		}
		if err != nil {
			t.Fatalf("Unable to get message: %v", err)
		}
		if msg.Subject != expectedSubj {
			t.Fatalf("Expected subject %q, got %q", expectedSubj, msg.Subject)
		}
		if msg.Sequence != expectedSeq {
			t.Fatalf("Expected sequence %v, got %v", expectedSeq, msg.Sequence)
		}
		if msg.Time.IsZero() {
			t.Fatal("Expected timestamp, did not get one")
		}
		if b := string(msg.Data); b != expectedBody {
			t.Fatalf("Expected body %q, got %q", expectedBody, b)
		}
	}

	check(0, nats.DirectGetNext("bar"), false, "bar", 4, "d")
	check(0, nats.DirectGet(), true, "foo", 5, "e")
	check(0, nats.DirectGetNext("foo"), false, "foo", 1, "a")
	check(4, nats.DirectGetNext("foo"), false, "foo", 5, "e")
	check(2, nats.DirectGetNext("foo"), false, "foo", 2, "b")

	msg := nats.NewMsg("foo")
	msg.Header.Set("MyHeader", "MyValue")
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	r, err := js.GetMsg("DGM", 6, nats.DirectGet())
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	if v := r.Header.Get("MyHeader"); v != "MyValue" {
		t.Fatalf("Expected header to be present, was not: %v", r.Header)
	}

	// Check for not found
	if _, err := js.GetMsg("DGM", 100, nats.DirectGet()); err != nats.ErrMsgNotFound {
		t.Fatalf("Expected not found error, got %v", err)
	}
	// Or invalid request
	if _, err := js.GetMsg("DGM", 0, nats.DirectGet()); err == nil || !strings.Contains(err.Error(), "Empty Request") {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Test direct get by subject by trying to get 'bar' directly
	r, err = js.GetLastMsg("DGM", "bar", nats.DirectGet())
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	if r.Subject != "bar" {
		t.Fatalf("expected subject to be 'bar', got: %v", r.Subject)
	}
	if string(r.Data) != "d" {
		t.Fatalf("expected data to be 'd', got: %v", string(r.Data))
	}
}

func TestJetStreamConsumerReplicasOption(t *testing.T) {
	withJSCluster(t, "CR", 3, func(t *testing.T, nodes ...*jsServer) {
		nc, js := jsClient(t, nodes[0].Server)
		defer nc.Close()

		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     "ConsumerReplicasTest",
			Subjects: []string{"foo"},
			Replicas: 3,
		}); err != nil {
			t.Fatalf("Error adding stream: %v", err)
		}

		// Subscribe to the stream with a durable consumer "bar" and replica set to 1.
		cb := func(msg *nats.Msg) {}
		_, err := js.Subscribe("foo", cb, nats.Durable("bar"), nats.ConsumerReplicas(1))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		// Get consumer info
		consInfo, err := js.ConsumerInfo("ConsumerReplicasTest", "bar")
		if err != nil {
			t.Fatalf("Error getting consumer info: %v", err)
		}

		// Check if the number of replicas is the same as we provided.
		if consInfo.Config.Replicas != 1 {
			t.Fatalf("Expected consumer replica to be %v, got %+v", 1, consInfo.Config.Replicas)
		}
	})
}

func TestJetStreamMsgAckShouldErrForConsumerAckNone(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "ACKNONE",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo"},
	}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}
	if _, err := js.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	sub, err := js.SubscribeSync("foo", nats.OrderedConsumer())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	if err := msg.Ack(); err != nats.ErrCantAckIfConsumerAckNone {
		t.Fatalf("Expected error indicating that sub is AckNone, got %v", err)
	}
}

func TestJetStreamOrderedConsumerRecreateAfterReconnect(t *testing.T) {
	s := RunBasicJetStreamServer()

	// monitor for ErrConsumerNotActive error and suppress logging
	hbMissed := make(chan struct{}, 10)
	errHandler := func(c *nats.Conn, s *nats.Subscription, err error) {
		if !errors.Is(err, nats.ErrConsumerNotActive) {
			t.Fatalf("Unexpected error: %v", err)
		}
		hbMissed <- struct{}{}
	}
	nc, js := jsClient(t, s, nats.ErrorHandler(errHandler))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.SubscribeSync("FOO.A", nats.OrderedConsumer(), nats.IdleHeartbeat(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	consInfo, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	consName := consInfo.Name
	// validate that the generated name of the consumer is 8
	// characters long (shorter than standard nuid)
	if len(consName) != 8 {
		t.Fatalf("Unexpected consumer name: %q", consName)
	}
	if _, err := js.Publish("FOO.A", []byte("msg 1")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if string(msg.Data) != "msg 1" {
		t.Fatalf("Invalid msg value; want: 'msg 1'; got: %q", string(msg.Data))
	}

	apiSub, err := nc.SubscribeSync("$JS.API.CONSUMER.*.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// restart the server
	s = restartBasicJSServer(t, s)
	defer shutdownJSServerAndRemoveStorage(t, s)

	// wait until we miss heartbeat
	select {
	case <-hbMissed:
	case <-time.After(10 * time.Second):
		t.Fatalf("Did not receive consumer not active error")
	}
	consDeleteMsg, err := apiSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !strings.HasPrefix(consDeleteMsg.Subject, "$JS.API.CONSUMER.") {
		t.Fatalf("Unexpected message subject: %q", consDeleteMsg.Subject)
	}
	consCreateMsg, err := apiSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !strings.HasPrefix(consCreateMsg.Subject, "$JS.API.CONSUMER.") {
		t.Fatalf("Unexpected message subject: %q", consCreateMsg.Subject)
	}
	if _, err := js.Publish("FOO.A", []byte("msg 2")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	consInfo, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if consInfo.Name == consName || len(consInfo.Name) != 8 {
		t.Fatalf("Unexpected consumer name: %q", consInfo.Name)
	}

	// make sure we pick up where we left off
	if string(msg.Data) != "msg 2" {
		t.Fatalf("Invalid msg value; want: 'msg 2'; got: %q", string(msg.Data))
	}
}

func TestJetStreamCreateStreamDiscardPolicy(t *testing.T) {
	tests := []struct {
		name                 string
		discardPolicy        nats.DiscardPolicy
		discardNewPerSubject bool
		maxMsgsPerSubject    int64
		withAPIError         bool
	}{
		{
			name:                 "with discard policy 'new' and discard new per subject set",
			discardPolicy:        nats.DiscardNew,
			discardNewPerSubject: true,
			maxMsgsPerSubject:    100,
		},
		{
			name:                 "with discard policy 'new' and discard new per subject not set",
			discardPolicy:        nats.DiscardNew,
			discardNewPerSubject: false,
			maxMsgsPerSubject:    100,
		},
		{
			name:                 "with discard policy 'old' and discard new per subject set",
			discardPolicy:        nats.DiscardOld,
			discardNewPerSubject: true,
			maxMsgsPerSubject:    100,
			withAPIError:         true,
		},
		{
			name:                 "with discard policy 'old' and discard new per subject not set",
			discardPolicy:        nats.DiscardOld,
			discardNewPerSubject: true,
			maxMsgsPerSubject:    100,
			withAPIError:         true,
		},
		{
			name:                 "with discard policy 'new' and discard new per subject set and max msgs per subject not set",
			discardPolicy:        nats.DiscardNew,
			discardNewPerSubject: true,
			withAPIError:         true,
		},
	}

	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			streamName := fmt.Sprintf("FOO%d", i)

			_, err := js.AddStream(&nats.StreamConfig{
				Name:                 streamName,
				Discard:              test.discardPolicy,
				DiscardNewPerSubject: test.discardNewPerSubject,
				MaxMsgsPerSubject:    test.maxMsgsPerSubject,
			})

			if test.withAPIError {
				var apiErr *nats.APIError
				if err == nil {
					t.Fatalf("Expected error, got nil")
				}
				if ok := errors.As(err, &apiErr); !ok {
					t.Fatalf("Expected nats.APIError, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			info, err := js.StreamInfo(streamName)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if info.Config.Discard != test.discardPolicy {
				t.Fatalf("Invalid value of discard policy; want: %s; got: %s", test.discardPolicy.String(), info.Config.Discard.String())
			}
			if info.Config.DiscardNewPerSubject != test.discardNewPerSubject {
				t.Fatalf("Invalid value of discard_new_per_subject; want: %t; got: %t", test.discardNewPerSubject, info.Config.DiscardNewPerSubject)
			}
		})
	}
}

func TestJetStreamStreamInfoAlternates(t *testing.T) {
	withJSCluster(t, "R3S", 3, func(t *testing.T, nodes ...*jsServer) {
		nc, js := jsClient(t, nodes[0].Server)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
		})
		expectOk(t, err)

		// Create a mirror as well.
		_, err = js.AddStream(&nats.StreamConfig{
			Name: "MIRROR",
			Mirror: &nats.StreamSource{
				Name: "TEST",
			},
		})
		expectOk(t, err)

		si, err := js.StreamInfo("TEST")
		expectOk(t, err)

		if len(si.Alternates) != 2 {
			t.Fatalf("Expected 2 alternates, got %d", len(si.Alternates))
		}
	})
}

func TestJetStreamSubscribeConsumerName(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz", "foo.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.Publish("foo", []byte("first"))
	if err != nil {
		t.Fatal(err)
	}

	// Lookup the stream for testing.
	_, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	sub, err := js.SubscribeSync("foo", nats.ConsumerName("my-ephemeral"))
	if err != nil {
		t.Fatal(err)
	}
	cinfo, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}
	got := cinfo.Config.Name
	expected := "my-ephemeral"
	if got != expected {
		t.Fatalf("Expected: %v, got: %v", expected, got)
	}
	// Confirm that this is a durable.
	got = cinfo.Config.Durable
	expected = ""
	if got != expected {
		t.Fatalf("Expected: %v, got: %v", expected, got)
	}
	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ConsumerName will be ignored in case a durable name has been set.
	sub, err = js.SubscribeSync("foo", nats.Durable("durable"), nats.ConsumerName("custom-name"))
	if err != nil {
		t.Fatal(err)
	}
	cinfo, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}
	got = cinfo.Config.Name
	expected = "durable"
	if got != expected {
		t.Fatalf("Expected: %v, got: %v", expected, got)
	}
	got = cinfo.Config.Durable
	expected = "durable"
	if got != expected {
		t.Fatalf("Expected: %v, got: %v", expected, got)
	}
	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Default Ephemeral name should be short like in the server.
	sub, err = js.SubscribeSync("foo", nats.ConsumerName(""))
	if err != nil {
		t.Fatal(err)
	}
	cinfo, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}
	expectedSize := 8
	result := len(cinfo.Config.Name)
	if result != expectedSize {
		t.Fatalf("Expected: %v, got: %v", expectedSize, result)
	}
}

func TestJetStreamOrderedConsumerDeleteAssets(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// For capturing errors.
	errCh := make(chan error, 1)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errCh <- err
	})

	// Create a sample asset.
	mlen := 128 * 1024
	msg := make([]byte, mlen)

	createStream := func() {
		t.Helper()
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "OBJECT",
			Subjects: []string{"a"},
			Storage:  nats.MemoryStorage,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Now send into the stream as chunks.
		const chunkSize = 256
		for i := 0; i < mlen; i += chunkSize {
			var chunk []byte
			if mlen-i <= chunkSize {
				chunk = msg[i:]
			} else {
				chunk = msg[i : i+chunkSize]
			}
			js.PublishAsync("a", chunk)
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(time.Second):
			t.Fatalf("Did not receive completion signal")
		}
	}

	t.Run("remove stream, expect error", func(t *testing.T) {
		createStream()

		sub, err := js.SubscribeSync("a", nats.OrderedConsumer(), nats.IdleHeartbeat(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()

		// Since we are sync we will be paused here due to flow control.
		time.Sleep(100 * time.Millisecond)
		// Now delete the asset and make sure we get an error.
		if err := js.DeleteStream("OBJECT"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Make sure we get an error.
		select {
		case err := <-errCh:
			if !errors.Is(err, nats.ErrStreamNotFound) {
				t.Fatalf("Got wrong error, wanted %v, got %v", nats.ErrStreamNotFound, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive err message as expected")
		}
	})

	t.Run("remove consumer, expect it to be recreated", func(t *testing.T) {
		createStream()

		createConsSub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.OBJECT.*.a")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer createConsSub.Unsubscribe()
		// Again here the IdleHeartbeat is not required, just overriding top shorten test time.
		sub, err := js.SubscribeSync("a", nats.OrderedConsumer(), nats.IdleHeartbeat(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()

		createConsMsg, err := createConsSub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !strings.Contains(string(createConsMsg.Data), `"stream_name":"OBJECT"`) {
			t.Fatalf("Invalid message on create consumer subject: %q", string(createConsMsg.Data))
		}

		time.Sleep(100 * time.Millisecond)
		ci, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		consName := ci.Name

		if err := js.DeleteConsumer("OBJECT", consName); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		createConsMsg, err = createConsSub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !strings.Contains(string(createConsMsg.Data), `"stream_name":"OBJECT"`) {
			t.Fatalf("Invalid message on create consumer subject: %q", string(createConsMsg.Data))
		}

		time.Sleep(100 * time.Millisecond)
		ci, err = sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		newConsName := ci.Name
		if consName == newConsName {
			t.Fatalf("Consumer should be recreated, but consumer name is the same")
		}
	})
}

// We want to make sure we do the right thing with lots of concurrent queue durable consumer requests.
// One should win and the others should share the delivery subject with the first one who wins.
func TestJetStreamConcurrentQueueDurablePushConsumers(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create stream.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create 10 durables concurrently.
	subs := make([]*nats.Subscription, 0, 10)
	var wg sync.WaitGroup
	mx := &sync.Mutex{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub, _ := js.QueueSubscribeSync("foo", "bar")
			mx.Lock()
			subs = append(subs, sub)
			mx.Unlock()
		}()
	}
	// Wait for all the consumers.
	wg.Wait()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Consumers != 1 {
		t.Fatalf("Expected exactly one consumer, got %d", si.State.Consumers)
	}

	// Now send some messages and make sure they are distributed.
	total := 1000
	for i := 0; i < total; i++ {
		js.Publish("foo", []byte("Hello"))
	}

	timeout := time.Now().Add(2 * time.Second)
	got := 0
	for time.Now().Before(timeout) {
		got = 0
		for _, sub := range subs {
			pending, _, _ := sub.Pending()
			// If a single sub has the total, then probably something is not right.
			if pending == total {
				t.Fatalf("A single member should not have gotten all messages")
			}
			got += pending
		}
		if got == total {
			// We are done!
			return
		}
	}
	t.Fatalf("Expected %v messages, got only %v", total, got)
}

func TestJetStreamAckTokens(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	now := time.Now()
	for _, test := range []struct {
		name     string
		expected *nats.MsgMetadata
		str      string
		end      string
		err      bool
	}{
		{
			"valid token size but not js ack",
			nil,
			"1.2.3.4.5.6.7.8.9",
			"",
			true,
		},
		{
			"valid token size but not js ack",
			nil,
			"1.2.3.4.5.6.7.8.9.10.11.12",
			"",
			true,
		},
		{
			"invalid token size",
			nil,
			"$JS.ACK.3.4.5.6.7.8",
			"",
			true,
		},
		{
			"invalid token size",
			nil,
			"$JS.ACK.3.4.5.6.7.8.9.10",
			"",
			true,
		},
		{
			"v1 style",
			&nats.MsgMetadata{
				Stream:       "TEST",
				Consumer:     "cons",
				NumDelivered: 1,
				Sequence: nats.SequencePair{
					Stream:   2,
					Consumer: 3,
				},
				Timestamp:  now,
				NumPending: 4,
			},
			"",
			"",
			false,
		},
		{
			"v2 style no domain with hash",
			&nats.MsgMetadata{
				Stream:       "TEST",
				Consumer:     "cons",
				NumDelivered: 1,
				Sequence: nats.SequencePair{
					Stream:   2,
					Consumer: 3,
				},
				Timestamp:  now,
				NumPending: 4,
			},
			"_.ACCHASH.",
			".abcde",
			false,
		},
		{
			"v2 style with domain and hash",
			&nats.MsgMetadata{
				Domain:       "HUB",
				Stream:       "TEST",
				Consumer:     "cons",
				NumDelivered: 1,
				Sequence: nats.SequencePair{
					Stream:   2,
					Consumer: 3,
				},
				Timestamp:  now,
				NumPending: 4,
			},
			"HUB.ACCHASH.",
			".abcde",
			false,
		},
		{
			"more than 12 tokens",
			&nats.MsgMetadata{
				Domain:       "HUB",
				Stream:       "TEST",
				Consumer:     "cons",
				NumDelivered: 1,
				Sequence: nats.SequencePair{
					Stream:   2,
					Consumer: 3,
				},
				Timestamp:  now,
				NumPending: 4,
			},
			"HUB.ACCHASH.",
			".abcde.ghijk.lmnop",
			false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo")
			msg.Sub = sub
			if test.err {
				msg.Reply = test.str
			} else {
				msg.Reply = fmt.Sprintf("$JS.ACK.%sTEST.cons.1.2.3.%v.4%s", test.str, now.UnixNano(), test.end)
			}

			meta, err := msg.Metadata()
			if test.err {
				if err == nil || meta != nil {
					t.Fatalf("Expected error for content: %q, got meta=%+v err=%v", test.str, meta, err)
				}
				// Expected error, we are done
				return
			}
			if err != nil {
				t.Fatalf("Expected: %+v with reply: %q, got error %v", test.expected, msg.Reply, err)
			}
			if meta.Timestamp.UnixNano() != now.UnixNano() {
				t.Fatalf("Timestamp is bad: %v vs %v", now.UnixNano(), meta.Timestamp.UnixNano())
			}
			meta.Timestamp = time.Time{}
			test.expected.Timestamp = time.Time{}
			if !reflect.DeepEqual(test.expected, meta) {
				t.Fatalf("Expected %+v, got %+v", test.expected, meta)
			}
		})
	}
}

func TestJetStreamTracing(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctr := 0
	js, err := nc.JetStream(&nats.ClientTrace{
		RequestSent: func(subj string, payload []byte) {
			ctr++
			if subj != "$JS.API.STREAM.CREATE.X" {
				t.Fatalf("Expected sent trace to %s: got: %s", "$JS.API.STREAM.CREATE.X", subj)
			}
		},
		ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
			ctr++
			if subj != "$JS.API.STREAM.CREATE.X" {
				t.Fatalf("Expected received trace to %s: got: %s", "$JS.API.STREAM.CREATE.X", subj)
			}
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{Name: "X"})
	if err != nil {
		t.Fatalf("add stream failed: %s", err)
	}
	if ctr != 2 {
		t.Fatalf("did not receive all trace events: %d", ctr)
	}
}

func TestJetStreamExpiredPullRequests(t *testing.T) {
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

	sub, err := js.PullSubscribe("foo", "bar", nats.PullMaxWaiting(2))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Make sure that we reject batch < 1
	if _, err := sub.Fetch(0); err == nil {
		t.Fatal("Expected error, did not get one")
	}
	if _, err := sub.Fetch(-1); err == nil {
		t.Fatal("Expected error, did not get one")
	}

	// Send 2 fetch requests
	for i := 0; i < 2; i++ {
		if _, err = sub.Fetch(1, nats.MaxWait(15*time.Millisecond)); err == nil {
			t.Fatalf("Expected error, got none")
		}
	}
	// Wait before the above expire
	time.Sleep(50 * time.Millisecond)
	batches := []int{1, 10}
	for _, bsz := range batches {
		start := time.Now()
		_, err = sub.Fetch(bsz, nats.MaxWait(250*time.Millisecond))
		dur := time.Since(start)
		if err == nil || dur < 50*time.Millisecond {
			t.Fatalf("Expected error and wait for 250ms, got err=%v and dur=%v", err, dur)
		}
	}
}

func TestJetStreamSyncSubscribeWithMaxAckPending(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.JetStreamLimits.MaxAckPending = 123
	s := RunServerWithOptions(&opts)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "MAX_ACK_PENDING", Subjects: []string{"foo"}}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// By default, the sync subscription will be created with a MaxAckPending equal
	// to the internal sync queue len, which is 64K. So that should error out
	// and make sure we get the actual limit

	checkSub := func(pull bool) {
		var sub *nats.Subscription
		var err error
		if pull {
			_, err = js.PullSubscribe("foo", "bar")
		} else {
			_, err = js.SubscribeSync("foo")
		}
		if err == nil || !strings.Contains(err.Error(), "system limit of 123") {
			t.Fatalf("Unexpected error: %v", err)
		}

		// But it should work if we use MaxAckPending() with lower value
		if pull {
			sub, err = js.PullSubscribe("foo", "bar", nats.MaxAckPending(64))
		} else {
			sub, err = js.SubscribeSync("foo", nats.MaxAckPending(64))
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		sub.Unsubscribe()
	}
	checkSub(false)
	checkSub(true)
}

func TestJetStreamClusterPlacement(t *testing.T) {
	// There used to be a test here that would not work because it would require
	// all servers in the cluster to know about each other tags. So we will simply
	// verify that if a stream is configured with placement and tags, the proper
	// "stream create" request is sent.
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	sub, err := nc.SubscribeSync("$JS.API.STREAM.CREATE.TEST")
	if err != nil {
		t.Fatalf("Error on sub: %v", err)
	}
	js.AddStream(&nats.StreamConfig{
		Name: "TEST",
		Placement: &nats.Placement{
			Tags: []string{"my_tag"},
		},
	})
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting stream create request: %v", err)
	}
	var req nats.StreamConfig
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if req.Placement == nil {
		t.Fatal("Expected placement, did not get it")
	}
	if n := len(req.Placement.Tags); n != 1 {
		t.Fatalf("Expected 1 tag, got %v", n)
	}
	if v := req.Placement.Tags[0]; v != "my_tag" {
		t.Fatalf("Unexpected tag: %q", v)
	}
}

func TestJetStreamConsumerMemoryStorage(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	s := RunServerWithOptions(&opts)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "STR", Subjects: []string{"foo"}}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// Pull ephemeral consumer with memory storage.
	sub, err := js.PullSubscribe("foo", "", nats.ConsumerMemoryStorage())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	consInfo, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Error getting consumer info: %v", err)
	}

	if !consInfo.Config.MemoryStorage {
		t.Fatalf("Expected memory storage to be %v, got %+v", true, consInfo.Config.MemoryStorage)
	}

	// Create a sync subscription with an in-memory ephemeral consumer.
	sub, err = js.SubscribeSync("foo", nats.ConsumerMemoryStorage())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	consInfo, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Error getting consumer info: %v", err)
	}

	if !consInfo.Config.MemoryStorage {
		t.Fatalf("Expected memory storage to be %v, got %+v", true, consInfo.Config.MemoryStorage)
	}

	// Async subscription with an in-memory ephemeral consumer.
	cb := func(msg *nats.Msg) {}
	sub, err = js.Subscribe("foo", cb, nats.ConsumerMemoryStorage())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	consInfo, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Error getting consumer info: %v", err)
	}

	if !consInfo.Config.MemoryStorage {
		t.Fatalf("Expected memory storage to be %v, got %+v", true, consInfo.Config.MemoryStorage)
	}
}

func TestJetStreamStreamInfoWithSubjectDetails(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Publish on enough subjects to exercise the pagination
	payload := make([]byte, 10)
	for i := 0; i < 100001; i++ {
		_, err := js.Publish(fmt.Sprintf("test.%d", i), payload)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Check that passing a filter returns the subject details
	result, err := js.StreamInfo("TEST", &nats.StreamInfoRequest{SubjectsFilter: ">"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.State.Subjects) != 100001 {
		t.Fatalf("expected 100001 subjects in the stream, but got %d instead", len(result.State.Subjects))
	}

	// Check that passing no filter does not return any subject details
	result, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.State.Subjects) != 0 {
		t.Fatalf("expected 0 subjects details from StreamInfo, but got %d instead", len(result.State.Subjects))
	}
}

func TestStreamNameBySubject(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range []struct {
		name       string
		streamName string
		err        error
	}{

		{name: "valid wildcard lookup", streamName: "test.*", err: nil},
		{name: "valid explicit lookup", streamName: "test.a", err: nil},
		{name: "lookup on not existing stream", streamName: "not.existing", err: nats.ErrNoMatchingStream},
	} {

		stream, err := js.StreamNameBySubject(test.streamName)
		if err != test.err {
			t.Fatalf("expected %v, got %v", test.err, err)
		}

		if stream != "TEST" && err == nil {
			t.Fatalf("returned stream name should be 'TEST'")
		}
	}
}

func TestJetStreamTransform(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:             "ORIGIN",
		Subjects:         []string{"test"},
		SubjectTransform: &nats.SubjectTransformConfig{Source: ">", Destination: "transformed.>"},
		Storage:          nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	err = nc.Publish("test", []byte("1"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Subjects: []string{},
		Name:     "SOURCING",
		Sources:  []*nats.StreamSource{{Name: "ORIGIN", SubjectTransforms: []nats.SubjectTransformConfig{{Source: ">", Destination: "fromtest.>"}}}},
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create a sync subscription with an in-memory ephemeral consumer.
	sub, err := js.SubscribeSync("fromtest.>", nats.ConsumerMemoryStorage(), nats.BindStream("SOURCING"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m.Subject != "fromtest.transformed.test" {
		t.Fatalf("the subject of the message doesn't match the expected fromtest.transformed.test: %s", m.Subject)
	}

}

func TestPullConsumerFetchRace(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	nc, js := jsClient(t, srv)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := js.Publish("FOO.123", []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Unexpected error during publish: %s", err)
		}
	}
	sub, err := js.PullSubscribe("FOO.123", "")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	cons, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msgs, err := sub.FetchBatch(5)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	errCh := make(chan error)
	go func() {
		for {
			err := msgs.Error()
			if err != nil {
				errCh <- err
				return
			}
		}
	}()
	deleteErrCh := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := js.DeleteConsumer("foo", cons.Name); err != nil {
			deleteErrCh <- err
		}
		close(deleteErrCh)
	}()

	var i int
	for msg := range msgs.Messages() {
		if string(msg.Data) != fmt.Sprintf("msg-%d", i) {
			t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, fmt.Sprintf("msg-%d", i), string(msg.Data))
		}
		i++
	}
	if i != 3 {
		t.Fatalf("Invalid number of messages received; want: %d; got: %d", 5, i)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, nats.ErrConsumerDeleted) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerDeleted, err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerDeleted, nil)
	}

	// wait until the consumer is deleted, otherwise we may close the connection
	// before the consumer delete response is received
	select {
	case ert, ok := <-deleteErrCh:
		if !ok {
			break
		}
		t.Fatalf("Error deleting consumer: %s", ert)
	case <-time.After(1 * time.Second):
		t.Fatalf("Expected done to be closed")
	}
}
