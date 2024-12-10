// Copyright 2022-2023 The NATS Authors
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
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublishMsg(t *testing.T) {
	type publishConfig struct {
		msg             *nats.Msg
		opts            []jetstream.PublishOpt
		expectedHeaders nats.Header
		expectedAck     jetstream.PubAck
		withError       func(*testing.T, error)
	}
	tests := []struct {
		name      string
		srvConfig []byte
		timeout   time.Duration
		msgs      []publishConfig
	}{
		{
			name: "publish 3 simple messages, no opts",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
						Domain:   "",
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
						Domain:   "",
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
						Domain:   "",
					},
				},
			},
		},
		{
			name: "publish 3 messages with message ID, with duplicate",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("1")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"1"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("1")},
					expectedAck: jetstream.PubAck{
						Stream:    "foo",
						Sequence:  1,
						Duplicate: true,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"1"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("2")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"2"},
					},
				},
			},
		},
		{
			name: "expect last msg ID",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("1")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"1"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("2")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"2"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastMsgID("2")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Last-Msg-Id": []string{"2"},
					},
				},
			},
		},
		{
			name: "invalid last msg id",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("1")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"1"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastMsgID("abc")},
					withError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10070 {
							t.Fatalf("Expected error code: 10070; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "expect last sequence",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastSequence(2)},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Last-Sequence": []string{"2"},
					},
				},
			},
		},
		{
			name: "invalid last seq",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastSequence(123)},
					withError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10071 {
							t.Fatalf("Expected error code: 10071; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "expect last sequence per subject",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastSequencePerSubject(1)},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Last-Subject-Sequence": []string{"1"},
					},
				},
			},
		},
		{
			name: "expect last sequence per subject for subject",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{
						jetstream.WithExpectLastSequencePerSubject(1),
						jetstream.WithExpectLastSequencePerSubjectForSubject("FOO.1"),
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Last-Subject-Sequence":         []string{"1"},
						"Nats-Expected-Last-Subject-Sequence-Subject": []string{"FOO.1"},
					},
				},
			},
		},
		{
			name: "last sequence per subject for different subject",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1.bar",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1.baz",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2.baz",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 4"),
						Subject: "FOO.1.bax",
					},
					opts: []jetstream.PublishOpt{
						jetstream.WithExpectLastSequencePerSubject(2),
						jetstream.WithExpectLastSequencePerSubjectForSubject("FOO.1.*"),
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 4,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Last-Subject-Sequence":         []string{"2"},
						"Nats-Expected-Last-Subject-Sequence-Subject": []string{"FOO.1.*"},
					},
				},
			},
		},
		{
			name: "invalid last sequence per subject",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastSequencePerSubject(123)},
					withError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10071 {
							t.Fatalf("Expected error code: 10071; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "invalid last sequence per subject for subject",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{
						jetstream.WithExpectLastSequencePerSubject(123),
						jetstream.WithExpectLastSequencePerSubjectForSubject("FOO.1"),
					},
					withError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10071 {
							t.Fatalf("Expected error code: 10071; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "invalid last sequence per subject for different subject",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1.bar",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1.baz",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2.baz",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 4"),
						Subject: "FOO.1.bax",
					},
					opts: []jetstream.PublishOpt{
						jetstream.WithExpectLastSequencePerSubject(123),
						jetstream.WithExpectLastSequencePerSubjectForSubject("FOO.1.*"),
					},
					withError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10071 {
							t.Fatalf("Expected error code: 10071; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "expect stream header",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectStream("foo")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Stream": []string{"foo"},
					},
				},
			},
		},
		{
			name: "invalid expected stream header",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectStream("abc")},
					withError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10060 {
							t.Fatalf("Expected error code: 10060; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "publish 3 simple messages with domain set",
			srvConfig: []byte(
				`
					listen: 127.0.0.1:-1
					jetstream: {domain: "test-domain"}
				`),
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
						Domain:   "test-domain",
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
						Domain:   "test-domain",
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
						Domain:   "test-domain",
					},
				},
			},
		},
		{
			name: "publish timeout",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Stream": []string{"foo"},
					},
					withError: func(t *testing.T, err error) {
						if !errors.Is(err, context.DeadlineExceeded) {
							t.Fatalf("Expected deadline exceeded error; got: %v", err)
						}
					},
				},
			},
			timeout: 1 * time.Nanosecond,
		},
		{
			name: "invalid option set",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithStallWait(1 * time.Second)},
					withError: func(t *testing.T, err error) {
						if !errors.Is(err, jetstream.ErrInvalidOption) {
							t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
						}
					},
				},
			},
		},
		{
			name: "no subject set on message",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data: []byte("msg 1"),
					},
					opts: []jetstream.PublishOpt{},
					withError: func(t *testing.T, err error) {
						if !errors.Is(err, nats.ErrBadSubject) {
							t.Fatalf("Expected error: %v; got: %v", nats.ErrBadSubject, err)
						}
					},
				},
			},
		},
		{
			name: "invalid subject set",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "ABC",
					},
					opts: []jetstream.PublishOpt{},
					withError: func(t *testing.T, err error) {
						if !errors.Is(err, jetstream.ErrNoStreamResponse) {
							t.Fatalf("Expected error: %v; got: %v", jetstream.ErrNoStreamResponse, err)
						}
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var srv *server.Server
			if test.srvConfig != nil {
				conf := createConfFile(t, test.srvConfig)
				defer os.Remove(conf)
				srv, _ = RunServerWithConfig(conf)
			} else {
				srv = RunBasicJetStreamServer()
			}
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
			_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.>"}, MaxMsgSize: 128})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			for _, pub := range test.msgs {
				var pubCtx context.Context
				var pubCancel context.CancelFunc
				if test.timeout != 0 {
					pubCtx, pubCancel = context.WithTimeout(ctx, test.timeout)
				} else {
					pubCtx, pubCancel = context.WithTimeout(ctx, 1*time.Minute)
				}
				ack, err := js.PublishMsg(pubCtx, pub.msg, pub.opts...)
				pubCancel()
				if pub.withError != nil {
					pub.withError(t, err)
					continue
				}
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if !reflect.DeepEqual(pub.expectedHeaders, pub.msg.Header) {
					t.Fatalf("Invalid headers on message; want: %v; got: %v", pub.expectedHeaders, pub.msg.Header)
				}
				if *ack != pub.expectedAck {
					t.Fatalf("Invalid ack received; want: %v; got: %v", pub.expectedAck, ack)
				}
			}
		})
	}
}

func TestPublish(t *testing.T) {
	// Only very basic test cases, as most use cases are tested in TestPublishMsg
	tests := []struct {
		name      string
		msg       []byte
		subject   string
		opts      []jetstream.PublishOpt
		withError error
	}{
		{
			name:    "publish single message on stream, no options",
			msg:     []byte("msg"),
			subject: "FOO.1",
		},
		{
			name:    "publish single message on stream with message id",
			msg:     []byte("msg"),
			subject: "FOO.1",
			opts:    []jetstream.PublishOpt{jetstream.WithMsgID("1")},
		},
		{
			name:      "empty subject passed",
			msg:       []byte("msg"),
			subject:   "",
			withError: nats.ErrBadSubject,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
			_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, MaxMsgSize: 64})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ack, err := js.Publish(ctx, test.subject, test.msg, test.opts...)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ack.Sequence != 1 || ack.Stream != "foo" {
				t.Fatalf("Invalid ack; want sequence 1 on stream foo, got: %v", ack)
			}
		})
	}
}

func TestPublishMsgAsync(t *testing.T) {
	type publishConfig struct {
		msg              *nats.Msg
		opts             []jetstream.PublishOpt
		expectedHeaders  nats.Header
		expectedAck      jetstream.PubAck
		withAckError     func(*testing.T, error)
		withPublishError func(*testing.T, error)
	}
	tests := []struct {
		name      string
		msgs      []publishConfig
		srvConfig []byte
		timeout   time.Duration
	}{
		{
			name: "publish 3 simple messages, no opts",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
						Domain:   "",
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
						Domain:   "",
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
						Domain:   "",
					},
				},
			},
		},
		{
			name: "publish 3 messages with message ID, with duplicate",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("1")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"1"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("1")},
					expectedAck: jetstream.PubAck{
						Stream:    "foo",
						Sequence:  1,
						Duplicate: true,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"1"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("2")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"2"},
					},
				},
			},
		},
		{
			name: "expect last msg ID",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("1")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"1"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("2")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"2"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastMsgID("2")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Last-Msg-Id": []string{"2"},
					},
				},
			},
		},
		{
			name: "invalid last msg id",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithMsgID("1")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
					expectedHeaders: nats.Header{
						"Nats-Msg-Id": []string{"1"},
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastMsgID("abc")},
					withAckError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10070 {
							t.Fatalf("Expected error code: 10070; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "expect last sequence",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastSequence(2)},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Last-Sequence": []string{"2"},
					},
				},
			},
		},
		{
			name: "invalid last seq",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastSequence(123)},
					withAckError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10071 {
							t.Fatalf("Expected error code: 10071; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "expect last sequence per subject",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastSequencePerSubject(1)},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Last-Subject-Sequence": []string{"1"},
					},
				},
			},
		},
		{
			name: "invalid last sequence per subject",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.2",
					},
					opts: []jetstream.PublishOpt{},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectLastSequencePerSubject(123)},
					withAckError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10071 {
							t.Fatalf("Expected error code: 10071; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "expect stream header",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectStream("foo")},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
					},
					expectedHeaders: nats.Header{
						"Nats-Expected-Stream": []string{"foo"},
					},
				},
			},
		},
		{
			name: "invalid expected stream header",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithExpectStream("abc")},
					withAckError: func(t *testing.T, err error) {
						var apiErr *jetstream.APIError
						if ok := errors.As(err, &apiErr); !ok {
							t.Fatalf("Expected API error; got: %v", err)
						}
						if apiErr.ErrorCode != 10060 {
							t.Fatalf("Expected error code: 10060; got: %d", apiErr.ErrorCode)
						}
					},
				},
			},
		},
		{
			name: "publish 3 simple messages with domain set",
			srvConfig: []byte(
				`
					listen: 127.0.0.1:-1
					jetstream: {domain: "test-domain"}
				`),
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 1,
						Domain:   "test-domain",
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 2"),
						Subject: "FOO.1",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 2,
						Domain:   "test-domain",
					},
				},
				{
					msg: &nats.Msg{
						Data:    []byte("msg 3"),
						Subject: "FOO.2",
					},
					expectedAck: jetstream.PubAck{
						Stream:   "foo",
						Sequence: 3,
						Domain:   "test-domain",
					},
				},
			},
		},
		{
			name: "invalid subject set",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "ABC",
					},
					withAckError: func(t *testing.T, err error) {
						if !errors.Is(err, jetstream.ErrNoStreamResponse) {
							t.Fatalf("Expected error: %v; got: %v", jetstream.ErrNoStreamResponse, err)
						}
					},
				},
			},
		},
		{
			name: "invalid retry number set",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithRetryAttempts(-1)},
					withPublishError: func(t *testing.T, err error) {
						if !errors.Is(err, jetstream.ErrInvalidOption) {
							t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
						}
					},
				},
			},
		},
		{
			name: "invalid retry wait set",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithRetryWait(-1)},
					withPublishError: func(t *testing.T, err error) {
						if !errors.Is(err, jetstream.ErrInvalidOption) {
							t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
						}
					},
				},
			},
		},
		{
			name: "invalid stall wait set",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
					},
					opts: []jetstream.PublishOpt{jetstream.WithStallWait(-1)},
					withPublishError: func(t *testing.T, err error) {
						if !errors.Is(err, jetstream.ErrInvalidOption) {
							t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
						}
					},
				},
			},
		},
		{
			name: "reply subject set",
			msgs: []publishConfig{
				{
					msg: &nats.Msg{
						Data:    []byte("msg 1"),
						Subject: "FOO.1",
						Reply:   "BAR",
					},
					withPublishError: func(t *testing.T, err error) {
						if !errors.Is(err, jetstream.ErrAsyncPublishReplySubjectSet) {
							t.Fatalf("Expected error: %v; got: %v", nats.ErrNoResponders, err)
						}
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var srv *server.Server
			if test.srvConfig != nil {
				conf := createConfFile(t, test.srvConfig)
				defer os.Remove(conf)
				srv, _ = RunServerWithConfig(conf)
			} else {
				srv = RunBasicJetStreamServer()
			}
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
			_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, MaxMsgSize: 64})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			for _, pub := range test.msgs {
				ackFuture, err := js.PublishMsgAsync(pub.msg, pub.opts...)
				if pub.withPublishError != nil {
					pub.withPublishError(t, err)
					continue
				}
				select {
				case ack := <-ackFuture.Ok():
					if pub.withAckError != nil {
						t.Fatalf("Expected error, got nil")
					}
					if *ack != pub.expectedAck {
						t.Fatalf("Invalid ack received; want: %v; got: %v", pub.expectedAck, ack)
					}
					msg := ackFuture.Msg()
					if !reflect.DeepEqual(pub.expectedHeaders, msg.Header) {
						t.Fatalf("Invalid headers on message; want: %v; got: %v", pub.expectedHeaders, pub.msg.Header)
					}
					if string(msg.Data) != string(pub.msg.Data) {
						t.Fatalf("Invalid message in ack; want: %q; got: %q", string(pub.msg.Data), string(msg.Data))
					}
				case err := <-ackFuture.Err():
					if pub.withAckError == nil {
						t.Fatalf("Expected no error. got: %v", err)
					}
					pub.withAckError(t, err)
				}
			}

			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}
		})
	}
}

func TestPublishMsgAsyncWithPendingMsgs(t *testing.T) {
	t.Run("outstanding ack exceed limit", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(5))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 20; i++ {
			_, err = js.PublishAsync("FOO.1", []byte("msg"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if numPending := js.PublishAsyncPending(); numPending > 5 {
				t.Fatalf("Expected 5 pending messages, got: %d", numPending)
			}
		}
	})
	t.Run("too many messages without ack", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(5))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     "foo",
			Subjects: []string{"FOO.*"},
			// disable stream acks
			NoAck: true,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 5; i++ {
			_, err = js.PublishAsync("FOO.1", []byte("msg"), jetstream.WithStallWait(10*time.Millisecond))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		if _, err = js.PublishAsync("FOO.1", []byte("msg"), jetstream.WithStallWait(10*time.Millisecond)); err == nil || !errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrTooManyStalledMsgs, err)
		}
	})

	t.Run("with server restart", func(t *testing.T) {
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

		_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		go func() {
			for i := 0; i < 50; i++ {
				_, _ = js.PublishAsync("FOO.1", []byte("msg"))
			}
		}()
		srv = restartBasicJSServer(t, srv)
		defer shutdownJSServerAndRemoveStorage(t, srv)

		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(10 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}
	})
}

func TestPublishAsyncResetPendingOnReconnect(t *testing.T) {
	s := RunBasicJetStreamServer()

	nc, err := nats.Connect(s.ClientURL())
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
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	errs := make(chan error, 1)
	done := make(chan struct{}, 1)
	acks := make(chan jetstream.PubAckFuture, 100)
	wg := sync.WaitGroup{}
	go func() {
		for i := 0; i < 100; i++ {
			if ack, err := js.PublishAsync("FOO.A", []byte("hello")); err != nil {
				errs <- err
				return
			} else {
				acks <- ack
			}
			wg.Add(1)
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
	for ack := range acks {
		go func(paf jetstream.PubAckFuture) {
			select {
			case <-paf.Ok():
			case err := <-paf.Err():
				if !errors.Is(err, nats.ErrDisconnected) && !errors.Is(err, nats.ErrNoResponders) {
					errs <- fmt.Errorf("Expected error: %v or %v; got: %v", nats.ErrDisconnected, nats.ErrNoResponders, err)
				}
			case <-time.After(5 * time.Second):
				errs <- errors.New("Did not receive completion signal")
			}
			wg.Done()
		}(ack)
	}
	s = restartBasicJSServer(t, s)
	defer shutdownJSServerAndRemoveStorage(t, s)

	wg.Wait()
	select {
	case err := <-errs:
		t.Fatalf("Unexpected error: %v", err)
	default:
	}
}

func TestPublishAsyncRetry(t *testing.T) {
	tests := []struct {
		name     string
		pubOpts  []jetstream.PublishOpt
		ackError error
	}{
		{
			name: "retry until stream is ready",
			pubOpts: []jetstream.PublishOpt{
				jetstream.WithRetryAttempts(10),
				jetstream.WithRetryWait(100 * time.Millisecond),
			},
		},
		{
			name: "fail after max retries",
			pubOpts: []jetstream.PublishOpt{
				jetstream.WithRetryAttempts(2),
				jetstream.WithRetryWait(50 * time.Millisecond),
			},
			ackError: jetstream.ErrNoStreamResponse,
		},
		{
			name: "retries disabled",
			pubOpts: []jetstream.PublishOpt{
				jetstream.WithRetryAttempts(0),
			},
			ackError: jetstream.ErrNoStreamResponse,
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
			js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(1))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			test.pubOpts = append(test.pubOpts, jetstream.WithStallWait(1*time.Nanosecond))
			ack, err := js.PublishAsync("foo", []byte("hello"), test.pubOpts...)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			publishComplete := js.PublishAsyncComplete()
			errs := make(chan error, 1)
			go func() {
				// create stream with delay so that publish will receive no responders
				time.Sleep(300 * time.Millisecond)
				if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST", Subjects: []string{"foo"}}); err != nil {
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

			select {
			case <-publishComplete:
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}
		})
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
	errCB := func(js jetstream.JetStream, m *nats.Msg, e error) {
		<-streamCreated
		_, err := js.PublishMsgAsync(m)
		if err != nil {
			t.Fatalf("Unexpected error when republishing: %v", err)
		}
	}

	js, err := jetstream.New(nc, jetstream.WithPublishAsyncErrHandler(errCB))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errs := make(chan error, 1)
	done := make(chan struct{}, 1)
	go func() {
		for i := 0; i < 10; i++ {
			if _, err := js.PublishAsync("FOO.A", []byte("hello"), jetstream.WithRetryAttempts(0)); err != nil {
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
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	close(streamCreated)
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if info.State.Msgs != 10 {
		t.Fatalf("Expected 10 messages in the stream; got: %d", info.State.Msgs)
	}
}
