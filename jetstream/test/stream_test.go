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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCreateOrUpdateConsumer(t *testing.T) {
	tests := []struct {
		name           string
		consumerConfig jetstream.ConsumerConfig
		shouldCreate   bool
		withError      error
	}{
		{
			name:           "create durable pull consumer",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur"},
			shouldCreate:   true,
		},
		{
			name:           "create ephemeral pull consumer",
			consumerConfig: jetstream.ConsumerConfig{AckPolicy: jetstream.AckNonePolicy},
			shouldCreate:   true,
		},
		{
			name:           "with filter subject",
			consumerConfig: jetstream.ConsumerConfig{FilterSubject: "FOO.A"},
			shouldCreate:   true,
		},
		{
			name:           "with multiple filter subjects",
			consumerConfig: jetstream.ConsumerConfig{FilterSubjects: []string{"FOO.A", "FOO.B"}},
			shouldCreate:   true,
		},
		{
			name:           "with multiple filter subjects, overlapping subjects",
			consumerConfig: jetstream.ConsumerConfig{FilterSubjects: []string{"FOO.*", "FOO.B"}},
			withError:      jetstream.ErrOverlappingFilterSubjects,
		},
		{
			name:           "with multiple filter subjects and filter subject provided",
			consumerConfig: jetstream.ConsumerConfig{FilterSubjects: []string{"FOO.A", "FOO.B"}, FilterSubject: "FOO.C"},
			withError:      jetstream.ErrDuplicateFilterSubjects,
		},
		{
			name:           "with empty subject in FilterSubjects",
			consumerConfig: jetstream.ConsumerConfig{FilterSubjects: []string{"FOO.A", ""}},
			withError:      jetstream.ErrEmptyFilter,
		},
		{
			name:           "consumer already exists, update",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur", Description: "test consumer"},
		},
		{
			name:           "consumer already exists, illegal update",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur", AckPolicy: jetstream.AckNonePolicy},
			withError:      jetstream.ErrConsumerCreate,
		},
		{
			name:           "invalid durable name",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur.123"},
			withError:      jetstream.ErrInvalidConsumerName,
		},
	}

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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var sub *nats.Subscription
			if test.consumerConfig.FilterSubject != "" {
				sub, err = nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.CREATE.foo.*.%s", test.consumerConfig.FilterSubject))
			} else {
				sub, err = nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.*")
			}
			c, err := s.CreateOrUpdateConsumer(ctx, test.consumerConfig)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if test.shouldCreate {
				if _, err := sub.NextMsgWithContext(ctx); err != nil {
					t.Fatalf("Expected request on %s; got %s", sub.Subject, err)
				}
			}
			ci, err := s.Consumer(ctx, c.CachedInfo().Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci.CachedInfo().Config.AckPolicy != test.consumerConfig.AckPolicy {
				t.Fatalf("Invalid ack policy; want: %s; got: %s", test.consumerConfig.AckPolicy, ci.CachedInfo().Config.AckPolicy)
			}
			if !reflect.DeepEqual(test.consumerConfig.FilterSubjects, ci.CachedInfo().Config.FilterSubjects) {
				t.Fatalf("Invalid filter subjects; want: %v; got: %v", test.consumerConfig.FilterSubjects, ci.CachedInfo().Config.FilterSubjects)
			}
		})
	}
}

func TestCreateConsumer(t *testing.T) {
	tests := []struct {
		name           string
		consumerConfig jetstream.ConsumerConfig
		shouldCreate   bool
		withError      error
	}{
		{
			name:           "create durable pull consumer",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur"},
			shouldCreate:   true,
		},
		{
			name:           "idempotent create, no error",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur"},
			shouldCreate:   true,
		},
		{
			name:           "create ephemeral pull consumer",
			consumerConfig: jetstream.ConsumerConfig{AckPolicy: jetstream.AckNonePolicy},
			shouldCreate:   true,
		},
		{
			name:           "with filter subject",
			consumerConfig: jetstream.ConsumerConfig{FilterSubject: "FOO.A"},
			shouldCreate:   true,
		},
		{
			name:           "with metadata",
			consumerConfig: jetstream.ConsumerConfig{Metadata: map[string]string{"foo": "bar", "baz": "quux"}},
			shouldCreate:   true,
		},
		{
			name:           "with multiple filter subjects",
			consumerConfig: jetstream.ConsumerConfig{FilterSubjects: []string{"FOO.A", "FOO.B"}},
			shouldCreate:   true,
		},
		{
			name:           "with multiple filter subjects, overlapping subjects",
			consumerConfig: jetstream.ConsumerConfig{FilterSubjects: []string{"FOO.*", "FOO.B"}},
			withError:      jetstream.ErrOverlappingFilterSubjects,
		},
		{
			name:           "with multiple filter subjects and filter subject provided",
			consumerConfig: jetstream.ConsumerConfig{FilterSubjects: []string{"FOO.A", "FOO.B"}, FilterSubject: "FOO.C"},
			withError:      jetstream.ErrDuplicateFilterSubjects,
		},
		{
			name:           "with empty subject in FilterSubjects",
			consumerConfig: jetstream.ConsumerConfig{FilterSubjects: []string{"FOO.A", ""}},
			withError:      jetstream.ErrEmptyFilter,
		},
		{
			name:           "with invalid filter subject, leading dot",
			consumerConfig: jetstream.ConsumerConfig{FilterSubject: ".foo"},
			withError:      jetstream.ErrInvalidSubject,
		},
		{
			name:           "with invalid filter subject, trailing dot",
			consumerConfig: jetstream.ConsumerConfig{FilterSubject: "foo."},
			withError:      jetstream.ErrInvalidSubject,
		},
		{
			name:           "consumer already exists, error",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur", Description: "test consumer"},
			withError:      jetstream.ErrConsumerExists,
		},
		{
			name:           "invalid durable name",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur.123"},
			withError:      jetstream.ErrInvalidConsumerName,
		},
	}

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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var sub *nats.Subscription
			if test.consumerConfig.FilterSubject != "" {
				sub, err = nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.CREATE.foo.*.%s", test.consumerConfig.FilterSubject))
			} else {
				sub, err = nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.*")
			}
			c, err := s.CreateConsumer(ctx, test.consumerConfig)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if test.shouldCreate {
				if _, err := sub.NextMsgWithContext(ctx); err != nil {
					t.Fatalf("Expected request on %s; got %s", sub.Subject, err)
				}
			}
			ci, err := s.Consumer(ctx, c.CachedInfo().Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci.CachedInfo().Config.AckPolicy != test.consumerConfig.AckPolicy {
				t.Fatalf("Invalid ack policy; want: %s; got: %s", test.consumerConfig.AckPolicy, ci.CachedInfo().Config.AckPolicy)
			}
			if !reflect.DeepEqual(test.consumerConfig.FilterSubjects, ci.CachedInfo().Config.FilterSubjects) {
				t.Fatalf("Invalid filter subjects; want: %v; got: %v", test.consumerConfig.FilterSubjects, ci.CachedInfo().Config.FilterSubjects)
			}
			if !reflect.DeepEqual(test.consumerConfig.Metadata, ci.CachedInfo().Config.Metadata) {
				t.Fatalf("Invalid metadata; want: %v; got: %v", test.consumerConfig.Metadata, ci.CachedInfo().Config.Metadata)
			}
		})
	}
}

func TestUpdateConsumer(t *testing.T) {
	tests := []struct {
		name           string
		consumerConfig jetstream.ConsumerConfig
		shouldUpdate   bool
		withError      error
	}{
		{
			name:           "update consumer",
			consumerConfig: jetstream.ConsumerConfig{Name: "testcons", Description: "updated consumer"},
			shouldUpdate:   true,
		},
		{
			name:           "update consumer, with metadata",
			consumerConfig: jetstream.ConsumerConfig{Name: "testcons", Description: "updated consumer", Metadata: map[string]string{"foo": "bar", "baz": "quux"}},
			shouldUpdate:   true,
		},
		{
			name:           "illegal update",
			consumerConfig: jetstream.ConsumerConfig{Name: "testcons", AckPolicy: jetstream.AckNonePolicy},
			withError:      jetstream.ErrConsumerCreate,
		},
		{
			name:           "consumer does not exist",
			consumerConfig: jetstream.ConsumerConfig{Name: "abc"},
			withError:      jetstream.ErrConsumerDoesNotExist,
		},
	}

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

	_, err = s.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: "testcons"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var sub *nats.Subscription
			if test.consumerConfig.FilterSubject != "" {
				sub, err = nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.CREATE.foo.*.%s", test.consumerConfig.FilterSubject))
			} else {
				sub, err = nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.*")
			}
			c, err := s.UpdateConsumer(ctx, test.consumerConfig)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if test.shouldUpdate {
				if _, err := sub.NextMsgWithContext(ctx); err != nil {
					t.Fatalf("Expected request on %s; got %s", sub.Subject, err)
				}
			}
			ci, err := s.Consumer(ctx, c.CachedInfo().Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci.CachedInfo().Config.AckPolicy != test.consumerConfig.AckPolicy {
				t.Fatalf("Invalid ack policy; want: %s; got: %s", test.consumerConfig.AckPolicy, ci.CachedInfo().Config.AckPolicy)
			}
			if !reflect.DeepEqual(test.consumerConfig.FilterSubjects, ci.CachedInfo().Config.FilterSubjects) {
				t.Fatalf("Invalid filter subjects; want: %v; got: %v", test.consumerConfig.FilterSubjects, ci.CachedInfo().Config.FilterSubjects)
			}
		})
	}
}

func TestConsumer(t *testing.T) {
	tests := []struct {
		name      string
		durable   string
		withError error
	}{
		{
			name:    "get existing consumer",
			durable: "dur",
		},
		{
			name:      "consumer does not exist",
			durable:   "abc",
			withError: jetstream.ErrConsumerNotFound,
		},
		{
			name:      "invalid durable name",
			durable:   "dur.123",
			withError: jetstream.ErrInvalidConsumerName,
		},
	}

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
	_, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "dur", AckPolicy: jetstream.AckAllPolicy, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := s.Consumer(ctx, test.durable)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if c.CachedInfo().Name != test.durable {
				t.Fatalf("Unexpected consumer fetched; want: %s; got: %s", test.durable, c.CachedInfo().Name)
			}
		})
	}
}

func TestDeleteConsumer(t *testing.T) {
	tests := []struct {
		name      string
		durable   string
		withError error
	}{
		{
			name:    "delete existing consumer",
			durable: "dur",
		},
		{
			name:      "consumer does not exist",
			durable:   "dur",
			withError: jetstream.ErrConsumerNotFound,
		},
		{
			name:      "invalid durable name",
			durable:   "dur.123",
			withError: jetstream.ErrInvalidConsumerName,
		},
	}

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
	_, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "dur", AckPolicy: jetstream.AckAllPolicy, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.DeleteConsumer(ctx, test.durable)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			_, err = s.Consumer(ctx, test.durable)
			if err == nil || !errors.Is(err, jetstream.ErrConsumerNotFound) {
				t.Fatalf("Expected error: %v; got: %v", jetstream.ErrConsumerNotFound, err)
			}
		})
	}
}

func TestStreamInfo(t *testing.T) {
	tests := []struct {
		name                string
		subjectsFilter      string
		expectedSubjectMsgs map[string]uint64
		deletedDetails      bool
		timeout             time.Duration
		withError           error
	}{
		{
			name:    "info without opts",
			timeout: 5 * time.Second,
		},
		{
			name: "with empty context",
		},
		{
			name:           "with deleted details",
			deletedDetails: true,
			timeout:        5 * time.Second,
		},
		{
			name:                "with subjects filter, one subject",
			subjectsFilter:      "FOO.A",
			timeout:             5 * time.Second,
			expectedSubjectMsgs: map[string]uint64{"FOO.A": 8},
		},
		{
			name:                "with subjects filter, wildcard subject",
			subjectsFilter:      "FOO.*",
			timeout:             5 * time.Second,
			expectedSubjectMsgs: map[string]uint64{"FOO.A": 8, "FOO.B": 10},
		},
		{
			name:                "with subjects filter, and deleted details",
			subjectsFilter:      "FOO.A",
			timeout:             5 * time.Second,
			expectedSubjectMsgs: map[string]uint64{"FOO.A": 8},
		},
		{
			name:      "context timeout",
			timeout:   1 * time.Microsecond,
			withError: context.DeadlineExceeded,
		},
	}

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

	s, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := js.Publish(context.Background(), "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(context.Background(), "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	if err := s.DeleteMsg(context.Background(), 3); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s.DeleteMsg(context.Background(), 5); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, test.timeout)
				defer cancel()
			}
			opts := make([]jetstream.StreamInfoOpt, 0)
			if test.deletedDetails {
				opts = append(opts, jetstream.WithDeletedDetails(test.deletedDetails))
			}
			if test.subjectsFilter != "" {
				opts = append(opts, jetstream.WithSubjectFilter(test.subjectsFilter))
			}
			info, err := s.Info(ctx, opts...)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if info.Config.Description != "desc" {
				t.Fatalf("Unexpected description value fetched; want: foo; got: %s", info.Config.Description)
			}
			if test.deletedDetails {
				if info.State.NumDeleted != 2 {
					t.Fatalf("Expected 2 deleted messages; got: %d", info.State.NumDeleted)
				}
				if len(info.State.Deleted) != 2 || !reflect.DeepEqual(info.State.Deleted, []uint64{3, 5}) {
					t.Fatalf("Invalid value for deleted details; want: [3 5] got: %v", info.State.Deleted)
				}
			}
			if test.subjectsFilter != "" {
				if !reflect.DeepEqual(test.expectedSubjectMsgs, info.State.Subjects) {
					t.Fatalf("Invalid value for subjects filter; want: %v; got: %v", test.expectedSubjectMsgs, info.State.Subjects)
				}
			}
		})
	}
}

func TestSubjectsFilterPaging(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	s, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 0; i < 110000; i++ {
		if _, err := js.PublishAsync(fmt.Sprintf("FOO.%d", i), nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatal("PublishAsyncComplete timeout")
	}

	info, err := s.Info(context.Background(), jetstream.WithSubjectFilter("FOO.*"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(info.State.Subjects) != 110000 {
		t.Fatalf("Unexpected number of subjects; want: 110000; got: %d", len(info.State.Subjects))
	}
	cInfo := s.CachedInfo()
	if len(cInfo.State.Subjects) != 0 {
		t.Fatalf("Unexpected number of subjects; want: 0; got: %d", len(cInfo.State.Subjects))
	}
}

func TestStreamCachedInfo(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	s, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        "foo",
		Subjects:    []string{"FOO.*"},
		Description: "desc",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info := s.CachedInfo()

	if info.Config.Name != "foo" {
		t.Fatalf("Invalid stream name; expected: 'foo'; got: %s", info.Config.Name)
	}
	if info.Config.Description != "desc" {
		t.Fatalf("Invalid stream description; expected: 'desc'; got: %s", info.Config.Description)
	}

	// update consumer and see if info is updated
	_, err = js.UpdateStream(ctx, jetstream.StreamConfig{
		Name:        "foo",
		Subjects:    []string{"FOO.*"},
		Description: "updated desc",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info = s.CachedInfo()

	if info.Config.Name != "foo" {
		t.Fatalf("Invalid stream name; expected: 'foo'; got: %s", info.Config.Name)
	}

	// description should not be updated when using cached values
	if info.Config.Description != "desc" {
		t.Fatalf("Invalid stream description; expected: 'updated desc'; got: %s", info.Config.Description)
	}
}

func TestGetMsg(t *testing.T) {
	tests := []struct {
		name            string
		seq             uint64
		opts            []jetstream.GetMsgOpt
		expectedData    string
		expectedHeaders nats.Header
		timeout         time.Duration
		withError       error
	}{
		{
			name:         "get existing msg",
			seq:          2,
			timeout:      5 * time.Second,
			expectedData: "msg 1 on subject B",
		},
		{
			name:         "with empty context",
			seq:          2,
			expectedData: `msg 1 on subject B`,
		},
		{
			name:      "get deleted msg",
			seq:       3,
			withError: jetstream.ErrMsgNotFound,
		},
		{
			name:      "get non existing msg",
			seq:       50,
			withError: jetstream.ErrMsgNotFound,
		},
		{
			name:         "with next for subject",
			seq:          1,
			opts:         []jetstream.GetMsgOpt{jetstream.WithGetMsgSubject("*.C")},
			expectedData: "msg with headers",
			expectedHeaders: map[string][]string{
				"X-Nats-Test-Data": {"test_data"},
				"X-Nats-Key":       {"123"},
			},
		},
		{
			name:         "get msg with headers",
			seq:          9,
			expectedData: "msg with headers",
			expectedHeaders: map[string][]string{
				"X-Nats-Test-Data": {"test_data"},
				"X-Nats-Key":       {"123"},
			},
		},
		{
			name:      "context timeout",
			seq:       1,
			timeout:   1 * time.Microsecond,
			withError: context.DeadlineExceeded,
		},
	}

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

	s1, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(context.Background(), "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(context.Background(), "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	if _, err := js.PublishMsg(context.Background(), &nats.Msg{
		Data: []byte("msg with headers"),
		Header: map[string][]string{
			"X-Nats-Test-Data": {"test_data"},
			"X-Nats-Key":       {"123"},
		},
		Subject: "FOO.C",
	}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s1.DeleteMsg(context.Background(), 3); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s1.DeleteMsg(context.Background(), 5); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// same stream, but with allow direct
	s2, err := js.CreateStream(context.Background(),
		jetstream.StreamConfig{Name: "bar",
			Subjects:    []string{"BAR.*"},
			Description: "desc",
			AllowDirect: true,
		})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(context.Background(), "BAR.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(context.Background(), "BAR.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	if _, err := js.PublishMsg(context.Background(), &nats.Msg{
		Data: []byte("msg with headers"),
		Header: map[string][]string{
			"X-Nats-Test-Data": {"test_data"},
			"X-Nats-Key":       {"123"},
		},
		Subject: "BAR.C",
	}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s2.DeleteMsg(context.Background(), 3); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s2.DeleteMsg(context.Background(), 5); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		ctx := context.Background()
		if test.timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, test.timeout)
			defer cancel()
		}
		t.Run(fmt.Sprintf("%s - %s", test.name, "allow direct: false"), func(t *testing.T) {
			msg, err := s1.GetMsg(ctx, test.seq, test.opts...)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(msg.Data) != test.expectedData {
				t.Fatalf("Invalid message data; want: %s; got: %s", test.expectedData, string(msg.Data))
			}
			if !reflect.DeepEqual(msg.Header, test.expectedHeaders) {
				t.Fatalf("Invalid message headers; want: %v; got: %v", test.expectedHeaders, msg.Header)
			}
		})
		t.Run(fmt.Sprintf("%s - %s", test.name, "allow direct: true"), func(t *testing.T) {
			msg, err := s2.GetMsg(ctx, test.seq, test.opts...)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(msg.Data) != test.expectedData {
				t.Fatalf("Invalid message data; want: %s; got: %s", test.expectedData, string(msg.Data))
			}
			for k, v := range test.expectedHeaders {
				if !reflect.DeepEqual(msg.Header[k], v) {
					t.Fatalf("Expected header: %v; got: %v", v, msg.Header[k])
				}
			}
		})
	}
}

func TestGetLastMsgForSubject(t *testing.T) {
	tests := []struct {
		name         string
		subject      string
		expectedData string
		allowDirect  bool
		timeout      time.Duration
		withError    error
	}{
		{
			name:         "get existing msg",
			subject:      "*.A",
			expectedData: "msg 4 on subject A",
			timeout:      5 * time.Second,
		},
		{
			name:         "with empty context",
			subject:      "*.A",
			expectedData: "msg 4 on subject A",
		},
		{
			name:         "get last msg from stream",
			subject:      ">",
			expectedData: "msg 4 on subject B",
			timeout:      5 * time.Second,
		},
		{
			name:      "no messages on subject",
			subject:   "*.Z",
			withError: jetstream.ErrMsgNotFound,
		},
		{
			name:      "context timeout",
			subject:   "*.A",
			timeout:   1 * time.Microsecond,
			withError: context.DeadlineExceeded,
		},
	}

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

	s1, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(context.Background(), "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(context.Background(), "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// same stream, but with allow direct
	s2, err := js.CreateStream(context.Background(),
		jetstream.StreamConfig{Name: "bar",
			Subjects:    []string{"BAR.*"},
			Description: "desc",
			AllowDirect: true,
		})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(context.Background(), "BAR.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(context.Background(), "BAR.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	for _, test := range tests {
		ctx := context.Background()
		if test.timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, test.timeout)
			defer cancel()
		}
		t.Run(fmt.Sprintf("%s - %s", test.name, "allow direct: false"), func(t *testing.T) {
			msg, err := s1.GetLastMsgForSubject(ctx, test.subject)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(msg.Data) != test.expectedData {
				t.Fatalf("Invalid message data; want: %s; got: %s", test.expectedData, string(msg.Data))
			}
		})
		t.Run(fmt.Sprintf("%s - %s", test.name, "allow direct: true"), func(t *testing.T) {
			msg, err := s2.GetLastMsgForSubject(ctx, test.subject)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(msg.Data) != test.expectedData {
				t.Fatalf("Invalid message data; want: %s; got: %s", test.expectedData, string(msg.Data))
			}
		})
	}
}

func TestDeleteMsg(t *testing.T) {
	tests := []struct {
		name      string
		seq       uint64
		timeout   time.Duration
		withError error
	}{
		{
			name:    "delete message",
			seq:     3,
			timeout: 5 * time.Second,
		},
		{
			name: "with empty context",
			seq:  2,
		},
		{
			name:      "msg not found",
			seq:       10,
			withError: jetstream.ErrMsgDeleteUnsuccessful,
		},
		{
			name:      "context timeout",
			seq:       1,
			timeout:   1 * time.Microsecond,
			withError: context.DeadlineExceeded,
		},
	}
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
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	for _, test := range tests {
		ctx := context.Background()
		if test.timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, test.timeout)
			defer cancel()
		}
		t.Run(test.name, func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.STREAM.MSG.DELETE.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			err = s.DeleteMsg(ctx, test.seq)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			deleteMsg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(deleteMsg.Data), `"no_erase":true`) {
				t.Fatalf("Expected no_erase on request; got: %q", string(deleteMsg.Data))
			}
			if _, err = s.GetMsg(ctx, test.seq); err == nil || !errors.Is(err, jetstream.ErrMsgNotFound) {
				t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgNotFound, err)
			}
		})
	}
}

func TestSecureDeleteMsg(t *testing.T) {
	tests := []struct {
		name      string
		seq       uint64
		withError error
	}{
		{
			name: "delete message",
			seq:  3,
		},
		{
			name:      "msg not found",
			seq:       10,
			withError: jetstream.ErrMsgDeleteUnsuccessful,
		},
	}
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
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.STREAM.MSG.DELETE.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			err = s.SecureDeleteMsg(ctx, test.seq)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			deleteMsg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if strings.Contains(string(deleteMsg.Data), `"no_erase":true`) {
				t.Fatalf("Expected no_erase to be set to false on request; got: %q", string(deleteMsg.Data))
			}
			if _, err = s.GetMsg(ctx, test.seq); err == nil || !errors.Is(err, jetstream.ErrMsgNotFound) {
				t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgNotFound, err)
			}
		})
	}
}

func TestListConsumers(t *testing.T) {
	tests := []struct {
		name         string
		consumersNum int
		timeout      time.Duration
		withError    error
	}{
		{
			name:         "list consumers",
			consumersNum: 500,
			timeout:      5 * time.Second,
		},
		{
			name:         "with empty context",
			consumersNum: 500,
		},
		{
			name:         "no consumers available",
			consumersNum: 0,
		},
		{
			name:         "context timeout",
			consumersNum: 500,
			timeout:      1 * time.Microsecond,
			withError:    context.DeadlineExceeded,
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
			s, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for i := 0; i < test.consumersNum; i++ {
				_, err = s.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			ctx := context.Background()
			if test.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, test.timeout)
				defer cancel()
			}
			consumersList := s.ListConsumers(ctx)
			consumers := make([]*jetstream.ConsumerInfo, 0)
			for s := range consumersList.Info() {
				consumers = append(consumers, s)
			}
			if test.withError != nil {
				if !errors.Is(consumersList.Err(), test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, consumersList.Err())
				}
				return
			}
			if consumersList.Err() != nil {
				t.Fatalf("Unexpected error: %v", consumersList.Err())
			}
			if len(consumers) != test.consumersNum {
				t.Fatalf("Wrong number of streams; want: %d; got: %d", test.consumersNum, len(consumers))
			}
		})
	}
}

func TestConsumerNames(t *testing.T) {
	tests := []struct {
		name         string
		consumersNum int
		timeout      time.Duration
		withError    error
	}{
		{
			name:         "list consumer names",
			consumersNum: 500,
			timeout:      5 * time.Second,
		},
		{
			name:         "with empty context",
			consumersNum: 500,
		},
		{
			name:         "no consumers available",
			consumersNum: 0,
			timeout:      5 * time.Second,
		},
		{
			name:         "context timeout",
			consumersNum: 500,
			timeout:      1 * time.Microsecond,
			withError:    context.DeadlineExceeded,
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
			s, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for i := 0; i < test.consumersNum; i++ {
				_, err = s.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			ctx := context.Background()
			if test.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, test.timeout)
				defer cancel()
			}
			consumersList := s.ConsumerNames(ctx)
			consumers := make([]string, 0)
			for name := range consumersList.Name() {
				consumers = append(consumers, name)
			}
			if test.withError != nil {
				if !errors.Is(consumersList.Err(), test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, consumersList.Err())
				}
				return
			}
			if consumersList.Err() != nil {
				t.Fatalf("Unexpected error: %v", consumersList.Err())
			}
			if len(consumers) != test.consumersNum {
				t.Fatalf("Wrong number of streams; want: %d; got: %d", test.consumersNum, len(consumers))
			}
		})
	}
}

func TestPurgeStream(t *testing.T) {
	tests := []struct {
		name        string
		opts        []jetstream.StreamPurgeOpt
		expectedSeq []uint64
		timeout     time.Duration
		withError   error
	}{
		{
			name:        "purge all messages",
			expectedSeq: []uint64{},
			timeout:     5 * time.Second,
		},
		{
			name:        "with empty context",
			expectedSeq: []uint64{},
		},
		{
			name:        "purge on subject",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject("FOO.2")},
			expectedSeq: []uint64{1, 3, 5, 7, 9},
			timeout:     5 * time.Second,
		},
		{
			name:        "purge with sequence",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeSequence(5)},
			expectedSeq: []uint64{5, 6, 7, 8, 9, 10},
			timeout:     5 * time.Second,
		},
		{
			name:        "purge with keep",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeKeep(3)},
			expectedSeq: []uint64{8, 9, 10},
			timeout:     5 * time.Second,
		},
		{
			name:        "purge with filter and sequence",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject("FOO.2"), jetstream.WithPurgeSequence(8)},
			expectedSeq: []uint64{1, 3, 5, 7, 8, 9, 10},
			timeout:     5 * time.Second,
		},
		{
			name:        "purge with filter and keep",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject("FOO.2"), jetstream.WithPurgeKeep(3)},
			expectedSeq: []uint64{1, 3, 5, 6, 7, 8, 9, 10},
			timeout:     5 * time.Second,
		},
		{
			name:      "with sequence and keep",
			opts:      []jetstream.StreamPurgeOpt{jetstream.WithPurgeSequence(5), jetstream.WithPurgeKeep(3)},
			withError: jetstream.ErrInvalidOption,
		},
		{
			name:      "context timeout",
			timeout:   1 * time.Microsecond,
			withError: context.DeadlineExceeded,
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

			s, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			for i := 0; i < 5; i++ {
				if _, err := js.Publish(context.Background(), "FOO.1", []byte(fmt.Sprintf("msg %d on FOO.1", i))); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if _, err := js.Publish(context.Background(), "FOO.2", []byte(fmt.Sprintf("msg %d on FOO.2", i))); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			ctx := context.Background()
			if test.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, test.timeout)
				defer cancel()
			}
			err = s.Purge(ctx, test.opts...)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			seqs := make([]uint64, 0)
		Loop:
			for {
				msgs, err := c.FetchNoWait(1)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				msg := <-msgs.Messages()
				if msg == nil {
					break Loop
				}
				if err := msgs.Error(); err != nil {
					t.Fatalf("unexpected error during fetch: %v", err)
				}
				meta, err := msg.Metadata()
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				seqs = append(seqs, meta.Sequence.Stream)
			}
			if !reflect.DeepEqual(seqs, test.expectedSeq) {
				t.Fatalf("Invalid result; want: %v; got: %v", test.expectedSeq, seqs)
			}
		})
	}
}

func TestPauseConsumer(t *testing.T) {
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

	s, err := js.CreateStream(context.TODO(), jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	t.Run("create a paused consumer", func(t *testing.T) {
		const consumerName = "durr"
		pauseUntil := time.Now().Add(1 * time.Minute)
		consumer, err := s.CreateOrUpdateConsumer(context.TODO(), jetstream.ConsumerConfig{
			Durable:     consumerName,
			AckPolicy:   jetstream.AckAllPolicy,
			Description: "desc",
			PauseUntil:  &pauseUntil,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		info, err := consumer.Info(context.TODO())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !info.Paused {
			t.Fatalf("Consumer should be paused")
		}
		if info.PauseRemaining <= time.Duration(0) {
			t.Fatalf("PauseRemaining should be greater than 0")
		}
	})

	t.Run("pausing a consumer that does not exists", func(t *testing.T) {
		const consumerName = "durr1"
		pauseUntil := time.Now().Add(1 * time.Minute)
		_, err := s.PauseConsumer(context.TODO(), consumerName, pauseUntil)
		if err == nil {
			t.Fatalf("Expected error; got: %v", err)
		}
		if !errors.Is(err, jetstream.ErrConsumerNotFound) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrConsumerNotFound, err)
		}
	})

	t.Run("pausing consumer", func(t *testing.T) {
		const consumerName = "durr2"
		consumer, err := s.CreateOrUpdateConsumer(context.TODO(), jetstream.ConsumerConfig{
			Durable:     consumerName,
			AckPolicy:   jetstream.AckAllPolicy,
			Description: "desc",
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		info, err := consumer.Info(context.TODO())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if info.Paused {
			t.Fatalf("Consumer should not be paused")
		}

		pauseUntil := time.Now().Add(1 * time.Minute)
		resp, err := s.PauseConsumer(context.TODO(), consumerName, pauseUntil)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if !resp.Paused {
			t.Fatalf("Consumer should be paused")
		}
		if !resp.PauseUntil.Equal(pauseUntil) {
			t.Fatalf("Invalid pause until; want: %v; got: %v", pauseUntil, resp.PauseUntil)
		}
		if resp.PauseRemaining <= time.Duration(0) {
			t.Fatalf("PauseRemaining should be greater than 0")
		}
	})

	t.Run("resuming consumer", func(t *testing.T) {
		const consumerName = "durr3"
		pauseUntil := time.Now().Add(20 * time.Minute)
		consumer, err := s.CreateOrUpdateConsumer(context.TODO(), jetstream.ConsumerConfig{
			Durable:     consumerName,
			AckPolicy:   jetstream.AckAllPolicy,
			Description: "desc",
			PauseUntil:  &pauseUntil,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		info, err := consumer.Info(context.TODO())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !info.Paused {
			t.Fatalf("Consumer should be paused")
		}

		resp, err := s.ResumeConsumer(context.TODO(), consumerName)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if resp.Paused {
			t.Fatalf("Consumer should not be paused")
		}
		if resp.PauseRemaining != time.Duration(0) {
			t.Fatalf("PauseRemaining should be 0")
		}
	})
}
