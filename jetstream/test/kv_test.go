// Copyright 2023-2025 The NATS Authors
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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestKeyValueBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST", History: 5, TTL: time.Hour})
	expectOk(t, err)

	if kv.Bucket() != "TEST" {
		t.Fatalf("Expected bucket name to be %q, got %q", "TEST", kv.Bucket())
	}

	// Simple Put
	r, err := kv.Put(ctx, "name", []byte("derek"))
	expectOk(t, err)
	if r != 1 {
		t.Fatalf("Expected 1 for the revision, got %d", r)
	}
	// Simple Get
	e, err := kv.Get(ctx, "name")
	expectOk(t, err)
	if string(e.Value()) != "derek" {
		t.Fatalf("Got wrong value: %q vs %q", e.Value(), "derek")
	}
	if e.Revision() != 1 {
		t.Fatalf("Expected 1 for the revision, got %d", e.Revision())
	}

	// Delete
	err = kv.Delete(ctx, "name")
	expectOk(t, err)
	_, err = kv.Get(ctx, "name")
	expectErr(t, err, jetstream.ErrKeyNotFound)
	r, err = kv.Create(ctx, "name", []byte("derek"))
	expectOk(t, err)
	if r != 3 {
		t.Fatalf("Expected 3 for the revision, got %d", r)
	}
	err = kv.Delete(ctx, "name", jetstream.LastRevision(4))
	expectErr(t, err)
	err = kv.Delete(ctx, "name", jetstream.LastRevision(3))
	expectOk(t, err)

	// Conditional Updates.
	r, err = kv.Update(ctx, "name", []byte("rip"), 4)
	expectOk(t, err)
	_, err = kv.Update(ctx, "name", []byte("ik"), 3)
	expectErr(t, err)
	_, err = kv.Update(ctx, "name", []byte("ik"), r)
	expectOk(t, err)
	r, err = kv.Create(ctx, "age", []byte("22"))
	expectOk(t, err)
	_, err = kv.Update(ctx, "age", []byte("33"), r)
	expectOk(t, err)

	// Status
	status, err := kv.Status(ctx)
	expectOk(t, err)
	if status.History() != 5 {
		t.Fatalf("expected history of 5 got %d", status.History())
	}
	if status.Bucket() != "TEST" {
		t.Fatalf("expected bucket TEST got %v", status.Bucket())
	}
	if status.TTL() != time.Hour {
		t.Fatalf("expected 1 hour TTL got %v", status.TTL())
	}
	if status.Values() != 7 {
		t.Fatalf("expected 7 values got %d", status.Values())
	}
	if status.BackingStore() != "JetStream" {
		t.Fatalf("invalid backing store kind %s", status.BackingStore())
	}

	kvs := status.(*jetstream.KeyValueBucketStatus)
	si := kvs.StreamInfo()
	if si == nil {
		t.Fatalf("StreamInfo not received")
	}
}

func TestCreateKeyValue(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	// invalid bucket name
	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST.", Description: "Test KV"})
	expectErr(t, err, jetstream.ErrInvalidBucketName)

	_, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST", Description: "Test KV"})
	expectOk(t, err)

	// Check that we can't overwrite existing bucket.
	_, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST", Description: "New KV"})
	expectErr(t, err, jetstream.ErrBucketExists)

	// assert that we're backwards compatible
	expectErr(t, err, jetstream.ErrStreamNameAlreadyInUse)
}

func TestUpdateKeyValue(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	// cannot update a non-existing bucket
	_, err := js.UpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST", Description: "Test KV"})
	expectErr(t, err, jetstream.ErrBucketNotFound)

	_, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST", Description: "Test KV"})
	expectOk(t, err)

	// update the bucket
	_, err = js.UpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST", Description: "New KV"})
	expectOk(t, err)
}

func TestCreateOrUpdateKeyValue(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	// invalid bucket name
	_, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST.", Description: "Test KV"})
	expectErr(t, err, jetstream.ErrInvalidBucketName)

	_, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST", Description: "Test KV"})
	expectOk(t, err)

	// update the bucket
	_, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST", Description: "New KV"})
	expectOk(t, err)
}

func TestKeyValueHistory(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "LIST", History: 10})
	expectOk(t, err)

	for i := 0; i < 50; i++ {
		age := strconv.FormatUint(uint64(i+22), 10)
		_, err := kv.Put(ctx, "age", []byte(age))
		expectOk(t, err)
	}

	vl, err := kv.History(ctx, "age")
	expectOk(t, err)

	if len(vl) != 10 {
		t.Fatalf("Expected %d values, got %d", 10, len(vl))
	}
	for i, v := range vl {
		if v.Key() != "age" {
			t.Fatalf("Expected key of %q, got %q", "age", v.Key())
		}
		if v.Revision() != uint64(i+41) {
			// History of 10, sent 50..
			t.Fatalf("Expected revision of %d, got %d", i+41, v.Revision())
		}
		age, err := strconv.Atoi(string(v.Value()))
		expectOk(t, err)
		if age != i+62 {
			t.Fatalf("Expected data value of %d, got %d", i+22, age)
		}
	}
}

func TestKeyValueWatch(t *testing.T) {
	expectUpdateF := func(t *testing.T, watcher jetstream.KeyWatcher) func(key, value string, revision uint64) {
		return func(key, value string, revision uint64) {
			t.Helper()
			select {
			case v := <-watcher.Updates():
				if v.Key() != key || string(v.Value()) != value || v.Revision() != revision {
					t.Fatalf("Did not get expected: %q %q %d vs %q %q %d", v.Key(), string(v.Value()), v.Revision(), key, value, revision)
				}
			case <-time.After(time.Second):
				t.Fatalf("Did not receive an update like expected")
			}
		}
	}
	expectDeleteF := func(t *testing.T, watcher jetstream.KeyWatcher) func(key string, revision uint64) {
		return func(key string, revision uint64) {
			t.Helper()
			select {
			case v := <-watcher.Updates():
				if v.Operation() != jetstream.KeyValueDelete {
					t.Fatalf("Expected a delete operation but got %+v", v)
				}
				if v.Revision() != revision {
					t.Fatalf("Did not get expected revision: %d vs %d", revision, v.Revision())
				}
			case <-time.After(time.Second):
				t.Fatalf("Did not receive an update like expected")
			}
		}
	}
	expectPurgeF := func(t *testing.T, watcher jetstream.KeyWatcher) func(key string, revision uint64) {
		return func(key string, revision uint64) {
			t.Helper()
			select {
			case v := <-watcher.Updates():
				if v.Operation() != jetstream.KeyValuePurge {
					t.Fatalf("Expected a delete operation but got %+v", v)
				}
				if v.Revision() != revision {
					t.Fatalf("Did not get expected revision: %d vs %d", revision, v.Revision())
				}
			case <-time.After(time.Second):
				t.Fatalf("Did not receive an update like expected")
			}
		}
	}
	expectInitDoneF := func(t *testing.T, watcher jetstream.KeyWatcher) func() {
		return func() {
			t.Helper()
			select {
			case v := <-watcher.Updates():
				if v != nil {
					t.Fatalf("Did not get expected: %+v", v)
				}
			case <-time.After(time.Second):
				t.Fatalf("Did not receive a init done like expected")
			}
		}
	}

	t.Run("default watcher", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCH"})
		expectOk(t, err)

		watcher, err := kv.WatchAll(ctx)
		expectOk(t, err)
		defer watcher.Stop()

		expectInitDone := expectInitDoneF(t, watcher)
		expectUpdate := expectUpdateF(t, watcher)
		expectDelete := expectDeleteF(t, watcher)
		// Make sure we already got an initial value marker.
		expectInitDone()

		_, err = kv.Create(ctx, "name", []byte("derek"))
		expectOk(t, err)
		expectUpdate("name", "derek", 1)
		_, err = kv.Put(ctx, "name", []byte("rip"))
		expectOk(t, err)
		expectUpdate("name", "rip", 2)
		_, err = kv.Put(ctx, "name", []byte("ik"))
		expectOk(t, err)
		expectUpdate("name", "ik", 3)
		_, err = kv.Put(ctx, "age", []byte("22"))
		expectOk(t, err)
		expectUpdate("age", "22", 4)
		_, err = kv.Put(ctx, "age", []byte("33"))
		expectOk(t, err)
		expectUpdate("age", "33", 5)
		expectOk(t, kv.Delete(ctx, "age"))
		expectDelete("age", 6)

		// Stop first watcher.
		watcher.Stop()

		// Now try wildcard matching and make sure we only get last value when starting.
		_, err = kv.Put(ctx, "t.name", []byte("rip"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "t.name", []byte("ik"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "t.age", []byte("22"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "t.age", []byte("44"))
		expectOk(t, err)

		watcher, err = kv.Watch(ctx, "t.*")
		expectOk(t, err)

		expectInitDone = expectInitDoneF(t, watcher)
		expectUpdate = expectUpdateF(t, watcher)
		expectUpdate("t.name", "ik", 8)
		expectUpdate("t.age", "44", 10)
		expectInitDone()
		watcher.Stop()

		// test watcher with multiple filters
		watcher, err = kv.WatchFiltered(ctx, []string{"t.name", "name"})
		expectOk(t, err)
		expectInitDone = expectInitDoneF(t, watcher)
		expectUpdate = expectUpdateF(t, watcher)
		expectPurge := expectPurgeF(t, watcher)
		expectUpdate("name", "ik", 3)
		expectUpdate("t.name", "ik", 8)
		expectInitDone()
		err = kv.Purge(ctx, "name")
		expectOk(t, err)
		expectPurge("name", 11)
		defer watcher.Stop()
	})

	t.Run("watcher with history included", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCH", History: 64})
		expectOk(t, err)

		_, err = kv.Create(ctx, "name", []byte("derek"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "name", []byte("rip"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "name", []byte("ik"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "age", []byte("22"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "age", []byte("33"))
		expectOk(t, err)
		expectOk(t, kv.Delete(ctx, "age"))

		// when using IncludeHistory(), UpdatesOnly() is not allowed
		if _, err := kv.WatchAll(ctx, jetstream.IncludeHistory(), jetstream.UpdatesOnly()); !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected %v, got %v", jetstream.ErrInvalidOption, err)
		}

		watcher, err := kv.WatchAll(ctx, jetstream.IncludeHistory())
		expectOk(t, err)
		defer watcher.Stop()

		expectInitDone := expectInitDoneF(t, watcher)
		expectUpdate := expectUpdateF(t, watcher)
		expectDelete := expectDeleteF(t, watcher)
		expectUpdate("name", "derek", 1)
		expectUpdate("name", "rip", 2)
		expectUpdate("name", "ik", 3)
		expectUpdate("age", "22", 4)
		expectUpdate("age", "33", 5)
		expectDelete("age", 6)
		expectInitDone()
		_, err = kv.Put(ctx, "name", []byte("pp"))
		expectOk(t, err)
		expectUpdate("name", "pp", 7)

		// Stop first watcher.
		watcher.Stop()

		_, err = kv.Put(ctx, "t.name", []byte("rip"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "t.name", []byte("ik"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "t.age", []byte("22"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "t.age", []byte("44"))
		expectOk(t, err)

		// try wildcard watcher and make sure we get all historical values
		watcher, err = kv.Watch(ctx, "t.*", jetstream.IncludeHistory())
		expectOk(t, err)
		defer watcher.Stop()
		expectInitDone = expectInitDoneF(t, watcher)
		expectUpdate = expectUpdateF(t, watcher)

		expectUpdate("t.name", "rip", 8)
		expectUpdate("t.name", "ik", 9)
		expectUpdate("t.age", "22", 10)
		expectUpdate("t.age", "44", 11)
		expectInitDone()

		_, err = kv.Put(ctx, "t.name", []byte("pp"))
		expectOk(t, err)
		expectUpdate("t.name", "pp", 12)
	})

	t.Run("watcher with updates only", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCH", History: 64})
		expectOk(t, err)

		_, err = kv.Create(ctx, "name", []byte("derek"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "name", []byte("rip"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "age", []byte("22"))
		expectOk(t, err)

		// when using UpdatesOnly(), IncludeHistory() is not allowed
		if _, err := kv.WatchAll(ctx, jetstream.UpdatesOnly(), jetstream.IncludeHistory()); !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected %v, got %v", jetstream.ErrInvalidOption, err)
		}

		watcher, err := kv.WatchAll(ctx, jetstream.UpdatesOnly())
		expectOk(t, err)
		defer watcher.Stop()
		expectUpdate := expectUpdateF(t, watcher)
		expectDelete := expectDeleteF(t, watcher)

		// now update some keys and expect updates
		_, err = kv.Put(ctx, "name", []byte("pp"))
		expectOk(t, err)
		expectUpdate("name", "pp", 4)
		_, err = kv.Put(ctx, "age", []byte("44"))
		expectOk(t, err)
		expectUpdate("age", "44", 5)
		expectOk(t, kv.Delete(ctx, "age"))
		expectDelete("age", 6)

		// Stop first watcher.
		watcher.Stop()

		_, err = kv.Put(ctx, "t.name", []byte("rip"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "t.name", []byte("ik"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "t.age", []byte("22"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "t.age", []byte("44"))
		expectOk(t, err)

		// try wildcard watcher and make sure we do not get any values initially
		watcher, err = kv.Watch(ctx, "t.*", jetstream.UpdatesOnly())
		expectOk(t, err)
		defer watcher.Stop()
		expectUpdate = expectUpdateF(t, watcher)

		// update some keys and expect updates
		_, err = kv.Put(ctx, "t.name", []byte("pp"))
		expectOk(t, err)
		expectUpdate("t.name", "pp", 11)
		_, err = kv.Put(ctx, "t.age", []byte("66"))
		expectOk(t, err)
		expectUpdate("t.age", "66", 12)
	})

	t.Run("watcher with start revision", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCH"})
		expectOk(t, err)

		_, err = kv.Create(ctx, "name", []byte("derek"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "name", []byte("rip"))
		expectOk(t, err)
		_, err = kv.Put(ctx, "age", []byte("22"))
		expectOk(t, err)

		watcher, err := kv.WatchAll(ctx, jetstream.ResumeFromRevision(2))
		expectOk(t, err)
		defer watcher.Stop()

		expectUpdate := expectUpdateF(t, watcher)

		// check that we get only updates after revision 2
		expectUpdate("name", "rip", 2)
		expectUpdate("age", "22", 3)

		// stop first watcher
		watcher.Stop()

		_, err = kv.Put(ctx, "name2", []byte("ik"))
		expectOk(t, err)

		// create a new watcher with start revision 3
		watcher, err = kv.WatchAll(ctx, jetstream.ResumeFromRevision(3))
		expectOk(t, err)
		defer watcher.Stop()

		expectUpdate = expectUpdateF(t, watcher)

		// check that we get only updates after revision 3
		expectUpdate("age", "22", 3)
		expectUpdate("name2", "ik", 4)
	})

	t.Run("invalid watchers", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCH"})
		expectOk(t, err)

		// empty keys
		_, err = kv.Watch(ctx, "")
		expectErr(t, err, jetstream.ErrInvalidKey)

		// invalid key
		_, err = kv.Watch(ctx, "a.>.b")
		expectErr(t, err, jetstream.ErrInvalidKey)

		_, err = kv.Watch(ctx, "foo.")
		expectErr(t, err, jetstream.ErrInvalidKey)

		// conflicting options
		_, err = kv.Watch(ctx, "foo", jetstream.IncludeHistory(), jetstream.UpdatesOnly())
		expectErr(t, err, jetstream.ErrInvalidOption)
	})

	t.Run("filtered watch with no filters", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCH"})
		expectOk(t, err)

		// this should behave like WatchAll
		watcher, err := kv.WatchFiltered(ctx, []string{})
		expectOk(t, err)
		defer watcher.Stop()

		expectInitDone := expectInitDoneF(t, watcher)
		expectUpdate := expectUpdateF(t, watcher)
		expectDelete := expectDeleteF(t, watcher)
		// Make sure we already got an initial value marker.
		expectInitDone()

		_, err = kv.Create(ctx, "name", []byte("derek"))
		expectOk(t, err)
		expectUpdate("name", "derek", 1)
		_, err = kv.Put(ctx, "name", []byte("rip"))
		expectOk(t, err)
		expectUpdate("name", "rip", 2)
		_, err = kv.Put(ctx, "name", []byte("ik"))
		expectOk(t, err)
		expectUpdate("name", "ik", 3)
		_, err = kv.Put(ctx, "age", []byte("22"))
		expectOk(t, err)
		expectUpdate("age", "22", 4)
		_, err = kv.Put(ctx, "age", []byte("33"))
		expectOk(t, err)
		expectUpdate("age", "33", 5)
		expectOk(t, kv.Delete(ctx, "age"))
		expectDelete("age", 6)
	})
}

func TestKeyValueWatchContext(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCHCTX"})
	expectOk(t, err)

	watcher, err := kv.WatchAll(ctx)
	expectOk(t, err)
	defer watcher.Stop()

	// Trigger unsubscribe internally.
	cancel()

	// Wait for a bit for unsubscribe to be done.
	time.Sleep(500 * time.Millisecond)

	// Stopping watch that is already stopped via cancellation propagation is an error.
	err = watcher.Stop()
	if err == nil || err != nats.ErrBadSubscription {
		t.Errorf("Expected invalid subscription, got: %v", err)
	}
}

func TestKeyValueWatchContextUpdates(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCHCTX"})
	expectOk(t, err)

	watcher, err := kv.WatchAll(ctx)
	expectOk(t, err)
	defer watcher.Stop()

	// Pull the initial state done marker which is nil.
	select {
	case v := <-watcher.Updates():
		if v != nil {
			t.Fatalf("Expected nil marker, got %+v", v)
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not receive nil marker like expected")
	}

	// Fire a timer and cancel the context after 250ms.
	time.AfterFunc(250*time.Millisecond, cancel)

	// Make sure canceling will break us out here.
	select {
	case <-watcher.Updates():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not break out like expected")
	}
}

func TestKeyValueBindStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	// Now bind to it..
	_, err = js.KeyValue(ctx, "WATCH")
	expectOk(t, err)

	// Make sure we can't bind to a non-kv style stream.
	// We have some protection with stream name prefix.
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "KV_TEST",
		Subjects: []string{"foo"},
	})
	expectOk(t, err)

	_, err = js.KeyValue(ctx, "TEST")
	expectErr(t, err)
	if err != jetstream.ErrBadBucket {
		t.Fatalf("Expected %v but got %v", jetstream.ErrBadBucket, err)
	}
}

func TestKeyValueDeleteStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	err = js.DeleteKeyValue(ctx, "WATCH")
	expectOk(t, err)

	// delete again should fail
	err = js.DeleteKeyValue(ctx, "WATCH")
	expectErr(t, err, jetstream.ErrBucketNotFound)

	// check that we're backwards compatible
	expectErr(t, err, jetstream.ErrStreamNotFound)

	_, err = js.KeyValue(ctx, "WATCH")
	expectErr(t, err, jetstream.ErrBucketNotFound)
}

func TestKeyValueDeleteVsPurge(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(ctx, key, []byte(value))
		expectOk(t, err)
	}

	// Put in a few names and ages.
	put("name", "derek")
	put("age", "22")
	put("name", "ivan")
	put("age", "33")
	put("name", "rip")
	put("age", "44")

	expectOk(t, kv.Delete(ctx, "age"))
	entries, err := kv.History(ctx, "age")
	expectOk(t, err)
	// Expect three entries and delete marker.
	if len(entries) != 4 {
		t.Fatalf("Expected 4 entries for age after delete, got %d", len(entries))
	}
	err = kv.Purge(ctx, "name", jetstream.LastRevision(4))
	expectErr(t, err)
	err = kv.Purge(ctx, "name", jetstream.LastRevision(5))
	expectOk(t, err)
	// Check marker
	e, err := kv.Get(ctx, "name")
	expectErr(t, err, jetstream.ErrKeyNotFound)
	if e != nil {
		t.Fatalf("Expected a nil entry but got %v", e)
	}
	entries, err = kv.History(ctx, "name")
	expectOk(t, err)
	if len(entries) != 1 {
		t.Fatalf("Expected only 1 entry for age after delete, got %d", len(entries))
	}
	// Make sure history also reports the purge operation.
	if e := entries[0]; e.Operation() != jetstream.KeyValuePurge {
		t.Fatalf("Expected a purge operation but got %v", e.Operation())
	}
}

func TestKeyValueDeleteTombstones(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(ctx, key, []byte(value))
		expectOk(t, err)
	}

	v := strings.Repeat("ABC", 33)
	for i := 1; i <= 100; i++ {
		put(fmt.Sprintf("key-%d", i), v)
	}
	// Now delete them.
	for i := 1; i <= 100; i++ {
		err := kv.Delete(ctx, fmt.Sprintf("key-%d", i))
		expectOk(t, err)
	}

	// Now cleanup.
	err = kv.PurgeDeletes(ctx, jetstream.DeleteMarkersOlderThan(-1))
	expectOk(t, err)

	si, err := js.Stream(ctx, "KV_KVS")
	expectOk(t, err)
	if si.CachedInfo().State.Msgs != 0 {
		t.Fatalf("Expected no stream msgs to be left, got %d", si.CachedInfo().State.Msgs)
	}

	// Try with context
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = kv.PurgeDeletes(nats.Context(ctx))
	expectOk(t, err)
}

func TestKeyValuePurgeDeletesMarkerThreshold(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(ctx, key, []byte(value))
		expectOk(t, err)
	}

	put("foo", "foo1")
	put("bar", "bar1")
	put("foo", "foo2")
	err = kv.Delete(ctx, "foo")
	expectOk(t, err)

	time.Sleep(200 * time.Millisecond)

	err = kv.Delete(ctx, "bar")
	expectOk(t, err)

	err = kv.PurgeDeletes(ctx, jetstream.DeleteMarkersOlderThan(100*time.Millisecond))
	expectOk(t, err)

	// The key foo should have been completely cleared of the data
	// and the delete marker.
	fooEntries, err := kv.History(ctx, "foo")
	if err != jetstream.ErrKeyNotFound {
		t.Fatalf("Expected all entries for key foo to be gone, got err=%v entries=%v", err, fooEntries)
	}
	barEntries, err := kv.History(ctx, "bar")
	expectOk(t, err)
	if len(barEntries) != 1 {
		t.Fatalf("Expected 1 entry, got %v", barEntries)
	}
	if e := barEntries[0]; e.Operation() != jetstream.KeyValueDelete {
		t.Fatalf("Unexpected entry: %+v", e)
	}
}

func TestKeyValueKeys(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "KVS", History: 2})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(ctx, key, []byte(value))
		expectOk(t, err)
	}

	_, err = kv.Keys(ctx)
	expectErr(t, err, jetstream.ErrNoKeysFound)

	// Put in a few names and ages.
	put("name", "derek")
	put("age", "22")
	put("country", "US")
	put("name", "ivan")
	put("age", "33")
	put("country", "US")
	put("name", "rip")
	put("age", "44")
	put("country", "MT")

	keys, err := kv.Keys(ctx)
	expectOk(t, err)

	kmap := make(map[string]struct{})
	for _, key := range keys {
		if _, ok := kmap[key]; ok {
			t.Fatalf("Already saw %q", key)
		}
		kmap[key] = struct{}{}
	}
	if len(kmap) != 3 {
		t.Fatalf("Expected 3 total keys, got %d", len(kmap))
	}
	expected := map[string]struct{}{
		"name":    struct{}{},
		"age":     struct{}{},
		"country": struct{}{},
	}
	if !reflect.DeepEqual(kmap, expected) {
		t.Fatalf("Expected %+v but got %+v", expected, kmap)
	}
	// Make sure delete and purge do the right thing and not return the keys.
	err = kv.Delete(ctx, "name")
	expectOk(t, err)
	err = kv.Purge(ctx, "country")
	expectOk(t, err)

	keys, err = kv.Keys(ctx)
	expectOk(t, err)

	kmap = make(map[string]struct{})
	for _, key := range keys {
		if _, ok := kmap[key]; ok {
			t.Fatalf("Already saw %q", key)
		}
		kmap[key] = struct{}{}
	}
	if len(kmap) != 1 {
		t.Fatalf("Expected 1 total key, got %d", len(kmap))
	}
	if _, ok := kmap["age"]; !ok {
		t.Fatalf("Expected %q to be only key present", "age")
	}
}

func TestKeyValueListKeys(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "KVS", History: 2})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(ctx, key, []byte(value))
		expectOk(t, err)
	}

	// Put in a few names and ages.
	put("name", "derek")
	put("age", "22")
	put("country", "US")
	put("name", "ivan")
	put("age", "33")
	put("country", "US")
	put("name", "rip")
	put("age", "44")
	put("country", "MT")

	keys, err := kv.ListKeys(ctx)
	expectOk(t, err)

	kmap := make(map[string]struct{})
	for key := range keys.Keys() {
		if _, ok := kmap[key]; ok {
			t.Fatalf("Already saw %q", key)
		}
		kmap[key] = struct{}{}
	}
	if len(kmap) != 3 {
		t.Fatalf("Expected 3 total keys, got %d", len(kmap))
	}
	expected := map[string]struct{}{
		"name":    struct{}{},
		"age":     struct{}{},
		"country": struct{}{},
	}
	if !reflect.DeepEqual(kmap, expected) {
		t.Fatalf("Expected %+v but got %+v", expected, kmap)
	}
	// Make sure delete and purge do the right thing and not return the keys.
	err = kv.Delete(ctx, "name")
	expectOk(t, err)
	err = kv.Purge(ctx, "country")
	expectOk(t, err)

	keys, err = kv.ListKeys(ctx)
	expectOk(t, err)

	kmap = make(map[string]struct{})
	for key := range keys.Keys() {
		if _, ok := kmap[key]; ok {
			t.Fatalf("Already saw %q", key)
		}
		kmap[key] = struct{}{}
	}
	if len(kmap) != 1 {
		t.Fatalf("Expected 1 total key, got %d", len(kmap))
	}
	if _, ok := kmap["age"]; !ok {
		t.Fatalf("Expected %q to be only key present", "age")
	}
}

func TestListKeysFiltered(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create Key-Value store.
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "KVS", History: 2})
	expectOk(t, err)

	// Helper function to add key-value pairs.
	putKeys := func(data map[string]string) {
		for key, value := range data {
			t.Helper()
			_, err := kv.Put(ctx, key, []byte(value))
			expectOk(t, err)
		}
	}

	// Add key-value pairs.
	putKeys(map[string]string{
		"apple":  "fruit",
		"banana": "fruit",
		"carrot": "vegetable",
	})

	// Use filters to list keys matching "apple".
	filters := []string{"apple"}
	keyLister, err := kv.ListKeysFiltered(ctx, filters...)
	expectOk(t, err)

	// Collect filtered keys from KeyLister
	var filteredKeys []string
	for key := range keyLister.Keys() {
		filteredKeys = append(filteredKeys, key)
	}

	// Validate expected keys.
	expectedKeys := []string{"apple"}
	if len(filteredKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d filtered key(s), got %d", len(expectedKeys), len(filteredKeys))
	}

	for _, key := range expectedKeys {
		if !contains(filteredKeys, key) {
			t.Fatalf("Expected key %s in filtered keys, but not found", key)
		}
	}
}

func contains(slice []string, key string) bool {
	for _, k := range slice {
		if k == key {
			return true
		}
	}
	return false
}

func TestKeyValueCrossAccounts(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
        jetstream: enabled
        accounts: {
           A: {
               users: [ {user: a, password: a} ]
               jetstream: enabled
               exports: [
                   {service: '$JS.API.>' }
                   {service: '$KV.>'}
                   {stream: 'accI.>'}
               ]
           },
           I: {
               users: [ {user: i, password: i} ]
               imports: [
                   {service: {account: A, subject: '$JS.API.>'}, to: 'fromA.>' }
                   {service: {account: A, subject: '$KV.>'}, to: 'fromA.$KV.>' }
                   {stream: {subject: 'accI.>', account: A}}
               ]
           }
		}`))
	defer os.Remove(conf)
	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	watchNext := func(w jetstream.KeyWatcher) jetstream.KeyValueEntry {
		t.Helper()
		select {
		case e := <-w.Updates():
			return e
		case <-time.After(time.Second):
			t.Fatal("Fail to get the next update")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nc1, js1 := jsClient(t, s, nats.UserInfo("a", "a"))
	defer nc1.Close()

	kv1, err := js1.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w1, err := kv1.Watch(ctx, "map")
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	if e := watchNext(w1); e != nil {
		t.Fatalf("Expected nil entry, got %+v", e)
	}

	nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("i", "i"), nats.CustomInboxPrefix("accI"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()
	js2, err := jetstream.NewWithAPIPrefix(nc2, "fromA")
	if err != nil {
		t.Fatalf("Error getting jetstream context: %v", err)
	}

	kv2, err := js2.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w2, err := kv2.Watch(ctx, "map")
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	if e := watchNext(w2); e != nil {
		t.Fatalf("Expected nil entry, got %+v", e)
	}

	// Do a Put from kv2
	rev, err := kv2.Put(ctx, "map", []byte("value"))
	if err != nil {
		t.Fatalf("Error on put: %v", err)
	}

	// Get from kv1
	e, err := kv1.Get(ctx, "map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get(ctx, "map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Watcher 1
	if e := watchNext(w1); e == nil || e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Watcher 2
	if e := watchNext(w2); e == nil || e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Try an update form kv2
	if _, err := kv2.Update(ctx, "map", []byte("updated"), rev); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Get from kv1
	e, err = kv1.Get(ctx, "map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get(ctx, "map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Watcher 1
	if e := watchNext(w1); e == nil || e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Watcher 2
	if e := watchNext(w2); e == nil || e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Purge from kv2
	if err := kv2.Purge(ctx, "map"); err != nil {
		t.Fatalf("Error on purge: %v", err)
	}

	// Check purge ok from w1
	if e := watchNext(w1); e == nil || e.Operation() != jetstream.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Check purge ok from w2
	if e := watchNext(w2); e == nil || e.Operation() != jetstream.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Delete purge records from kv2
	if err := kv2.PurgeDeletes(ctx, jetstream.DeleteMarkersOlderThan(-1)); err != nil {
		t.Fatalf("Error on purge deletes: %v", err)
	}

	// Check all gone from js1
	if si, err := js1.Stream(ctx, "KV_Map"); err != nil || si == nil || si.CachedInfo().State.Msgs != 0 {
		t.Fatalf("Error getting stream info: err=%v si=%+v", err, si)
	}

	// Delete key from kv2
	if err := kv2.Delete(ctx, "map"); err != nil {
		t.Fatalf("Error on delete: %v", err)
	}

	// Check key gone from kv1
	if e, err := kv1.Get(ctx, "map"); err != jetstream.ErrKeyNotFound || e != nil {
		t.Fatalf("Expected key not found, got err=%v e=%+v", err, e)
	}
}

func TestKeyValueDuplicatesWindow(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	checkWindow := func(ttl, expectedDuplicates time.Duration) {
		t.Helper()

		_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST", History: 5, TTL: ttl})
		expectOk(t, err)
		defer func() { expectOk(t, js.DeleteKeyValue(ctx, "TEST")) }()

		si, err := js.Stream(ctx, "KV_TEST")
		if err != nil {
			t.Fatalf("StreamInfo error: %v", err)
		}
		if si.CachedInfo().Config.Duplicates != expectedDuplicates {
			t.Fatalf("Expected duplicates to be %v, got %v", expectedDuplicates, si.CachedInfo().Config.Duplicates)
		}
	}

	checkWindow(0, 2*time.Minute)
	checkWindow(time.Hour, 2*time.Minute)
	checkWindow(5*time.Second, 5*time.Second)
}

func TestListKeyValueStores(t *testing.T) {
	tests := []struct {
		name       string
		bucketsNum int
	}{
		{
			name:       "single page",
			bucketsNum: 5,
		},
		{
			name:       "multi page",
			bucketsNum: 1025,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			nc, js := jsClient(t, s)
			defer nc.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// create stream without the chunk subject, but with KV_ prefix
			_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "KV_FOO", Subjects: []string{"FOO.*"}})
			expectOk(t, err)
			// create stream with chunk subject, but without "KV_" prefix
			_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "FOO", Subjects: []string{"$KV.ABC.>"}})
			expectOk(t, err)
			for i := 0; i < test.bucketsNum; i++ {
				_, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: fmt.Sprintf("KVS_%d", i), MaxBytes: 1024})
				expectOk(t, err)
			}
			names := make([]string, 0)
			kvNames := js.KeyValueStoreNames(ctx)
			for name := range kvNames.Name() {
				if strings.HasPrefix(name, "KV_") {
					t.Fatalf("Expected name without KV_ prefix, got %q", name)
				}
				names = append(names, name)
			}
			if kvNames.Error() != nil {
				t.Fatalf("Unexpected error: %v", kvNames.Error())
			}
			if len(names) != test.bucketsNum {
				t.Fatalf("Invalid number of stream names; want: %d; got: %d", test.bucketsNum, len(names))
			}
			infos := make([]nats.KeyValueStatus, 0)
			kvInfos := js.KeyValueStores(ctx)
			for info := range kvInfos.Status() {
				infos = append(infos, info)
			}
			if kvInfos.Error() != nil {
				t.Fatalf("Unexpected error: %v", kvNames.Error())
			}
			if len(infos) != test.bucketsNum {
				t.Fatalf("Invalid number of streams; want: %d; got: %d", test.bucketsNum, len(infos))
			}
		})
	}
}

func TestKeyValueMirrorCrossDomains(t *testing.T) {
	keyExists := func(t *testing.T, kv jetstream.KeyValue, key string, expected string) jetstream.KeyValueEntry {
		var e jetstream.KeyValueEntry
		var err error
		checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
			e, err = kv.Get(context.Background(), key)
			if err != nil {
				return err
			}
			if string(e.Value()) != expected {
				return fmt.Errorf("Expected value to be %q, got %q", expected, e.Value())
			}
			return nil
		})

		return e
	}

	keyDeleted := func(t *testing.T, kv jetstream.KeyValue, key string) {
		checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
			_, err := kv.Get(context.Background(), key)
			if err == nil {
				return errors.New("Expected key to be gone")
			}
			if !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
			return nil
		})
	}
	conf := createConfFile(t, []byte(`
		server_name: HUB
		listen: 127.0.0.1:-1
		jetstream: { domain: HUB }
		leafnodes { listen: 127.0.0.1:7422 }
	}`))
	defer os.Remove(conf)
	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	lconf := createConfFile(t, []byte(`
		server_name: LEAF
		listen: 127.0.0.1:-1
 		jetstream: { domain:LEAF }
 		leafnodes {
 		 	remotes = [ { url: "leaf://127.0.0.1" } ]
 		}
	}`))
	defer os.Remove(lconf)
	ln, _ := RunServerWithConfig(lconf)
	defer shutdownJSServerAndRemoveStorage(t, ln)

	// Create main KV on HUB
	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST"})
	expectOk(t, err)

	_, err = kv.PutString(ctx, "name", "derek")
	expectOk(t, err)
	_, err = kv.PutString(ctx, "age", "22")
	expectOk(t, err)
	_, err = kv.PutString(ctx, "v", "v")
	expectOk(t, err)
	err = kv.Delete(ctx, "v")
	expectOk(t, err)

	lnc, ljs := jsClient(t, ln)
	defer lnc.Close()

	// Capture cfg so we can make sure it does not change.
	// NOTE: We use different name to test all possibilities, etc, but in practice for truly nomadic applications
	// this should be named the same, e.g. TEST.
	cfg := jetstream.KeyValueConfig{
		Bucket: "MIRROR",
		Mirror: &jetstream.StreamSource{
			Name:   "TEST",
			Domain: "HUB",
		},
	}
	ccfg := cfg

	_, err = ljs.CreateKeyValue(ctx, cfg)
	expectOk(t, err)

	if !reflect.DeepEqual(cfg, ccfg) {
		t.Fatalf("Did not expect config to be altered: %+v vs %+v", cfg, ccfg)
	}

	si, err := ljs.Stream(ctx, "KV_MIRROR")
	expectOk(t, err)

	// Make sure mirror direct set.
	if !si.CachedInfo().Config.MirrorDirect {
		t.Fatalf("Expected mirror direct to be set")
	}

	// Make sure we sync.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		si, err := ljs.Stream(ctx, "KV_MIRROR")
		expectOk(t, err)
		if si.CachedInfo().State.Msgs == 3 {
			return nil
		}
		return fmt.Errorf("Did not get synched messages: %d", si.CachedInfo().State.Msgs)
	})

	// Bind locally from leafnode and make sure both get and put work.
	mkv, err := ljs.KeyValue(ctx, "MIRROR")
	expectOk(t, err)

	_, err = mkv.PutString(ctx, "name", "rip")
	expectOk(t, err)

	_, err = mkv.PutString(ctx, "v", "vv")
	expectOk(t, err)

	e := keyExists(t, kv, "v", "vv")
	if e.Operation() != jetstream.KeyValuePut {
		t.Fatalf("Got wrong value: %q vs %q", e.Operation(), nats.KeyValuePut)
	}
	err = mkv.Delete(ctx, "v")
	expectOk(t, err)
	keyDeleted(t, kv, "v")

	keyExists(t, kv, "name", "rip")

	// Also make sure we can create a watcher on the mirror KV.
	watcher, err := mkv.WatchAll(ctx)
	expectOk(t, err)
	defer watcher.Stop()

	// Bind through leafnode connection but to origin KV.
	rjs, err := jetstream.NewWithDomain(nc, "HUB")
	expectOk(t, err)

	rkv, err := rjs.KeyValue(ctx, "TEST")
	expectOk(t, err)

	_, err = rkv.PutString(ctx, "name", "ivan")
	expectOk(t, err)

	keyExists(t, mkv, "name", "ivan")
	_, err = rkv.PutString(ctx, "v", "vv")
	expectOk(t, err)
	e = keyExists(t, mkv, "v", "vv")
	if e.Operation() != jetstream.KeyValuePut {
		t.Fatalf("Got wrong value: %q vs %q", e.Operation(), nats.KeyValuePut)
	}
	err = rkv.Delete(ctx, "v")
	expectOk(t, err)
	keyDeleted(t, mkv, "v")

	// Shutdown cluster and test get still work.
	shutdownJSServerAndRemoveStorage(t, s)

	keyExists(t, mkv, "name", "ivan")
}

func TestKeyValueRePublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "TEST_UPDATE",
	}); err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	// This is expected to fail since server does not support as of now
	// the update of RePublish.
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:    "TEST_UPDATE",
		RePublish: &jetstream.RePublish{Source: ">", Destination: "bar.>"},
	}); err == nil {
		t.Fatal("Expected failure, did not get one")
	}

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:    "TEST",
		RePublish: &jetstream.RePublish{Source: ">", Destination: "bar.>"},
	})
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	si, err := js.Stream(ctx, "KV_TEST")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}
	if si.CachedInfo().Config.RePublish == nil {
		t.Fatal("Expected republish to be set, it was not")
	}

	sub, err := nc.SubscribeSync("bar.>")
	if err != nil {
		t.Fatalf("Error on sub: %v", err)
	}
	if _, err := kv.Put(ctx, "foo", []byte("value")); err != nil {
		t.Fatalf("Error on put: %v", err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error on next: %v", err)
	}
	if v := string(msg.Data); v != "value" {
		t.Fatalf("Unexpected value: %s", v)
	}
	// The message should also have a header with the actual subject
	expected := "$KV.TEST.foo"
	if v := msg.Header.Get(jetstream.SubjectHeader); v != expected {
		t.Fatalf("Expected subject header %q, got %q", expected, v)
	}
}

func TestKeyValueMirrorDirectGet(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "TEST"})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:         "MIRROR",
		Mirror:       &jetstream.StreamSource{Name: "KV_TEST"},
		MirrorDirect: true,
	})
	if err != nil {
		t.Fatalf("Error creating mirror: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("KEY.%d", i)
		if _, err := kv.PutString(ctx, key, "42"); err != nil {
			t.Fatalf("Error adding key: %v", err)
		}
	}

	// Make sure all gets work.
	for i := 0; i < 100; i++ {
		if _, err := kv.Get(ctx, "KEY.22"); err != nil {
			t.Fatalf("Got error getting key: %v", err)
		}
	}
}

func TestKeyValueCreate(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:       "TEST",
		Description:  "Test KV",
		MaxValueSize: 128,
		History:      10,
		TTL:          1 * time.Hour,
		MaxBytes:     1024,
		Storage:      jetstream.FileStorage,
	})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	expectedStreamConfig := jetstream.StreamConfig{
		Name:              "KV_TEST",
		Description:       "Test KV",
		Subjects:          []string{"$KV.TEST.>"},
		MaxMsgs:           -1,
		MaxBytes:          1024,
		Discard:           jetstream.DiscardNew,
		MaxAge:            1 * time.Hour,
		MaxMsgsPerSubject: 10,
		MaxMsgSize:        128,
		Storage:           jetstream.FileStorage,
		DenyDelete:        true,
		AllowRollup:       true,
		AllowDirect:       true,
		MaxConsumers:      -1,
		Replicas:          1,
		Duplicates:        2 * time.Minute,
	}

	stream, err := js.Stream(ctx, "KV_TEST")
	if err != nil {
		t.Fatalf("Error getting stream: %v", err)
	}
	// server will set metadata values, so we need to clear them
	stream.CachedInfo().Config.Metadata = nil
	if !reflect.DeepEqual(stream.CachedInfo().Config, expectedStreamConfig) {
		t.Fatalf("Expected stream config to be %+v, got %+v", expectedStreamConfig, stream.CachedInfo().Config)
	}

	_, err = kv.Create(ctx, "key", []byte("1"))
	if err != nil {
		t.Fatalf("Error creating key: %v", err)
	}

	_, err = kv.Create(ctx, "key", []byte("1"))
	expected := "wrong last sequence: 1: key exists"
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("Expected %q, got: %v", expected, err)
	}
	if !errors.Is(err, jetstream.ErrKeyExists) {
		t.Fatalf("Expected ErrKeyExists, got: %v", err)
	}
	aerr := &jetstream.APIError{}
	if !errors.As(err, &aerr) {
		t.Fatalf("Expected APIError, got: %v", err)
	}
	if aerr.Description != "wrong last sequence: 1" {
		t.Fatalf("Unexpected APIError message, got: %v", aerr.Description)
	}
	if aerr.ErrorCode != 10071 {
		t.Fatalf("Unexpected error code, got: %v", aerr.ErrorCode)
	}
	if aerr.Code != jetstream.ErrKeyExists.APIError().Code {
		t.Fatalf("Unexpected error code, got: %v", aerr.Code)
	}
	var kerr jetstream.JetStreamError
	if !errors.As(err, &kerr) {
		t.Fatalf("Expected KeyValueError, got: %v", err)
	}
	if kerr.APIError().ErrorCode != 10071 {
		t.Fatalf("Unexpected error code, got: %v", kerr.APIError().ErrorCode)
	}
}

// Helpers

func client(t *testing.T, s *server.Server, opts ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return nc
}

func jsClient(t *testing.T, s *server.Server, opts ...nats.Option) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc := client(t, s, opts...)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func expectOk(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func expectErr(t *testing.T, err error, expected ...error) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
	if len(expected) == 0 {
		return
	}
	for _, e := range expected {
		if errors.Is(err, e) {
			return
		}
	}
	t.Fatalf("Expected one of %+v, got '%v'", expected, err)
}

func TestKeyValueCompression(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	kvCompressed, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      "A",
		Compression: true,
	})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	status, err := kvCompressed.Status(ctx)
	if err != nil {
		t.Fatalf("Error getting bucket status: %v", err)
	}

	if !status.IsCompressed() {
		t.Fatalf("Expected bucket to be compressed")
	}

	kvStream, err := js.Stream(ctx, "KV_A")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}

	if kvStream.CachedInfo().Config.Compression != jetstream.S2Compression {
		t.Fatalf("Expected stream to be compressed with S2")
	}
}

func TestKeyValueCreateRepairOldKV(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	// create a standard kv
	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "A",
	})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	// get stream config and set discard policy to old and AllowDirect to false
	stream, err := js.Stream(ctx, "KV_A")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}
	streamCfg := stream.CachedInfo().Config
	streamCfg.Discard = jetstream.DiscardOld
	streamCfg.AllowDirect = false

	// create a new kv with the same name - client should fix the config
	_, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "A",
	})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	// get stream config again and check if the discard policy is set to new
	stream, err = js.Stream(ctx, "KV_A")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}
	if stream.CachedInfo().Config.Discard != jetstream.DiscardNew {
		t.Fatalf("Expected stream to have discard policy set to new")
	}
	if !stream.CachedInfo().Config.AllowDirect {
		t.Fatalf("Expected stream to have AllowDirect set to true")
	}

	// attempting to create a new kv with the same name and different settings should fail
	_, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      "A",
		Description: "New KV",
	})
	if !errors.Is(err, jetstream.ErrBucketExists) {
		t.Fatalf("Expected error to be ErrBucketExists, got: %v", err)
	}
}
