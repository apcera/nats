// Copyright 2021-2023 The NATS Authors
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
)

func TestKeyValueBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST", History: 5, TTL: time.Hour})
	expectOk(t, err)

	if kv.Bucket() != "TEST" {
		t.Fatalf("Expected bucket name to be %q, got %q", "TEST", kv.Bucket())
	}

	// Simple Put
	r, err := kv.Put("name", []byte("derek"))
	expectOk(t, err)
	if r != 1 {
		t.Fatalf("Expected 1 for the revision, got %d", r)
	}
	// Simple Get
	e, err := kv.Get("name")
	expectOk(t, err)
	if string(e.Value()) != "derek" {
		t.Fatalf("Got wrong value: %q vs %q", e.Value(), "derek")
	}
	if e.Revision() != 1 {
		t.Fatalf("Expected 1 for the revision, got %d", e.Revision())
	}

	// Delete
	err = kv.Delete("name")
	expectOk(t, err)
	_, err = kv.Get("name")
	expectErr(t, err, nats.ErrKeyNotFound)
	r, err = kv.Create("name", []byte("derek"))
	expectOk(t, err)
	if r != 3 {
		t.Fatalf("Expected 3 for the revision, got %d", r)
	}
	err = kv.Delete("name", nats.LastRevision(4))
	expectErr(t, err)
	err = kv.Delete("name", nats.LastRevision(3))
	expectOk(t, err)

	// Conditional Updates.
	r, err = kv.Update("name", []byte("rip"), 4)
	expectOk(t, err)
	_, err = kv.Update("name", []byte("ik"), 3)
	expectErr(t, err)
	_, err = kv.Update("name", []byte("ik"), r)
	expectOk(t, err)
	r, err = kv.Create("age", []byte("22"))
	expectOk(t, err)
	_, err = kv.Update("age", []byte("33"), r)
	expectOk(t, err)

	// Status
	status, err := kv.Status()
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

	kvs := status.(*nats.KeyValueBucketStatus)
	si := kvs.StreamInfo()
	if si == nil {
		t.Fatalf("StreamInfo not received")
	}
}

func TestKeyValueHistory(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "LIST", History: 10})
	expectOk(t, err)

	for i := 0; i < 50; i++ {
		age := strconv.FormatUint(uint64(i+22), 10)
		_, err := kv.Put("age", []byte(age))
		expectOk(t, err)
	}

	vl, err := kv.History("age")
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
	expectUpdateF := func(t *testing.T, watcher nats.KeyWatcher) func(key, value string, revision uint64) {
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
	expectDeleteF := func(t *testing.T, watcher nats.KeyWatcher) func(key string, revision uint64) {
		return func(key string, revision uint64) {
			t.Helper()
			select {
			case v := <-watcher.Updates():
				if v.Operation() != nats.KeyValueDelete {
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
	expectPurgeF := func(t *testing.T, watcher nats.KeyWatcher) func(key string, revision uint64) {
		return func(key string, revision uint64) {
			t.Helper()
			select {
			case v := <-watcher.Updates():
				if v.Operation() != nats.KeyValuePurge {
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
	expectInitDoneF := func(t *testing.T, watcher nats.KeyWatcher) func() {
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

		kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
		expectOk(t, err)

		watcher, err := kv.WatchAll()
		expectOk(t, err)
		defer watcher.Stop()

		expectInitDone := expectInitDoneF(t, watcher)
		expectUpdate := expectUpdateF(t, watcher)
		expectDelete := expectDeleteF(t, watcher)
		// Make sure we already got an initial value marker.
		expectInitDone()

		kv.Create("name", []byte("derek"))
		expectUpdate("name", "derek", 1)
		kv.Put("name", []byte("rip"))
		expectUpdate("name", "rip", 2)
		kv.Put("name", []byte("ik"))
		expectUpdate("name", "ik", 3)
		kv.Put("age", []byte("22"))
		expectUpdate("age", "22", 4)
		kv.Put("age", []byte("33"))
		expectUpdate("age", "33", 5)
		kv.Delete("age")
		expectDelete("age", 6)

		// Stop first watcher.
		watcher.Stop()

		// Now try wildcard matching and make sure we only get last value when starting.
		kv.Put("t.name", []byte("rip"))
		kv.Put("t.name", []byte("ik"))
		kv.Put("t.age", []byte("22"))
		kv.Put("t.age", []byte("44"))

		watcher, err = kv.Watch("t.*")
		expectOk(t, err)

		expectInitDone = expectInitDoneF(t, watcher)
		expectUpdate = expectUpdateF(t, watcher)
		expectUpdate("t.name", "ik", 8)
		expectUpdate("t.age", "44", 10)
		expectInitDone()
		watcher.Stop()

		// test watcher with multiple filters
		watcher, err = kv.WatchFiltered([]string{"t.name", "name"})
		expectOk(t, err)
		expectInitDone = expectInitDoneF(t, watcher)
		expectUpdate = expectUpdateF(t, watcher)
		expectPurge := expectPurgeF(t, watcher)
		expectUpdate("name", "ik", 3)
		expectUpdate("t.name", "ik", 8)
		expectInitDone()
		err = kv.Purge("name")
		expectOk(t, err)
		expectPurge("name", 11)
		defer watcher.Stop()
	})

	t.Run("watcher with history included", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH", History: 64})
		expectOk(t, err)

		kv.Create("name", []byte("derek"))
		kv.Put("name", []byte("rip"))
		kv.Put("name", []byte("ik"))
		kv.Put("age", []byte("22"))
		kv.Put("age", []byte("33"))
		kv.Delete("age")

		// when using UpdatesOnly(), IncludeHistory() is not allowed
		if _, err := kv.WatchAll(nats.IncludeHistory(), nats.UpdatesOnly()); !strings.Contains(err.Error(), "updates only can not be used with include history") {
			t.Fatalf("Expected error to contain %q, got %q", "updates only can not be used with include history", err)
		}

		watcher, err := kv.WatchAll(nats.IncludeHistory())
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
		kv.Put("name", []byte("pp"))
		expectUpdate("name", "pp", 7)

		// Stop first watcher.
		watcher.Stop()

		kv.Put("t.name", []byte("rip"))
		kv.Put("t.name", []byte("ik"))
		kv.Put("t.age", []byte("22"))
		kv.Put("t.age", []byte("44"))

		// try wildcard watcher and make sure we get all historical values
		watcher, err = kv.Watch("t.*", nats.IncludeHistory())
		expectOk(t, err)
		defer watcher.Stop()
		expectInitDone = expectInitDoneF(t, watcher)
		expectUpdate = expectUpdateF(t, watcher)

		expectUpdate("t.name", "rip", 8)
		expectUpdate("t.name", "ik", 9)
		expectUpdate("t.age", "22", 10)
		expectUpdate("t.age", "44", 11)
		expectInitDone()

		kv.Put("t.name", []byte("pp"))
		expectUpdate("t.name", "pp", 12)
	})

	t.Run("watcher with updates only", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH", History: 64})
		expectOk(t, err)

		kv.Create("name", []byte("derek"))
		kv.Put("name", []byte("rip"))
		kv.Put("age", []byte("22"))

		// when using UpdatesOnly(), IncludeHistory() is not allowed
		if _, err := kv.WatchAll(nats.UpdatesOnly(), nats.IncludeHistory()); !strings.Contains(err.Error(), "include history can not be used with updates only") {
			t.Fatalf("Expected error to contain %q, got %q", "include history can not be used with updates only", err)
		}

		watcher, err := kv.WatchAll(nats.UpdatesOnly())
		expectOk(t, err)
		defer watcher.Stop()
		expectUpdate := expectUpdateF(t, watcher)
		expectDelete := expectDeleteF(t, watcher)

		// now update some keys and expect updates
		kv.Put("name", []byte("pp"))
		expectUpdate("name", "pp", 4)
		kv.Put("age", []byte("44"))
		expectUpdate("age", "44", 5)
		kv.Delete("age")
		expectDelete("age", 6)

		// Stop first watcher.
		watcher.Stop()

		kv.Put("t.name", []byte("rip"))
		kv.Put("t.name", []byte("ik"))
		kv.Put("t.age", []byte("22"))
		kv.Put("t.age", []byte("44"))

		// try wildcard watcher and make sure we do not get any values initially
		watcher, err = kv.Watch("t.*", nats.UpdatesOnly())
		expectOk(t, err)
		defer watcher.Stop()
		expectUpdate = expectUpdateF(t, watcher)

		// update some keys and expect updates
		kv.Put("t.name", []byte("pp"))
		expectUpdate("t.name", "pp", 11)
		kv.Put("t.age", []byte("66"))
		expectUpdate("t.age", "66", 12)
	})

	t.Run("invalid watchers", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
		expectOk(t, err)

		// empty keys
		_, err = kv.Watch("")
		expectErr(t, err, nats.ErrInvalidKey)

		// invalid key
		_, err = kv.Watch("a.>.b")
		expectErr(t, err, nats.ErrInvalidKey)

		_, err = kv.Watch("foo.")
		expectErr(t, err, nats.ErrInvalidKey)
	})

	t.Run("filtered watch with no filters", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
		expectOk(t, err)

		// this should behave like WatchAll
		watcher, err := kv.WatchFiltered([]string{})
		expectOk(t, err)
		defer watcher.Stop()

		expectInitDone := expectInitDoneF(t, watcher)
		expectUpdate := expectUpdateF(t, watcher)
		expectDelete := expectDeleteF(t, watcher)
		// Make sure we already got an initial value marker.
		expectInitDone()

		_, err = kv.Create("name", []byte("derek"))
		expectOk(t, err)
		expectUpdate("name", "derek", 1)
		_, err = kv.Put("name", []byte("rip"))
		expectOk(t, err)
		expectUpdate("name", "rip", 2)
		_, err = kv.Put("name", []byte("ik"))
		expectOk(t, err)
		expectUpdate("name", "ik", 3)
		_, err = kv.Put("age", []byte("22"))
		expectOk(t, err)
		expectUpdate("age", "22", 4)
		_, err = kv.Put("age", []byte("33"))
		expectOk(t, err)
		expectUpdate("age", "33", 5)
		expectOk(t, kv.Delete("age"))
		expectDelete("age", 6)
	})
}

func TestKeyValueWatchContext(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCHCTX"})
	expectOk(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watcher, err := kv.WatchAll(nats.Context(ctx))
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

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCHCTX"})
	expectOk(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watcher, err := kv.WatchAll(nats.Context(ctx))
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

	_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	// Now bind to it..
	_, err = js.KeyValue("WATCH")
	expectOk(t, err)

	// Make sure we can't bind to a non-kv style stream.
	// We have some protection with stream name prefix.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "KV_TEST",
		Subjects: []string{"foo"},
	})
	expectOk(t, err)

	_, err = js.KeyValue("TEST")
	expectErr(t, err)
	if err != nats.ErrBadBucket {
		t.Fatalf("Expected %v but got %v", nats.ErrBadBucket, err)
	}
}

func TestKeyValueDeleteStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	err = js.DeleteKeyValue("WATCH")
	expectOk(t, err)

	_, err = js.KeyValue("WATCH")
	expectErr(t, err)
}

func TestKeyValueDeleteVsPurge(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
		expectOk(t, err)
	}

	// Put in a few names and ages.
	put("name", "derek")
	put("age", "22")
	put("name", "ivan")
	put("age", "33")
	put("name", "rip")
	put("age", "44")

	kv.Delete("age")
	entries, err := kv.History("age")
	expectOk(t, err)
	// Expect three entries and delete marker.
	if len(entries) != 4 {
		t.Fatalf("Expected 4 entries for age after delete, got %d", len(entries))
	}
	err = kv.Purge("name", nats.LastRevision(4))
	expectErr(t, err)
	err = kv.Purge("name", nats.LastRevision(5))
	expectOk(t, err)
	// Check marker
	e, err := kv.Get("name")
	expectErr(t, err, nats.ErrKeyNotFound)
	if e != nil {
		t.Fatalf("Expected a nil entry but got %v", e)
	}
	entries, err = kv.History("name")
	expectOk(t, err)
	if len(entries) != 1 {
		t.Fatalf("Expected only 1 entry for age after delete, got %d", len(entries))
	}
	// Make sure history also reports the purge operation.
	if e := entries[0]; e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Expected a purge operation but got %v", e.Operation())
	}
}

func TestKeyValueDeleteTombstones(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
		expectOk(t, err)
	}

	v := strings.Repeat("ABC", 33)
	for i := 1; i <= 100; i++ {
		put(fmt.Sprintf("key-%d", i), v)
	}
	// Now delete them.
	for i := 1; i <= 100; i++ {
		err := kv.Delete(fmt.Sprintf("key-%d", i))
		expectOk(t, err)
	}

	// Now cleanup.
	err = kv.PurgeDeletes(nats.DeleteMarkersOlderThan(-1))
	expectOk(t, err)

	si, err := js.StreamInfo("KV_KVS")
	expectOk(t, err)
	if si.State.Msgs != 0 {
		t.Fatalf("Expected no stream msgs to be left, got %d", si.State.Msgs)
	}

	// Try with context
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = kv.PurgeDeletes(nats.Context(ctx))
	expectOk(t, err)
}

func TestKeyValuePurgeDeletesMarkerThreshold(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
		expectOk(t, err)
	}

	put("foo", "foo1")
	put("bar", "bar1")
	put("foo", "foo2")
	err = kv.Delete("foo")
	expectOk(t, err)

	time.Sleep(200 * time.Millisecond)

	err = kv.Delete("bar")
	expectOk(t, err)

	err = kv.PurgeDeletes(nats.DeleteMarkersOlderThan(100 * time.Millisecond))
	expectOk(t, err)

	// The key foo should have been completely cleared of the data
	// and the delete marker.
	fooEntries, err := kv.History("foo")
	if err != nats.ErrKeyNotFound {
		t.Fatalf("Expected all entries for key foo to be gone, got err=%v entries=%v", err, fooEntries)
	}
	barEntries, err := kv.History("bar")
	expectOk(t, err)
	if len(barEntries) != 1 {
		t.Fatalf("Expected 1 entry, got %v", barEntries)
	}
	if e := barEntries[0]; e.Operation() != nats.KeyValueDelete {
		t.Fatalf("Unexpected entry: %+v", e)
	}
}

func TestKeyValueKeys(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "KVS", History: 2})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
		expectOk(t, err)
	}

	//lint:ignore SA1019 testing deprecated api
	_, err = kv.Keys()
	expectErr(t, err, nats.ErrNoKeysFound)

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

	//lint:ignore SA1019 testing deprecated api
	keys, err := kv.Keys()
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
	err = kv.Delete("name")
	expectOk(t, err)
	err = kv.Purge("country")
	expectOk(t, err)

	//lint:ignore SA1019 testing deprecated api
	keys, err = kv.Keys()
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

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "KVS", History: 2})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
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

	keys, err := kv.ListKeys()
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
	err = kv.Delete("name")
	expectOk(t, err)
	err = kv.Purge("country")
	expectOk(t, err)

	keys, err = kv.ListKeys()
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

func TestKeyValueCrossAccounts(t *testing.T) {
	conf := createConfFile(t, []byte(`
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

	watchNext := func(w nats.KeyWatcher) nats.KeyValueEntry {
		t.Helper()
		select {
		case e := <-w.Updates():
			return e
		case <-time.After(time.Second):
			t.Fatal("Fail to get the next update")
		}
		return nil
	}

	nc1, js1 := jsClient(t, s, nats.UserInfo("a", "a"))
	defer nc1.Close()

	kv1, err := js1.CreateKeyValue(&nats.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w1, err := kv1.Watch("map")
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
	js2, err := nc2.JetStream(nats.APIPrefix("fromA"))
	if err != nil {
		t.Fatalf("Error getting jetstream context: %v", err)
	}

	kv2, err := js2.CreateKeyValue(&nats.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w2, err := kv2.Watch("map")
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	if e := watchNext(w2); e != nil {
		t.Fatalf("Expected nil entry, got %+v", e)
	}

	// Do a Put from kv2
	rev, err := kv2.Put("map", []byte("value"))
	if err != nil {
		t.Fatalf("Error on put: %v", err)
	}

	// Get from kv1
	e, err := kv1.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get("map")
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
	if _, err := kv2.Update("map", []byte("updated"), rev); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Get from kv1
	e, err = kv1.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get("map")
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
	if err := kv2.Purge("map"); err != nil {
		t.Fatalf("Error on purge: %v", err)
	}

	// Check purge ok from w1
	if e := watchNext(w1); e == nil || e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Check purge ok from w2
	if e := watchNext(w2); e == nil || e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Delete purge records from kv2
	if err := kv2.PurgeDeletes(nats.DeleteMarkersOlderThan(-1)); err != nil {
		t.Fatalf("Error on purge deletes: %v", err)
	}

	// Check all gone from js1
	if si, err := js1.StreamInfo("KV_Map"); err != nil || si == nil || si.State.Msgs != 0 {
		t.Fatalf("Error getting stream info: err=%v si=%+v", err, si)
	}

	// Delete key from kv2
	if err := kv2.Delete("map"); err != nil {
		t.Fatalf("Error on delete: %v", err)
	}

	// Check key gone from kv1
	if e, err := kv1.Get("map"); err != nats.ErrKeyNotFound || e != nil {
		t.Fatalf("Expected key not found, got err=%v e=%+v", err, e)
	}
}

func TestKeyValueDuplicatesWindow(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	checkWindow := func(ttl, expectedDuplicates time.Duration) {
		t.Helper()

		_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST", History: 5, TTL: ttl})
		expectOk(t, err)
		defer js.DeleteKeyValue("TEST")

		si, err := js.StreamInfo("KV_TEST")
		if err != nil {
			t.Fatalf("StreamInfo error: %v", err)
		}
		if si.Config.Duplicates != expectedDuplicates {
			t.Fatalf("Expected duplicates to be %v, got %v", expectedDuplicates, si.Config.Duplicates)
		}
	}

	checkWindow(0, 2*time.Minute)
	checkWindow(time.Hour, 2*time.Minute)
	checkWindow(5*time.Second, 5*time.Second)
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

func jsClient(t *testing.T, s *server.Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc := client(t, s, opts...)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
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
			// create stream without the chunk subject, but with KV_ prefix
			_, err := js.AddStream(&nats.StreamConfig{Name: "KV_FOO", Subjects: []string{"FOO.*"}})
			expectOk(t, err)
			// create stream with chunk subject, but without "KV_" prefix
			_, err = js.AddStream(&nats.StreamConfig{Name: "FOO", Subjects: []string{"$KV.ABC.>"}})
			expectOk(t, err)
			for i := 0; i < test.bucketsNum; i++ {
				_, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: fmt.Sprintf("KVS_%d", i), MaxBytes: 1024})
				expectOk(t, err)
			}
			names := make([]string, 0)
			for name := range js.KeyValueStoreNames() {
				if strings.HasPrefix(name, "KV_") {
					t.Fatalf("Expected name without KV_ prefix, got %q", name)
				}
				names = append(names, name)
			}
			if len(names) != test.bucketsNum {
				t.Fatalf("Invalid number of stream names; want: %d; got: %d", test.bucketsNum, len(names))
			}
			infos := make([]nats.KeyValueStatus, 0)
			for info := range js.KeyValueStores() {
				infos = append(infos, info)
			}
			if len(infos) != test.bucketsNum {
				t.Fatalf("Invalid number of streams; want: %d; got: %d", test.bucketsNum, len(infos))
			}
		})
	}
}

func TestKeyValueMirrorCrossDomains(t *testing.T) {
	keyExists := func(t *testing.T, kv nats.KeyValue, key string, expected string) nats.KeyValueEntry {
		var e nats.KeyValueEntry
		var err error
		checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
			e, err = kv.Get(key)
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

	keyDeleted := func(t *testing.T, kv nats.KeyValue, key string) {
		checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
			_, err := kv.Get(key)
			if err == nil {
				return errors.New("Expected key to be gone")
			}
			if !errors.Is(err, nats.ErrKeyNotFound) {
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

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST"})
	expectOk(t, err)

	_, err = kv.PutString("name", "derek")
	expectOk(t, err)
	_, err = kv.PutString("age", "22")
	expectOk(t, err)
	_, err = kv.PutString("v", "v")
	expectOk(t, err)
	err = kv.Delete("v")
	expectOk(t, err)

	lnc, ljs := jsClient(t, ln)
	defer lnc.Close()

	// Capture cfg so we can make sure it does not change.
	// NOTE: We use different name to test all possibilities, etc, but in practice for truly nomadic applications
	// this should be named the same, e.g. TEST.
	cfg := &nats.KeyValueConfig{
		Bucket: "MIRROR",
		Mirror: &nats.StreamSource{
			Name:   "TEST",
			Domain: "HUB",
		},
	}
	ccfg := *cfg

	_, err = ljs.CreateKeyValue(cfg)
	expectOk(t, err)

	if !reflect.DeepEqual(cfg, &ccfg) {
		t.Fatalf("Did not expect config to be altered: %+v vs %+v", cfg, ccfg)
	}

	si, err := ljs.StreamInfo("KV_MIRROR")
	expectOk(t, err)

	// Make sure mirror direct set.
	if !si.Config.MirrorDirect {
		t.Fatalf("Expected mirror direct to be set")
	}

	// Make sure we sync.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		si, err := ljs.StreamInfo("KV_MIRROR")
		expectOk(t, err)
		if si.State.Msgs == 3 {
			return nil
		}
		return fmt.Errorf("Did not get synched messages: %d", si.State.Msgs)
	})

	// Bind locally from leafnode and make sure both get and put work.
	mkv, err := ljs.KeyValue("MIRROR")
	expectOk(t, err)

	_, err = mkv.PutString("name", "rip")
	expectOk(t, err)

	_, err = mkv.PutString("v", "vv")
	expectOk(t, err)
	// wait for the key to be propagated to the mirror
	e := keyExists(t, kv, "v", "vv")
	if e.Operation() != nats.KeyValuePut {
		t.Fatalf("Got wrong value: %q vs %q", e.Operation(), nats.KeyValuePut)
	}
	err = mkv.Delete("v")
	expectOk(t, err)
	keyDeleted(t, kv, "v")

	keyExists(t, kv, "name", "rip")

	// Also make sure we can create a watcher on the mirror KV.
	watcher, err := mkv.WatchAll()
	expectOk(t, err)
	defer watcher.Stop()

	// Bind through leafnode connection but to origin KV.
	rjs, err := lnc.JetStream(nats.Domain("HUB"))
	expectOk(t, err)

	rkv, err := rjs.KeyValue("TEST")
	expectOk(t, err)

	_, err = rkv.PutString("name", "ivan")
	expectOk(t, err)

	e = keyExists(t, mkv, "name", "ivan")
	_, err = rkv.PutString("v", "vv")
	expectOk(t, err)
	keyExists(t, mkv, "v", "vv")
	if e.Operation() != nats.KeyValuePut {
		t.Fatalf("Got wrong value: %q vs %q", e.Operation(), nats.KeyValuePut)
	}
	err = rkv.Delete("v")
	expectOk(t, err)
	keyDeleted(t, mkv, "v")

	// Shutdown cluster and test get still work.
	shutdownJSServerAndRemoveStorage(t, s)

	keyExists(t, mkv, "name", "ivan")
}

func TestKeyValueNonDirectGet(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST"})
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	si, err := js.StreamInfo("KV_TEST")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}
	if !si.Config.AllowDirect {
		t.Fatal("Expected allow direct to be set, it was not")
	}

	cfg := si.Config
	cfg.AllowDirect = false
	if _, err := js.UpdateStream(&cfg); err != nil {
		t.Fatalf("Error updating stream: %v", err)
	}
	kvi, err := js.KeyValue("TEST")
	if err != nil {
		t.Fatalf("Error getting kv: %v", err)
	}

	if _, err := kvi.PutString("key1", "val1"); err != nil {
		t.Fatalf("Error putting key: %v", err)
	}
	if _, err := kvi.PutString("key2", "val2"); err != nil {
		t.Fatalf("Error putting key: %v", err)
	}
	if v, err := kvi.Get("key2"); err != nil || string(v.Value()) != "val2" {
		t.Fatalf("Error on get: v=%+v err=%v", v, err)
	}
	if v, err := kvi.GetRevision("key1", 1); err != nil || string(v.Value()) != "val1" {
		t.Fatalf("Error on get revisiong: v=%+v err=%v", v, err)
	}
	if v, err := kvi.GetRevision("key1", 2); err == nil {
		t.Fatalf("Expected error, got %+v", v)
	}
}

func TestKeyValueRePublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "TEST_UPDATE",
	}); err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	// This is expected to fail since server does not support as of now
	// the update of RePublish.
	if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:    "TEST_UPDATE",
		RePublish: &nats.RePublish{Source: ">", Destination: "bar.>"},
	}); err == nil {
		t.Fatal("Expected failure, did not get one")
	}

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:    "TEST",
		RePublish: &nats.RePublish{Source: ">", Destination: "bar.>"},
	})
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	si, err := js.StreamInfo("KV_TEST")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}
	if si.Config.RePublish == nil {
		t.Fatal("Expected republish to be set, it was not")
	}

	sub, err := nc.SubscribeSync("bar.>")
	if err != nil {
		t.Fatalf("Error on sub: %v", err)
	}
	if _, err := kv.Put("foo", []byte("value")); err != nil {
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
	kvSubjectsPreTmpl := "$KV.%s."
	expected := fmt.Sprintf(kvSubjectsPreTmpl, "TEST") + "foo"
	if v := msg.Header.Get(nats.JSSubject); v != expected {
		t.Fatalf("Expected subject header %q, got %q", expected, v)
	}
}

func TestKeyValueMirrorDirectGet(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST"})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:         "MIRROR",
		Mirror:       &nats.StreamSource{Name: "KV_TEST"},
		MirrorDirect: true,
	})
	if err != nil {
		t.Fatalf("Error creating mirror: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("KEY.%d", i)
		if _, err := kv.PutString(key, "42"); err != nil {
			t.Fatalf("Error adding key: %v", err)
		}
	}

	// Make sure all gets work.
	for i := 0; i < 100; i++ {
		if _, err := kv.Get("KEY.22"); err != nil {
			t.Fatalf("Got error getting key: %v", err)
		}
	}
}

func TestKeyValueCreate(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       "TEST",
		Description:  "Test KV",
		MaxValueSize: 128,
		History:      10,
		TTL:          1 * time.Hour,
		MaxBytes:     1024,
		Storage:      nats.FileStorage,
	})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	expectedStreamConfig := nats.StreamConfig{
		Name:              "KV_TEST",
		Description:       "Test KV",
		Subjects:          []string{"$KV.TEST.>"},
		MaxMsgs:           -1,
		MaxBytes:          1024,
		Discard:           nats.DiscardNew,
		MaxAge:            1 * time.Hour,
		MaxMsgsPerSubject: 10,
		MaxMsgSize:        128,
		Storage:           nats.FileStorage,
		DenyDelete:        true,
		AllowRollup:       true,
		AllowDirect:       true,
		MaxConsumers:      -1,
		Replicas:          1,
		Duplicates:        2 * time.Minute,
	}

	si, err := js.StreamInfo("KV_TEST")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}
	if !reflect.DeepEqual(si.Config, expectedStreamConfig) {
		t.Fatalf("Expected stream config to be %+v, got %+v", expectedStreamConfig, si.Config)
	}

	_, err = kv.Create("key", []byte("1"))
	if err != nil {
		t.Fatalf("Error creating key: %v", err)
	}

	_, err = kv.Create("key", []byte("1"))
	expected := "nats: wrong last sequence: 1: key exists"
	if err.Error() != expected {
		t.Fatalf("Expected %q, got: %v", expected, err)
	}
	if !errors.Is(err, nats.ErrKeyExists) {
		t.Fatalf("Expected ErrKeyExists, got: %v", err)
	}
	aerr := &nats.APIError{}
	if !errors.As(err, &aerr) {
		t.Fatalf("Expected APIError, got: %v", err)
	}
	if aerr.Description != "wrong last sequence: 1" {
		t.Fatalf("Unexpected APIError message, got: %v", aerr.Description)
	}
	if aerr.ErrorCode != 10071 {
		t.Fatalf("Unexpected error code, got: %v", aerr.ErrorCode)
	}
	if aerr.Code != nats.ErrKeyExists.APIError().Code {
		t.Fatalf("Unexpected error code, got: %v", aerr.Code)
	}
	var kerr nats.JetStreamError
	if !errors.As(err, &kerr) {
		t.Fatalf("Expected KeyValueError, got: %v", err)
	}
	if kerr.APIError().ErrorCode != 10071 {
		t.Fatalf("Unexpected error code, got: %v", kerr.APIError().ErrorCode)
	}
}

func TestKeyValueSourcing(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kvA, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "A"})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	_, err = kvA.Create("keyA", []byte("1"))
	if err != nil {
		t.Fatalf("Error creating key: %v", err)
	}

	if _, err := kvA.Get("keyA"); err != nil {
		t.Fatalf("Got error getting keyA from A: %v", err)
	}

	kvB, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "B"})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	_, err = kvB.Create("keyB", []byte("1"))
	if err != nil {
		t.Fatalf("Error creating key: %v", err)
	}

	kvC, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "C", Sources: []*nats.StreamSource{{Name: "A"}, {Name: "B"}}})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	i := 0
	for {
		status, err := kvC.Status()
		if err != nil {
			t.Fatalf("Error getting bucket status: %v", err)
		}
		if status.Values() == 2 {
			break
		} else {
			i++
			if i > 10 {
				t.Fatalf("Error sourcing bucket does not contain the expected number of values")
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if _, err := kvC.Get("keyA"); err != nil {
		t.Fatalf("Got error getting keyA from C: %v", err)
	}

	if _, err := kvC.Get("keyB"); err != nil {
		t.Fatalf("Got error getting keyB from C: %v", err)
	}
}

func TestKeyValueCompression(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      "A",
		Compression: true,
	})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	status, err := kv.Status()
	if err != nil {
		t.Fatalf("Error getting bucket status: %v", err)
	}

	if !status.IsCompressed() {
		t.Fatalf("Expected bucket to be compressed")
	}

	kvStream, err := js.StreamInfo("KV_A")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}

	if kvStream.Config.Compression != nats.S2Compression {
		t.Fatalf("Expected stream to be compressed with S2")
	}
}
