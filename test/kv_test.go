// Copyright 2022 The NATS Authors
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
	"fmt"
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

	// Conditional Updates.
	r, err = kv.Update("name", []byte("rip"), 3)
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

func TestKeyValueCrossAccount(t *testing.T) {
	s, _ := RunServerWithConfig("configs/kv_accounts.conf")
	defer shutdown(s)

	setupBucket := func(t *testing.T) {
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("a", "a"))
		expectOk(t, err)
		defer nc.Close()

		js, err := nc.JetStream()
		expectOk(t, err)

		js.DeleteKeyValue("CROSS")

		_, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "CROSS", History: 10})
		expectOk(t, err)
	}

	testAccount := func(t *testing.T, u, p, apiPre, kvPre string) {
		setupBucket(t)

		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo(u, p))
		expectOk(t, err)
		defer nc.Close()

		js, err := nc.JetStream(nats.APIPrefix(apiPre))
		expectOk(t, err)

		kv, err := js.KeyValue("CROSS", nats.BucketSubjectPrefix(kvPre))
		expectOk(t, err)

		_, err = kv.PutString("FOO", "BAR")
		expectOk(t, err)
		v, err := kv.Get("FOO")
		expectOk(t, err)
		if string(v.Value()) != "BAR" {
			t.Fatalf("received wrong value: %q", v.Value())
		}
	}

	testAccount(t, "o", "o", "fromA", "X.cross")
	testAccount(t, "o", "o", "fromA", "X.cross.")
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
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	watcher, err := kv.WatchAll()
	expectOk(t, err)
	defer watcher.Stop()

	expectUpdate := func(key, value string, revision uint64) {
		t.Helper()
		select {
		case v := <-watcher.Updates():
			if v.Key() != key || string(v.Value()) != value || v.Revision() != revision {
				t.Fatalf("Did not get expected: %+v vs %q %q %d", v, key, value, revision)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive an update like expected")
		}
	}
	expectDelete := func(key string, revision uint64) {
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
	expectInitDone := func() {
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
	defer watcher.Stop()

	expectUpdate("t.name", "ik", 8)
	expectUpdate("t.age", "44", 10)
	expectInitDone()
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
	err = kv.Purge("name")
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
	err = kv.PurgeDeletes()
	expectOk(t, err)

	si, err := js.StreamInfo("KV_KVS")
	expectOk(t, err)
	if si.State.Msgs != 0 {
		t.Fatalf("Expected no stream msgs to be left, got %d", si.State.Msgs)
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
		if err == e || strings.Contains(e.Error(), err.Error()) {
			return
		}
	}
	t.Fatalf("Expected one of %+v, got '%v'", expected, err)
}
