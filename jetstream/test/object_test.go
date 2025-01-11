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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestObjectBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	_, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "notok!", Description: "testing"})
	expectErr(t, err, jetstream.ErrInvalidStoreName)

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "OBJS", Description: "testing"})
	expectOk(t, err)

	// Create ~16MB object.
	blob := make([]byte, 16*1024*1024+22)
	_, err = rand.Read(blob)
	expectOk(t, err)

	now := time.Now().UTC().Round(time.Second)
	_, err = obs.PutBytes(ctx, "BLOB", blob)
	expectOk(t, err)

	// Test info
	info, err := obs.GetInfo(ctx, "BLOB")
	expectOk(t, err)
	if len(info.NUID) == 0 {
		t.Fatalf("Expected object to have a NUID")
	}
	if info.ModTime.IsZero() {
		t.Fatalf("Expected object to have a non-zero ModTime")
	}
	if mt := info.ModTime.Round(time.Second); mt.Sub(now) != 0 && mt.Sub(now) != time.Second {
		t.Fatalf("Expected ModTime to be about %v, got %v", now, mt)
	}

	// Make sure the stream is sealed.
	err = obs.Seal(ctx)
	expectOk(t, err)
	si, err := js.Stream(ctx, "OBJ_OBJS")
	expectOk(t, err)
	if !si.CachedInfo().Config.Sealed {
		t.Fatalf("Expected the object stream to be sealed, got %+v", si)
	}

	status, err := obs.Status(ctx)
	expectOk(t, err)
	if !status.Sealed() {
		t.Fatalf("expected sealed status")
	}
	if status.Size() == 0 {
		t.Fatalf("size is 0")
	}
	if status.Storage() != jetstream.FileStorage {
		t.Fatalf("stauts reports %d storage", status.Storage())
	}
	if status.Description() != "testing" {
		t.Fatalf("invalid description: '%s'", status.Description())
	}

	// Now get the object back.
	result, err := obs.Get(ctx, "BLOB")
	expectOk(t, err)
	expectOk(t, result.Error())
	defer result.Close()

	// Check info.
	info, err = result.Info()
	expectOk(t, err)
	if info.Size != uint64(len(blob)) {
		t.Fatalf("Size does not match, %d vs %d", info.Size, len(blob))
	}

	// Check result.
	copy, err := io.ReadAll(result)
	expectOk(t, err)
	if !bytes.Equal(copy, blob) {
		t.Fatalf("Result not the same")
	}

	// Check simple errors.
	_, err = obs.Get(ctx, "FOO")
	expectErr(t, err, jetstream.ErrObjectNotFound)

	_, err = obs.Get(ctx, "")
	expectErr(t, err, jetstream.ErrNameRequired)

	_, err = obs.PutBytes(ctx, "", blob)
	expectErr(t, err, jetstream.ErrBadObjectMeta)

	// Test delete.
	err = js.DeleteObjectStore(ctx, "OBJS")
	expectOk(t, err)
	_, err = js.ObjectStore(ctx, "BLOB")
	expectErr(t, err, jetstream.ErrBucketNotFound)
}

func TestCreateObjectStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	// invalid bucket name
	_, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST.", Description: "Test store"})
	expectErr(t, err, jetstream.ErrInvalidStoreName)

	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST", Description: "Test store"})
	expectOk(t, err)

	// Check that we can't overwrite existing bucket.
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST", Description: "New store"})
	expectErr(t, err, jetstream.ErrBucketExists)

	// assert that we're backwards compatible
	expectErr(t, err, jetstream.ErrStreamNameAlreadyInUse)
}

func TestUpdateObjectStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	// cannot update a non-existing bucket
	_, err := js.UpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST", Description: "Test store"})
	expectErr(t, err, jetstream.ErrBucketNotFound)

	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST", Description: "Test store"})
	expectOk(t, err)

	// update the bucket
	_, err = js.UpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST", Description: "New store"})
	expectOk(t, err)
}

func TestCreateOrUpdateObjectStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	// invalid bucket name
	_, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST.", Description: "Test store"})
	expectErr(t, err, jetstream.ErrInvalidStoreName)

	_, err = js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST", Description: "Test store"})
	expectOk(t, err)

	// update the bucket
	_, err = js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST", Description: "New store"})
	expectOk(t, err)
}

func TestGetObjectDigestMismatch(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "FOO"})
	expectOk(t, err)

	_, err = obs.PutString(ctx, "A", "abc")
	expectOk(t, err)
	res, err := obs.Get(ctx, "A")
	expectOk(t, err)
	// first read should be successful
	data, err := io.ReadAll(res)
	expectOk(t, err)
	if string(data) != "abc" {
		t.Fatalf("Expected result: 'abc'; got: %s", string(data))
	}

	info, err := obs.GetInfo(ctx, "A")
	expectOk(t, err)

	// add new chunk after using Put(), this will change the digest hash on Get()
	_, err = js.Publish(ctx, fmt.Sprintf("$O.FOO.C.%s", info.NUID), []byte("123"))
	expectOk(t, err)

	res, err = obs.Get(ctx, "A")
	expectOk(t, err)
	_, err = io.ReadAll(res)
	expectErr(t, err, jetstream.ErrDigestMismatch)
	expectErr(t, res.Error(), jetstream.ErrDigestMismatch)
}

func TestDefaultObjectStatus(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "OBJS", Description: "testing"})
	expectOk(t, err)

	blob := make([]byte, 1024*1024+22)
	_, err = rand.Read(blob)
	expectOk(t, err)

	_, err = obs.PutBytes(ctx, "BLOB", blob)
	expectOk(t, err)

	status, err := obs.Status(ctx)
	expectOk(t, err)
	if status.BackingStore() != "JetStream" {
		t.Fatalf("invalid backing store kind: %s", status.BackingStore())
	}
	bs := status.(*jetstream.ObjectBucketStatus)
	info := bs.StreamInfo()
	if info.Config.Name != "OBJ_OBJS" {
		t.Fatalf("invalid stream name %+v", info)
	}
}

func TestObjectFileBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "FILES"})
	expectOk(t, err)

	// Create ~8MB object.
	blob := make([]byte, 8*1024*1024+33)
	_, err = rand.Read(blob)
	expectOk(t, err)

	tmpFile, err := os.CreateTemp("", "objfile")
	expectOk(t, err)
	defer os.Remove(tmpFile.Name()) // clean up
	err = os.WriteFile(tmpFile.Name(), blob, 0600)
	expectOk(t, err)

	_, err = obs.PutFile(ctx, tmpFile.Name())
	expectOk(t, err)

	tmpResult, err := os.CreateTemp("", "objfileresult")
	expectOk(t, err)
	defer os.Remove(tmpResult.Name()) // clean up

	err = obs.GetFile(ctx, tmpFile.Name(), tmpResult.Name())
	expectOk(t, err)

	// Make sure they are the same.
	original, err := os.ReadFile(tmpFile.Name())
	expectOk(t, err)

	restored, err := os.ReadFile(tmpResult.Name())
	expectOk(t, err)

	if !bytes.Equal(original, restored) {
		t.Fatalf("Files did not match")
	}
}

func TestObjectMulti(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "TEST_FILES"})
	expectOk(t, err)

	numFiles := 0
	fis, _ := os.ReadDir(".")
	for _, fi := range fis {
		fn := fi.Name()
		// Just grab clean test files.
		if filepath.Ext(fn) != ".go" || fn[0] == '.' || fn[0] == '#' {
			continue
		}
		_, err = obs.PutFile(ctx, fn)
		expectOk(t, err)
		numFiles++
	}
	expectOk(t, obs.Seal(ctx))

	_, err = js.Stream(ctx, "OBJ_TEST_FILES")
	expectOk(t, err)

	result, err := obs.Get(ctx, "object_test.go")
	expectOk(t, err)
	expectOk(t, result.Error())
	defer result.Close()

	_, err = result.Info()
	expectOk(t, err)

	copy, err := io.ReadAll(result)
	expectOk(t, err)

	orig, err := os.ReadFile(path.Join(".", "object_test.go"))
	expectOk(t, err)

	if !bytes.Equal(orig, copy) {
		t.Fatalf("Files did not match")
	}
}

func TestObjectDeleteMarkers(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	msg := bytes.Repeat([]byte("A"), 100)
	_, err = obs.PutBytes(ctx, "A", msg)
	expectOk(t, err)

	err = obs.Delete(ctx, "A")
	expectOk(t, err)

	si, err := js.Stream(ctx, "OBJ_OBJS")
	expectOk(t, err)

	// We should have one message left, the "delete" marker.
	if si.CachedInfo().State.Msgs != 1 {
		t.Fatalf("Expected 1 marker msg, got %d msgs", si.CachedInfo().State.Msgs)
	}
	// For deleted object return error
	_, err = obs.GetInfo(ctx, "A")
	expectErr(t, err, jetstream.ErrObjectNotFound)
	_, err = obs.Get(ctx, "A")
	expectErr(t, err, jetstream.ErrObjectNotFound)

	info, err := obs.GetInfo(ctx, "A", jetstream.GetObjectInfoShowDeleted())
	expectOk(t, err)
	// Make sure we have a delete marker, this will be there to drive Watch functionality.
	if !info.Deleted {
		t.Fatalf("Expected info to be marked as deleted")
	}
	_, err = obs.Get(ctx, "A", jetstream.GetObjectShowDeleted())
	expectOk(t, err)
}

func TestObjectMultiWithDelete(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "2OD"})
	expectOk(t, err)

	pa := bytes.Repeat([]byte("A"), 2_000_000)
	pb := bytes.Repeat([]byte("B"), 3_000_000)

	_, err = obs.PutBytes(ctx, "A", pa)
	expectOk(t, err)

	// Hold onto this so we can make sure DeleteObject clears all messages, chunks and meta.
	si, err := js.Stream(ctx, "OBJ_2OD")
	expectOk(t, err)

	_, err = obs.PutBytes(ctx, "B", pb)
	expectOk(t, err)

	pb2, err := obs.GetBytes(ctx, "B")
	expectOk(t, err)

	if !bytes.Equal(pb, pb2) {
		t.Fatalf("Did not retrieve same object")
	}

	// Now delete B
	err = obs.Delete(ctx, "B")
	expectOk(t, err)

	siad, err := js.Stream(ctx, "OBJ_2OD")
	expectOk(t, err)
	if siad.CachedInfo().State.Msgs != si.CachedInfo().State.Msgs+1 { // +1 more delete marker.
		t.Fatalf("Expected to have %d msgs after delete, got %d", siad.CachedInfo().State.Msgs, si.CachedInfo().State.Msgs+1)
	}
}

func TestObjectNames(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	// Test filename like naming.
	_, err = obs.PutString(ctx, "BLOB.txt", "A")
	expectOk(t, err)

	// Spaces ok
	_, err = obs.PutString(ctx, "foo bar", "A")
	expectOk(t, err)

	// things that can be in a filename across multiple OSes
	// dot, asterisk, lt, gt, colon, double-quote, fwd-slash, backslash, pipe, question-mark, ampersand
	_, err = obs.PutString(ctx, ".*<>:\"/\\|?&", "A")
	expectOk(t, err)

	// Errors
	_, err = obs.PutString(ctx, "", "A")
	expectErr(t, err, jetstream.ErrBadObjectMeta)
}

func TestObjectMetadata(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	bucketMetadata := map[string]string{"foo": "bar", "baz": "boo"}
	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:   "META-TEST",
		Metadata: bucketMetadata,
	})
	expectOk(t, err)
	status, err := obs.Status(ctx)
	expectOk(t, err)
	for k, v := range bucketMetadata {
		if status.Metadata()[k] != v {
			t.Fatalf("invalid bucket metadata: %+v", status.Metadata())
		}
	}

	// Simple with no Meta.
	_, err = obs.PutString(ctx, "A", "AAA")
	expectOk(t, err)
	buf := bytes.NewBufferString("CCC")
	objectMetadata := map[string]string{"name": "C", "description": "descC"}
	info, err := obs.Put(ctx, jetstream.ObjectMeta{Name: "C", Metadata: objectMetadata}, buf)
	expectOk(t, err)
	if !reflect.DeepEqual(info.Metadata, objectMetadata) {
		t.Fatalf("invalid object metadata: %+v", info.Metadata)
	}

	meta := jetstream.ObjectMeta{Name: "A"}
	meta.Description = "descA"
	meta.Headers = make(nats.Header)
	meta.Headers.Set("color", "blue")
	objectMetadata["description"] = "updated desc"
	objectMetadata["version"] = "0.1"
	meta.Metadata = objectMetadata

	// simple update that does not change the name, just adds data
	err = obs.UpdateMeta(ctx, "A", meta)
	expectOk(t, err)

	info, err = obs.GetInfo(ctx, "A")
	expectOk(t, err)
	if info.Name != "A" || info.Description != "descA" || info.Headers == nil || info.Headers.Get("color") != "blue" ||
		!reflect.DeepEqual(info.Metadata, objectMetadata) {
		t.Fatalf("Update failed: %+v", info)
	}

	// update that changes the name and some data
	meta = jetstream.ObjectMeta{Name: "B"}
	meta.Description = "descB"
	meta.Headers = make(nats.Header)
	meta.Headers.Set("color", "red")
	meta.Metadata = nil

	err = obs.UpdateMeta(ctx, "A", meta)
	expectOk(t, err)

	_, err = obs.GetInfo(ctx, "A")
	if err == nil {
		t.Fatal("Object meta for original name was not removed.")
	}

	info, err = obs.GetInfo(ctx, "B")
	expectOk(t, err)
	if info.Name != "B" || info.Description != "descB" || info.Headers == nil || info.Headers.Get("color") != "red" || info.Metadata != nil {
		t.Fatalf("Update failed: %+v", info)
	}

	// Change meta name to existing object's name
	meta = jetstream.ObjectMeta{Name: "C"}

	err = obs.UpdateMeta(ctx, "B", meta)
	expectErr(t, err, jetstream.ErrObjectAlreadyExists)

	err = obs.Delete(ctx, "C")
	expectOk(t, err)
	err = obs.UpdateMeta(ctx, "B", meta)
	expectOk(t, err)

	// delete the object to test updating against a deleted object
	err = obs.Delete(ctx, "C")
	expectOk(t, err)
	err = obs.UpdateMeta(ctx, "C", meta)
	expectErr(t, err, jetstream.ErrUpdateMetaDeleted)

	err = obs.UpdateMeta(ctx, "X", meta)
	if err == nil {
		t.Fatal("Expected an error when trying to update an object that does not exist.")
	}

	// can't have a link when putting an object
	meta.Opts = &jetstream.ObjectMetaOptions{Link: &jetstream.ObjectLink{Bucket: "DoesntMatter"}}
	_, err = obs.Put(ctx, meta, nil)
	expectErr(t, err, jetstream.ErrLinkNotAllowed)
}

func TestObjectWatch(t *testing.T) {
	expectUpdateF := func(t *testing.T, watcher jetstream.ObjectWatcher) func(name string) {
		return func(name string) {
			t.Helper()
			select {
			case info := <-watcher.Updates():
				if false && info.Name != name { // TODO what is supposed to happen here?
					t.Fatalf("Expected update for %q, but got %+v", name, info)
				}
			case <-time.After(time.Second):
				t.Fatalf("Did not receive an update like expected")
			}
		}
	}

	expectNoMoreUpdatesF := func(t *testing.T, watcher jetstream.ObjectWatcher) func() {
		return func() {
			t.Helper()
			select {
			case info := <-watcher.Updates():
				t.Fatalf("Got an unexpected update: %+v", info)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}

	expectInitDoneF := func(t *testing.T, watcher jetstream.ObjectWatcher) func() {
		return func() {
			t.Helper()
			select {
			case info := <-watcher.Updates():
				if info != nil {
					t.Fatalf("Did not get expected: %+v", info)
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
		ctx := context.Background()

		obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "WATCH-TEST"})
		expectOk(t, err)

		watcher, err := obs.Watch(ctx)
		expectOk(t, err)
		defer watcher.Stop()

		expectUpdate := expectUpdateF(t, watcher)
		expectNoMoreUpdates := expectNoMoreUpdatesF(t, watcher)
		expectInitDone := expectInitDoneF(t, watcher)

		// We should get a marker that is nil when all initial values are delivered.
		expectInitDone()

		_, err = obs.PutString(ctx, "A", "AAA")
		expectOk(t, err)
		_, err = obs.PutString(ctx, "B", "BBB")
		expectOk(t, err)

		// Initial Values.
		expectUpdate("A")
		expectUpdate("B")
		expectNoMoreUpdates()

		// Delete
		err = obs.Delete(ctx, "A")
		expectOk(t, err)

		expectUpdate("A")
		expectNoMoreUpdates()

		// New
		_, err = obs.PutString(ctx, "C", "CCC")
		expectOk(t, err)

		// Update Meta
		deletedInfo, err := obs.GetInfo(ctx, "A", jetstream.GetObjectInfoShowDeleted())
		expectOk(t, err)
		if !deletedInfo.Deleted {
			t.Fatalf("Expected object to be deleted.")
		}
		meta := deletedInfo.ObjectMeta
		meta.Description = "Making a change."
		err = obs.UpdateMeta(ctx, "A", meta)
		expectErr(t, err, jetstream.ErrUpdateMetaDeleted)
	})

	t.Run("watcher with update", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx := context.Background()

		obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "WATCH-TEST"})
		expectOk(t, err)

		_, err = obs.PutString(ctx, "A", "AAA")
		expectOk(t, err)
		_, err = obs.PutString(ctx, "B", "BBB")
		expectOk(t, err)

		watcher, err := obs.Watch(ctx, jetstream.UpdatesOnly())
		expectOk(t, err)
		defer watcher.Stop()

		expectUpdate := expectUpdateF(t, watcher)
		expectNoMoreUpdates := expectNoMoreUpdatesF(t, watcher)

		// when listening for updates only, we should not receive anything when watcher is started
		expectNoMoreUpdates()

		// Delete
		err = obs.Delete(ctx, "A")
		expectOk(t, err)

		expectUpdate("A")
		expectNoMoreUpdates()

		// New
		_, err = obs.PutString(ctx, "C", "CCC")
		expectOk(t, err)
		expectUpdate("C")
	})
}

func TestObjectLinks(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	root, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "ROOT"})
	expectOk(t, err)

	_, err = root.PutString(ctx, "A", "AAA")
	expectOk(t, err)
	_, err = root.PutString(ctx, "B", "BBB")
	expectOk(t, err)

	infoA, err := root.GetInfo(ctx, "A")
	expectOk(t, err)

	// Link to individual object.
	infoLA, err := root.AddLink(ctx, "LA", infoA)
	expectOk(t, err)
	expectLinkIsCorrect(t, infoA, infoLA)

	// link to a link
	_, err = root.AddLink(ctx, "LALA", infoLA)
	expectErr(t, err, jetstream.ErrNoLinkToLink)

	dir, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "DIR"})
	expectOk(t, err)

	_, err = dir.PutString(ctx, "DIR/A", "DIR-AAA")
	expectOk(t, err)
	_, err = dir.PutString(ctx, "DIR/B", "DIR-BBB")
	expectOk(t, err)

	infoB, err := dir.GetInfo(ctx, "DIR/B")
	expectOk(t, err)

	infoLB, err := root.AddLink(ctx, "DBL", infoB)
	expectOk(t, err)
	expectLinkIsCorrect(t, infoB, infoLB)

	// Now add whole other store as a link, like a directory.
	infoBucketLink, err := root.AddBucketLink(ctx, "dir", dir)
	expectOk(t, err)

	_, err = root.Get(ctx, infoBucketLink.Name)
	expectErr(t, err, jetstream.ErrCantGetBucket)

	expectLinkPartsAreCorrect(t, infoBucketLink, "DIR", "")

	// Try to get a linked object, same bucket
	getLA, err := root.GetString(ctx, "LA")
	expectOk(t, err)

	if getLA != "AAA" {
		t.Fatalf("Expected %q but got %q", "AAA", getLA)
	}

	// Try to get a linked object, cross bucket
	getDbl, err := root.GetString(ctx, "DBL")
	expectOk(t, err)

	if getDbl != "DIR-BBB" {
		t.Fatalf("Expected %q but got %q", "DIR-BBB", getDbl)
	}

	// change a link
	infoB, err = root.GetInfo(ctx, "B")
	expectOk(t, err)

	infoLA, err = root.GetInfo(ctx, "LA")
	expectOk(t, err)
	expectLinkIsCorrect(t, infoA, infoLA)

	infoLA, err = root.AddLink(ctx, "LA", infoB)
	expectOk(t, err)
	expectLinkIsCorrect(t, infoB, infoLA)

	// change a bucket link
	infoBucketLink, err = root.GetInfo(ctx, "dir")
	expectOk(t, err)
	expectLinkPartsAreCorrect(t, infoBucketLink, "DIR", "")

	infoBucketLink, err = root.AddBucketLink(ctx, "dir", root)
	expectOk(t, err)
	expectLinkPartsAreCorrect(t, infoBucketLink, "ROOT", "")

	// Check simple errors.
	_, err = root.AddLink(ctx, "", infoB)
	expectErr(t, err, jetstream.ErrNameRequired)

	// A is already an object
	_, err = root.AddLink(ctx, "A", infoB)
	expectErr(t, err, jetstream.ErrObjectAlreadyExists)

	_, err = root.AddLink(ctx, "Nil Object", nil)
	expectErr(t, err, jetstream.ErrObjectRequired)

	infoB.Name = ""
	_, err = root.AddLink(ctx, "Empty Info Name", infoB)
	expectErr(t, err, jetstream.ErrObjectRequired)

	// Check Error Link to a Link
	_, err = root.AddLink(ctx, "Link To Link", infoLB)
	expectErr(t, err, jetstream.ErrNoLinkToLink)

	// Check Errors on bucket linking
	_, err = root.AddBucketLink(ctx, "", root)
	expectErr(t, err, jetstream.ErrNameRequired)

	_, err = root.AddBucketLink(ctx, "Nil Bucket", nil)
	expectErr(t, err, jetstream.ErrBucketRequired)

	err = root.Delete(ctx, "A")
	expectOk(t, err)

	_, err = root.AddLink(ctx, "ToDeletedStale", infoA)
	expectOk(t, err) // TODO deal with this in the code somehow

	infoA, err = root.GetInfo(ctx, "A", jetstream.GetObjectInfoShowDeleted())
	expectOk(t, err)

	_, err = root.AddLink(ctx, "ToDeletedFresh", infoA)
	expectErr(t, err, jetstream.ErrNoLinkToDeleted)
}

func expectLinkIsCorrect(t *testing.T, originalObject *jetstream.ObjectInfo, linkObject *jetstream.ObjectInfo) {
	if linkObject.Opts.Link == nil || !expectLinkPartsAreCorrect(t, linkObject, originalObject.Bucket, originalObject.Name) {
		t.Fatalf("Link info not what was expected:\nActual: %+v\nTarget: %+v", linkObject, originalObject)
	}
}

func expectLinkPartsAreCorrect(t *testing.T, linkObject *jetstream.ObjectInfo, bucket, name string) bool {
	return linkObject.Opts.Link.Bucket == bucket &&
		linkObject.Opts.Link.Name == name &&
		!linkObject.ModTime.IsZero() &&
		linkObject.NUID != ""
}

// Right now no history, just make sure we are cleaning up after ourselves.
func TestObjectHistory(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	info, err := obs.PutBytes(ctx, "A", bytes.Repeat([]byte("A"), 10))
	expectOk(t, err)

	if info.Size != 10 {
		t.Fatalf("Invalid first put when testing history %+v", info)
	}

	info, err = obs.PutBytes(ctx, "A", bytes.Repeat([]byte("a"), 20))
	expectOk(t, err)

	if info.Size != 20 {
		t.Fatalf("Invalid second put when testing history %+v", info)
	}

	// Should only be 1 copy of 'A', so 1 data and 1 meta since history was not selected.
	si, err := js.Stream(ctx, "OBJ_OBJS")
	expectOk(t, err)

	if si.CachedInfo().State.Msgs != 2 {
		t.Fatalf("Expected 2 msgs (1 data 1 meta) but got %d", si.CachedInfo().State.Msgs)
	}
}

func TestObjectList(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	root, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "ROOT"})
	expectOk(t, err)

	_, err = root.List(ctx)
	expectErr(t, err, jetstream.ErrNoObjectsFound)

	put := func(name, value string) {
		_, err = root.PutString(ctx, name, value)
		expectOk(t, err)
	}

	put("A", "AAA")
	put("B", "BBB")
	put("C", "CCC")
	put("B", "bbb")

	// Self link
	info, err := root.GetInfo(ctx, "B")
	expectOk(t, err)
	_, err = root.AddLink(ctx, "b", info)
	expectOk(t, err)

	put("D", "DDD")
	err = root.Delete(ctx, "D")
	expectOk(t, err)

	t.Run("without deleted objects", func(t *testing.T) {
		lch, err := root.List(ctx)
		expectOk(t, err)

		omap := make(map[string]struct{})
		for _, info := range lch {
			if _, ok := omap[info.Name]; ok {
				t.Fatalf("Already saw %q", info.Name)
			}
			omap[info.Name] = struct{}{}
		}
		if len(omap) != 4 {
			t.Fatalf("Expected 4 total objects, got %d", len(omap))
		}
		expected := map[string]struct{}{
			"A": struct{}{},
			"B": struct{}{},
			"C": struct{}{},
			"b": struct{}{},
		}
		if !reflect.DeepEqual(omap, expected) {
			t.Fatalf("Expected %+v but got %+v", expected, omap)
		}
	})

	t.Run("with deleted objects", func(t *testing.T) {
		lch, err := root.List(ctx, jetstream.ListObjectsShowDeleted())
		expectOk(t, err)

		res := make([]string, 0)
		for _, info := range lch {
			res = append(res, info.Name)
		}
		if len(res) != 5 {
			t.Fatalf("Expected 5 total objects, got %d", len(res))
		}
		expected := []string{"A", "C", "B", "b", "D"}

		if !reflect.DeepEqual(res, expected) {
			t.Fatalf("Expected %+v but got %+v", expected, res)
		}
	})

	t.Run("with context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		lch, err := root.List(ctx)
		expectOk(t, err)

		omap := make(map[string]struct{})
		for _, info := range lch {
			if _, ok := omap[info.Name]; ok {
				t.Fatalf("Already saw %q", info.Name)
			}
			omap[info.Name] = struct{}{}
		}
		if len(omap) != 4 {
			t.Fatalf("Expected 4 total objects, got %d", len(omap))
		}
		expected := map[string]struct{}{
			"A": struct{}{},
			"B": struct{}{},
			"C": struct{}{},
			"b": struct{}{},
		}
		if !reflect.DeepEqual(omap, expected) {
			t.Fatalf("Expected %+v but got %+v", expected, omap)
		}
	})
}

func TestObjectMaxBytes(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "OBJS", MaxBytes: 1024})
	expectOk(t, err)

	status, err := obs.Status(ctx)
	expectOk(t, err)
	bs := status.(*jetstream.ObjectBucketStatus)
	info := bs.StreamInfo()
	if info.Config.MaxBytes != 1024 {
		t.Fatalf("invalid object stream MaxSize %+v", info.Config.MaxBytes)
	}
}

func TestListObjectStores(t *testing.T) {
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
			ctx := context.Background()
			// create stream without the chunk subject, but with OBJ_ prefix
			_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "OBJ_FOO", Subjects: []string{"FOO.*"}})
			expectOk(t, err)
			// create stream with chunk subject, but without "OBJ_" prefix
			_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "FOO", Subjects: []string{"$O.ABC.C.>"}})
			expectOk(t, err)
			for i := 0; i < test.bucketsNum; i++ {
				_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: fmt.Sprintf("OBJS_%d", i), MaxBytes: 1024})
				expectOk(t, err)
			}
			names := make([]string, 0)
			objectStoreNames := js.ObjectStoreNames(ctx)
			for name := range objectStoreNames.Name() {
				if strings.HasPrefix(name, "OBJ_") {
					t.Fatalf("Expected name without OBJ_ prefix, got: %s", name)
				}
				names = append(names, name)
			}
			expectOk(t, objectStoreNames.Error())
			if len(names) != test.bucketsNum {
				t.Fatalf("Invalid number of stream names; want: %d; got: %d", test.bucketsNum, len(names))
			}
			infos := make([]jetstream.ObjectStoreStatus, 0)
			objectStores := js.ObjectStores(ctx)
			for info := range objectStores.Status() {
				infos = append(infos, info)
			}
			expectOk(t, objectStores.Error())
			if len(infos) != test.bucketsNum {
				t.Fatalf("Invalid number of streams; want: %d; got: %d", test.bucketsNum, len(infos))
			}
		})
	}
}

func TestGetObjectDigestValue(t *testing.T) {
	tests := []struct {
		inputFile string
		expected  string
	}{
		{
			inputFile: "digester_test_bytes_000100.txt",
			expected:  "SHA-256=IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=",
		},
		{
			inputFile: "digester_test_bytes_001000.txt",
			expected:  "SHA-256=DZj4RnBpuEukzFIY0ueZ-xjnHY4Rt9XWn4Dh8nkNfnI=",
		},
		{
			inputFile: "digester_test_bytes_010000.txt",
			expected:  "SHA-256=RgaJ-VSJtjNvgXcujCKIvaheiX_6GRCcfdRYnAcVy38=",
		},
		{
			inputFile: "digester_test_bytes_100000.txt",
			expected:  "SHA-256=yan7pwBVnC1yORqqgBfd64_qAw6q9fNA60_KRiMMooE=",
		},
	}

	for _, test := range tests {
		t.Run(test.inputFile, func(t *testing.T) {
			data, err := os.ReadFile(fmt.Sprintf("./testdata/%s", test.inputFile))
			expectOk(t, err)
			h := sha256.New()
			h.Write(data)
			if res := nats.GetObjectDigestValue(h); res != test.expected {
				t.Fatalf("Invalid digest; want: %s; got: %s", test.expected, res)
			}
		})
	}
}

func TestDecodeObjectDigest(t *testing.T) {
	tests := []struct {
		inputDigest  string
		expectedFile string
		withError    error
	}{
		{
			expectedFile: "digester_test_bytes_000100.txt",
			inputDigest:  "SHA-256=IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=",
		},
		{
			expectedFile: "digester_test_bytes_001000.txt",
			inputDigest:  "SHA-256=DZj4RnBpuEukzFIY0ueZ-xjnHY4Rt9XWn4Dh8nkNfnI=",
		},
		{
			expectedFile: "digester_test_bytes_010000.txt",
			inputDigest:  "SHA-256=RgaJ-VSJtjNvgXcujCKIvaheiX_6GRCcfdRYnAcVy38=",
		},
		{
			expectedFile: "digester_test_bytes_100000.txt",
			inputDigest:  "SHA-256=yan7pwBVnC1yORqqgBfd64_qAw6q9fNA60_KRiMMooE=",
		},
	}

	for _, test := range tests {
		t.Run(test.expectedFile, func(t *testing.T) {
			expected, err := os.ReadFile(fmt.Sprintf("./testdata/%s", test.expectedFile))
			h := sha256.New()
			h.Write(expected)
			expected = h.Sum(nil)
			expectOk(t, err)
			res, err := nats.DecodeObjectDigest(test.inputDigest)
			if test.withError != nil {
				expectErr(t, err, nats.ErrInvalidDigestFormat)
				return
			}
			expectOk(t, err)
			if !bytes.Equal(res[:], expected) {
				t.Fatalf("Invalid decoded value; want: %s; got: %s", expected, res)
			}
		})
	}
}

func TestObjectStoreGetObjectContextTimeout(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	blob := make([]byte, 1024)
	_, err = rand.Read(blob)
	expectOk(t, err)

	_, err = obs.PutBytes(context.Background(), "blob", blob)
	expectOk(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	r, err := obs.Get(ctx, "blob")
	expectOk(t, err)
	time.Sleep(15 * time.Millisecond)
	var res []byte
	_, err = r.Read(res)
	expectErr(t, err, nats.ErrTimeout)
	r.Close()
}

func TestObjectStoreCompression(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx := context.Background()

	objCompressed, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      "A",
		Compression: true,
	})
	if err != nil {
		t.Fatalf("Error creating object store: %v", err)
	}

	status, err := objCompressed.Status(ctx)
	if err != nil {
		t.Fatalf("Error getting bucket status: %v", err)
	}

	if !status.IsCompressed() {
		t.Fatalf("Expected bucket to be compressed")
	}

	objStream, err := js.Stream(ctx, "OBJ_A")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}

	if objStream.CachedInfo().Config.Compression != jetstream.S2Compression {
		t.Fatalf("Expected stream to be compressed with S2")
	}
}

func TestObjectStoreMirror(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	bucketName := "test-bucket"

	ctx := context.Background()
	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName, Description: "testing"})
	expectOk(t, err)

	mirrorBucketName := "mirror-test-bucket"

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name: fmt.Sprintf("OBJ_%s", mirrorBucketName),
		Mirror: &jetstream.StreamSource{
			Name: fmt.Sprintf("OBJ_%s", bucketName),
			SubjectTransforms: []jetstream.SubjectTransformConfig{
				{
					Source:      fmt.Sprintf("$O.%s.>", bucketName),
					Destination: fmt.Sprintf("$O.%s.>", mirrorBucketName),
				},
			},
		},
		AllowRollup: true, // meta messages are always rollups
	})
	if err != nil {
		t.Fatalf("Error creating object store bucket mirror: %v", err)
	}

	_, err = obs.PutString(ctx, "A", "abc")
	expectOk(t, err)

	mirrorObs, err := js.ObjectStore(ctx, mirrorBucketName)
	expectOk(t, err)

	// Make sure we sync.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		mirrorValue, err := mirrorObs.GetString(ctx, "A")
		if err != nil {
			return err
		}
		if mirrorValue != "abc" {
			t.Fatalf("Expected mirrored object store value to be the same as original")
		}
		return nil
	})

	watcher, err := mirrorObs.Watch(ctx)
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	defer watcher.Stop()

	// expect to get one value and nil
	for {
		select {
		case info := <-watcher.Updates():
			if info == nil {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Expected to receive an update")
		}
	}
}
