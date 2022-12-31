// Copyright 2012-2020 The NATS Authors
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
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestAuth(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	opts.Username = "derek"
	opts.Password = "foo"
	s := RunServerWithOptions(opts)
	defer s.Shutdown()

	_, err := nats.Connect("nats://127.0.0.1:8232")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	// This test may be a bit too strict for the future, but for now makes
	// sure that we correctly process the -ERR content on connect.
	if strings.ToLower(err.Error()) != nats.ErrAuthorization.Error() {
		t.Fatalf("Expected error '%v', got '%v'", nats.ErrAuthorization, err)
	}

	if !errors.Is(err, nats.ErrAuthorization) {
		t.Fatalf("Expected error '%v', got '%v'", nats.ErrAuthorization, err)
	}

	nc, err := nats.Connect("nats://derek:foo@127.0.0.1:8232")
	if err != nil {
		t.Fatal("Should have connected successfully with a token")
	}
	nc.Close()

	// Use Options
	nc, err = nats.Connect("nats://127.0.0.1:8232", nats.UserInfo("derek", "foo"))
	if err != nil {
		t.Fatalf("Should have connected successfully with a token: %v", err)
	}
	nc.Close()
	// Verify that credentials in URL take precedence.
	nc, err = nats.Connect("nats://derek:foo@127.0.0.1:8232", nats.UserInfo("foo", "bar"))
	if err != nil {
		t.Fatalf("Should have connected successfully with a token: %v", err)
	}
	nc.Close()
}

func TestAuthFailNoDisconnectErrCB(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	opts.Username = "derek"
	opts.Password = "foo"
	s := RunServerWithOptions(opts)
	defer s.Shutdown()

	copts := nats.GetDefaultOptions()
	copts.Url = "nats://127.0.0.1:8232"
	receivedDisconnectErrCB := int32(0)
	copts.DisconnectedErrCB = func(nc *nats.Conn, _ error) {
		atomic.AddInt32(&receivedDisconnectErrCB, 1)
	}

	_, err := copts.Connect()
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	if atomic.LoadInt32(&receivedDisconnectErrCB) > 0 {
		t.Fatal("Should not have received a disconnect callback on auth failure")
	}
}

func TestAuthFailAllowReconnect(t *testing.T) {
	ts := RunServerOnPort(23232)
	defer ts.Shutdown()

	var servers = []string{
		"nats://127.0.0.1:23232",
		"nats://127.0.0.1:23233",
		"nats://127.0.0.1:23234",
	}

	opts2 := test.DefaultTestOptions
	opts2.Port = 23233
	opts2.Username = "ivan"
	opts2.Password = "foo"
	ts2 := RunServerWithOptions(opts2)
	defer ts2.Shutdown()

	ts3 := RunServerOnPort(23234)
	defer ts3.Shutdown()

	reconnectch := make(chan bool)
	defer close(reconnectch)

	copts := nats.GetDefaultOptions()
	copts.Servers = servers
	copts.AllowReconnect = true
	copts.NoRandomize = true
	copts.MaxReconnect = 10
	copts.ReconnectWait = 100 * time.Millisecond
	nats.ReconnectJitter(0, 0)(&copts)

	copts.ReconnectedCB = func(_ *nats.Conn) {
		reconnectch <- true
	}

	// Connect
	nc, err := copts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	// Override default handler for test.
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, _ error) {})

	// Stop the server
	ts.Shutdown()

	// The client will try to connect to the second server, and that
	// should fail. It should then try to connect to the third and succeed.

	// Wait for the reconnect CB.
	if e := Wait(reconnectch); e != nil {
		t.Fatal("Reconnect callback should have been triggered")
	}

	if nc.IsClosed() {
		t.Fatal("Should have reconnected")
	}

	if nc.ConnectedUrl() != servers[2] {
		t.Fatalf("Should have reconnected to %s, reconnected to %s instead", servers[2], nc.ConnectedUrl())
	}
}

func TestTokenHandlerReconnect(t *testing.T) {
	var servers = []string{
		"nats://127.0.0.1:8232",
		"nats://127.0.0.1:8233",
	}

	ts := RunServerOnPort(8232)
	defer ts.Shutdown()

	opts2 := test.DefaultTestOptions
	opts2.Port = 8233
	secret := "S3Cr3T0k3n!"
	opts2.Authorization = secret
	ts2 := RunServerWithOptions(opts2)
	defer ts2.Shutdown()

	reconnectch := make(chan bool)
	defer close(reconnectch)

	copts := nats.GetDefaultOptions()
	copts.Servers = servers
	copts.AllowReconnect = true
	copts.NoRandomize = true
	copts.MaxReconnect = 10
	copts.ReconnectWait = 100 * time.Millisecond
	nats.ReconnectJitter(0, 0)(&copts)

	copts.TokenHandler = func() string {
		return secret
	}

	copts.ReconnectedCB = func(_ *nats.Conn) {
		reconnectch <- true
	}

	// Connect
	nc, err := copts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	// Stop the server
	ts.Shutdown()

	// The client will try to connect to the second server and succeed.

	// Wait for the reconnect CB.
	if e := Wait(reconnectch); e != nil {
		t.Fatal("Reconnect callback should have been triggered")
	}

	if nc.IsClosed() {
		t.Fatal("Should have reconnected")
	}

	if nc.ConnectedUrl() != servers[1] {
		t.Fatalf("Should have reconnected to %s, reconnected to %s instead", servers[1], nc.ConnectedUrl())
	}
}

func TestTokenAuth(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	secret := "S3Cr3T0k3n!"
	opts.Authorization = secret
	s := RunServerWithOptions(opts)
	defer s.Shutdown()

	_, err := nats.Connect("nats://127.0.0.1:8232")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	tokenURL := fmt.Sprintf("nats://%s@127.0.0.1:8232", secret)
	nc, err := nats.Connect(tokenURL)
	if err != nil {
		t.Fatal("Should have connected successfully")
	}
	nc.Close()

	// Use Options
	nc, err = nats.Connect("nats://127.0.0.1:8232", nats.Token(secret))
	if err != nil {
		t.Fatalf("Should have connected successfully: %v", err)
	}
	nc.Close()
	// Verify that token cannot be set when token handler is provided.
	_, err = nats.Connect("nats://127.0.0.1:8232", nats.TokenHandler(func() string { return secret }), nats.Token(secret))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token handler cannot be provided when token is set.
	_, err = nats.Connect("nats://127.0.0.1:8232", nats.Token(secret), nats.TokenHandler(func() string { return secret }))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token in the URL takes precedence.
	nc, err = nats.Connect(tokenURL, nats.Token("badtoken"))
	if err != nil {
		t.Fatalf("Should have connected successfully: %v", err)
	}
	nc.Close()
}

func TestTokenHandlerAuth(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	secret := "S3Cr3T0k3n!"
	opts.Authorization = secret
	s := RunServerWithOptions(opts)
	defer s.Shutdown()

	_, err := nats.Connect("nats://127.0.0.1:8232")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	tokenURL := fmt.Sprintf("nats://%s@127.0.0.1:8232", secret)
	nc, err := nats.Connect(tokenURL)
	if err != nil {
		t.Fatal("Should have connected successfully")
	}
	nc.Close()

	// Use Options
	nc, err = nats.Connect("nats://127.0.0.1:8232", nats.TokenHandler(func() string { return secret }))
	if err != nil {
		t.Fatalf("Should have connected successfully: %v", err)
	}
	nc.Close()
	// Verify that token cannot be set when token handler is provided.
	_, err = nats.Connect("nats://127.0.0.1:8232", nats.TokenHandler(func() string { return secret }), nats.Token(secret))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token handler cannot be provided when token is set.
	_, err = nats.Connect("nats://127.0.0.1:8232", nats.Token(secret), nats.TokenHandler(func() string { return secret }))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token handler cannot be provided when token is in URL.
	_, err = nats.Connect(tokenURL, nats.TokenHandler(func() string { return secret }))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
}

func TestPermViolation(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	opts.Users = []*server.User{
		{
			Username: "ivan",
			Password: "pwd",
			Permissions: &server.Permissions{
				Publish:   &server.SubjectPermission{Allow: []string{"Foo"}},
				Subscribe: &server.SubjectPermission{Allow: []string{"Bar"}},
			},
		},
	}
	s := RunServerWithOptions(opts)
	defer s.Shutdown()

	errCh := make(chan error, 2)
	errCB := func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errCh <- err
	}
	nc, err := nats.Connect(
		fmt.Sprintf("nats://ivan:pwd@127.0.0.1:%d", opts.Port),
		nats.ErrorHandler(errCB))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Cause a publish error
	nc.Publish("Bar", []byte("fail"))
	// Cause a subscribe error
	nc.Subscribe("Foo", func(_ *nats.Msg) {})

	expectedErrorTypes := []string{"publish", "subscription"}
	for _, expectedErr := range expectedErrorTypes {
		select {
		case e := <-errCh:
			if !strings.Contains(strings.ToLower(e.Error()), nats.PERMISSIONS_ERR) {
				t.Fatalf("Did not receive error about permissions")
			}
			if !strings.Contains(strings.ToLower(e.Error()), expectedErr) {
				t.Fatalf("Did not receive error about %q, got %v", expectedErr, e.Error())
			}
			// Make sure subject is not converted to lower case
			if expectedErr == "publish" && !strings.Contains(e.Error(), "Bar") {
				t.Fatalf("Subject Bar not found in error: %v", e)
			} else if expectedErr == "subscribe" && !strings.Contains(e.Error(), "Foo") {
				t.Fatalf("Subject Foo not found in error: %v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get the permission error")
		}
	}
	// Make sure connection has not been closed
	if nc.IsClosed() {
		t.Fatal("Connection should be not be closed")
	}
}

func TestCrossAccountRespond(t *testing.T) {
	conf := createConfFile(t, []byte(`
                listen: 127.0.0.1:-1

                accounts: {
                        A: {
                                users: [ { user: a, password: a,
                                           permissions = {
                                             subscribe = ["foo", "bar", "baz", "quux", "_INBOX.>"]
                                             publish = ["_INBOX.>", "_R_.>"]
                                           }
                                         }
                                       ]
                                exports [ 
                                  { service: "foo"  }
                                  { service: "bar"  }
                                  { service: "baz" }
                                  # Multiple responses
                                  { service: "quux", response: "stream", threshold: "1s" }
                                ]
                        },
                        B: {
                                users:  [ { user: b, password: b } ]
                                imports [
                                   { service: { subject: "foo", account: A } }
                                   { service: { subject: "bar", account: A } }
                                   { service: { subject: "baz", account: A } }
                                   { service: { subject: "quux", account: A } }
                                ]
                        },
                }
        `))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	errCh := make(chan error, 1)
	ncA, err := nats.Connect(s.ClientURL(), nats.UserInfo("a", "a"), nats.ErrorHandler(func(c *nats.Conn, sub *nats.Subscription, err error) {
		t.Logf("Connection A: WARN: %s", err)
		errCh <- err
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ncA.Close()

	ncB, err := nats.Connect(s.ClientURL(), nats.UserInfo("b", "b"), nats.ErrorHandler(func(c *nats.Conn, sub *nats.Subscription, err error) {
		t.Logf("Connection B: WARN: %s", err)
		errCh <- err
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ncB.Close()

	got := ""
	expect := "ok"
	t.Run("with NewMsg", func(t *testing.T) {
		ncA.Subscribe("foo", func(m *nats.Msg) {
			// NewMsg works with the side effect that it will reset the headers.
			msg := nats.NewMsg(m.Reply)
			msg.Data = []byte("ok")
			msg.Header["X-NATS-Result"] = []string{"ok"}
			err := m.RespondMsg(msg)
			if err != nil {
				errCh <- err
			}
		})
		ncA.Flush()

		msg := nats.NewMsg("foo")
		msg.Header = nats.Header{
			"X-NATS-ID": []string{"1"},
		}
		msg.Data = []byte("ping")
		resp, err := ncB.RequestMsg(msg, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		got = string(resp.Data)
		if got != expect {
			t.Errorf("Expected %v, got: %v", expect, got)
		}

		if len(resp.Header) != 1 {
			t.Errorf("Expected single header, got: %d", len(resp.Header))
		}
		_, ok := resp.Header["X-NATS-Result"]
		if !ok {
			t.Error("Missing header in response")
		}
	})

	t.Run("single RespondMsg", func(t *testing.T) {
		ncA.Subscribe("bar", func(m *nats.Msg) {
			// RespondMsg will keep the original headers of the request.
			m.Data = []byte("ok")
			m.Header["X-NATS-Result"] = []string{"ok"}
			err := m.RespondMsg(m)
			if err != nil {
				errCh <- err
			}
		})
		ncA.Flush()

		resp, err := ncB.Request("bar", []byte("ping"), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		got = string(resp.Data)
		if got != expect {
			t.Errorf("Expected %v, got: %v", expect, got)
		}

		if len(resp.Header) != 2 {
			t.Errorf("Expected original headers as well, got: %d", len(resp.Header))
		}
		_, ok := resp.Header["X-NATS-Result"]
		if !ok {
			t.Error("Missing header in response")
		}

		// Server injects this header.
		_, ok = resp.Header["Nats-Request-Info"]
		if !ok {
			t.Error("Missing header in response")
		}
	})

	t.Run("multiple RespondMsg stream", func(t *testing.T) {
		_, err := ncA.Subscribe("quux", func(m *nats.Msg) {
			m.Data = []byte("start")
			m.Header["Task-Progress"] = []string{"0%"}
			err := m.RespondMsg(m)
			if err != nil {
				errCh <- err
				return
			}
			m.Header["Task-Progress"] = []string{"50%"}
			m.Data = []byte("wip")
			err = m.RespondMsg(m)
			if err != nil {
				errCh <- err
				return
			}
			m.Header["Task-Progress"] = []string{"100%"}
			m.Data = []byte("done")
			err = m.RespondMsg(m)
			if err != nil {
				errCh <- err
				return
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		ncA.Flush()

		inbox := nats.NewInbox()
		responses, err := ncB.SubscribeSync(inbox)
		if err != nil {
			errCh <- err
			return
		}
		ncB.Flush()
		err = ncB.PublishRequest("quux", inbox, []byte("start"))
		if err != nil {
			t.Error(err)
		}

		getNext := func(t *testing.T, sub *nats.Subscription) *nats.Msg {
			resp, err := sub.NextMsg(2 * time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.Header) != 2 {
				t.Errorf("Expected original headers as well, got: %d", len(resp.Header))
			}
			return resp
		}

		resp := getNext(t, responses)
		got = string(resp.Data)
		expect = "start"
		if got != expect {
			t.Errorf("Expected %v, got: %v", expect, got)
		}
		v, ok := resp.Header["Task-Progress"]
		if !ok {
			t.Error("Missing header in response")
		}
		got = v[0]
		if got != "0%" {
			t.Errorf("Unexpected value in header, got: %v", got)
		}

		resp = getNext(t, responses)
		got = string(resp.Data)
		expect = "wip"
		if got != expect {
			t.Errorf("Expected %v, got: %v", expect, got)
		}
		v, ok = resp.Header["Task-Progress"]
		if !ok {
			t.Error("Missing header in response")
		}
		got = v[0]
		if got != "50%" {
			t.Errorf("Unexpected value in header, got: %v", got)
		}

		resp = getNext(t, responses)
		got = string(resp.Data)
		expect = "done"
		if got != expect {
			t.Errorf("Expected %v, got: %v", expect, got)
		}
		v, ok = resp.Header["Task-Progress"]
		if !ok {
			t.Error("Missing header in response")
		}
		got = v[0]
		if got != "100%" {
			t.Errorf("Unexpected value in header, got: %v", got)
		}
	})

	t.Run("RespondMsg different reply", func(t *testing.T) {
		inbox := nats.NewInbox()
		sub, err := ncA.SubscribeSync(inbox)
		if err != nil {
			t.Fatal(err)
		}
		_, err = ncA.Subscribe("baz", func(m *nats.Msg) {
			// Publish new message with different reply from to sub in Account A.
			msg := &nats.Msg{
				Header: m.Header,
				Data:   []byte("baz"),
				Reply:  inbox,
			}
			err := m.RespondMsg(msg)
			if err != nil {
				errCh <- err
			}
		})
		ncA.Flush()
		if err != nil {
			t.Fatal(err)
		}
		resp, err := ncB.Request("baz", []byte("ping"), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		// Modify original request data
		resp.Data = append(resp.Data, '!')
		resp.RespondMsg(resp)
		m2, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		expect = "baz!"
		got = string(m2.Data)
		if got != expect {
			t.Errorf("Expected %v, got: %v", expect, got)
		}
	})

	select {
	case err := <-errCh:
		if err != nil {
			t.Error(err)
		}
	default:
	}
}
