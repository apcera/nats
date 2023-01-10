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

package micro_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	natsserver "github.com/nats-io/nats-server/v2/test"
)

func TestServiceBasics(t *testing.T) {
	ctx := context.Background()

	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	// Stub service.
	doAdd := func(ctx context.Context, req micro.Request) {
		if rand.Intn(10) == 0 {
			if err := req.Error("500", "Unexpected error!", nil); err != nil {
				t.Fatalf("Unexpected error when sending error response: %v", err)
			}
			return
		}
		// Happy Path.
		// Random delay between 5-10ms
		time.Sleep(5*time.Millisecond + time.Duration(rand.Intn(5))*time.Millisecond)
		if err := req.Respond([]byte("42")); err != nil {
			if err := req.Error("500", "Unexpected error!", nil); err != nil {
				t.Fatalf("Unexpected error when sending error response: %v", err)
			}
			return
		}
	}

	var svcs []micro.Service

	// Create 5 service responders.
	config := micro.Config{
		Name:        "CoolAddService",
		Version:     "0.1.0",
		Description: "Add things together",
		Endpoint: micro.Endpoint{
			Subject: "svc.add",
			Handler: micro.HandlerFunc(doAdd),
		},
		Schema: micro.Schema{Request: "", Response: ""},
	}

	for i := 0; i < 5; i++ {
		svc, err := micro.AddService(ctx, nc, config)
		if err != nil {
			t.Fatalf("Expected to create Service, got %v", err)
		}
		defer svc.Stop(ctx)
		svcs = append(svcs, svc)
	}

	// Now send 50 requests.
	for i := 0; i < 50; i++ {
		_, err := nc.Request("svc.add", []byte(`{ "x": 22, "y": 11 }`), time.Second)
		if err != nil {
			t.Fatalf("Expected a response, got %v", err)
		}
	}

	for _, svc := range svcs {
		info := svc.Info(ctx)
		if info.Name != "CoolAddService" {
			t.Fatalf("Expected %q, got %q", "CoolAddService", info.Name)
		}
		if len(info.Description) == 0 || len(info.Version) == 0 {
			t.Fatalf("Expected non empty description and version")
		}
	}

	// Make sure we can request info, 1 response.
	// This could be exported as well as main ServiceImpl.
	subj, err := micro.ControlSubject(micro.InfoVerb, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Failed to building info subject %v", err)
	}
	info, err := nc.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Expected a response, got %v", err)
	}
	var inf micro.Info
	if err := json.Unmarshal(info.Data, &inf); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if inf.Subject != "svc.add" {
		t.Fatalf("expected service subject to be srv.add: %s", inf.Subject)
	}

	// Ping all services. Multiple responses.
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}
	pingSubject, err := micro.ControlSubject(micro.PingVerb, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := nc.PublishRequest(pingSubject, inbox, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var pingCount int
	for {
		_, err := sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			break
		}
		pingCount++
	}
	if pingCount != 5 {
		t.Fatalf("Expected 5 ping responses, got: %d", pingCount)
	}

	// Get stats from all services
	statsInbox := nats.NewInbox()
	sub, err = nc.SubscribeSync(statsInbox)
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}
	statsSubject, err := micro.ControlSubject(micro.StatsVerb, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := nc.PublishRequest(statsSubject, statsInbox, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	stats := make([]micro.Stats, 0)
	var requestsNum int
	for {
		resp, err := sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			break
		}
		var srvStats micro.Stats
		if err := json.Unmarshal(resp.Data, &srvStats); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		requestsNum += srvStats.NumRequests
		stats = append(stats, srvStats)
	}
	if len(stats) != 5 {
		t.Fatalf("Expected stats for 5 services, got: %d", len(stats))
	}

	// Services should process 50 requests total
	if requestsNum != 50 {
		t.Fatalf("Expected a total fo 50 requests processed, got: %d", requestsNum)
	}
	// Reset stats for a service
	svcs[0].Reset(ctx)
	emptyStats := micro.Stats{
		Type:            micro.StatsResponseType,
		ServiceIdentity: svcs[0].Info(ctx).ServiceIdentity,
	}

	if !reflect.DeepEqual(svcs[0].Stats(ctx), emptyStats) {
		t.Fatalf("Expected empty stats after reset; got: %+v", svcs[0].Stats(ctx))
	}

}

func TestAddService(t *testing.T) {
	testHandler := func(context.Context, micro.Request) {}
	errNats := make(chan struct{})
	errService := make(chan struct{})
	closedNats := make(chan struct{})
	doneService := make(chan struct{})
	ctx := context.Background()

	tests := []struct {
		name              string
		givenConfig       micro.Config
		natsClosedHandler nats.ConnHandler
		natsErrorHandler  nats.ErrHandler
		asyncErrorSubject string
		expectedPing      micro.Ping
		withError         error
	}{
		{
			name: "minimal config",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(testHandler),
				},
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
				},
			},
		},
		{
			name: "with done handler, no handlers on nats connection",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(testHandler),
				},
				DoneHandler: func(context.Context, micro.Service) {
					doneService <- struct{}{}
				},
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
				},
			},
		},
		{
			name: "with error handler, no handlers on nats connection",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(testHandler),
				},
				ErrorHandler: func(context.Context, micro.Service, *micro.NATSError) {
					errService <- struct{}{}
				},
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
				},
			},
			asyncErrorSubject: "test.sub",
		},
		{
			name: "with error handler, no handlers on nats connection, error on monitoring subject",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(testHandler),
				},
				ErrorHandler: func(context.Context, micro.Service, *micro.NATSError) {
					errService <- struct{}{}
				},
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
				},
			},
			asyncErrorSubject: "$SVC.PING.TEST_SERVICE",
		},
		{
			name: "with done handler, append to nats handlers",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(testHandler),
				},
				DoneHandler: func(context.Context, micro.Service) {
					doneService <- struct{}{}
				},
			},
			natsClosedHandler: func(c *nats.Conn) {
				closedNats <- struct{}{}
			},
			natsErrorHandler: func(*nats.Conn, *nats.Subscription, error) {
				errNats <- struct{}{}
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
				},
			},
			asyncErrorSubject: "test.sub",
		},
		{
			name: "with error handler, append to nats handlers",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(testHandler),
				},
				DoneHandler: func(context.Context, micro.Service) {
					doneService <- struct{}{}
				},
			},
			natsClosedHandler: func(c *nats.Conn) {
				closedNats <- struct{}{}
			},
			natsErrorHandler: func(*nats.Conn, *nats.Subscription, error) {
				errNats <- struct{}{}
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
				},
			},
		},
		{
			name: "with error handler, append to nats handlers, error on monitoring subject",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(testHandler),
				},
				DoneHandler: func(context.Context, micro.Service) {
					doneService <- struct{}{}
				},
			},
			natsClosedHandler: func(c *nats.Conn) {
				closedNats <- struct{}{}
			},
			natsErrorHandler: func(*nats.Conn, *nats.Subscription, error) {
				errNats <- struct{}{}
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
				},
			},
			asyncErrorSubject: "$SVC.PING.TEST_SERVICE",
		},
		{
			name: "validation error, invalid service name",
			givenConfig: micro.Config{
				Name:    "test_service!",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(testHandler),
				},
			},
			withError: micro.ErrConfigValidation,
		},
		{
			name: "validation error, invalid version",
			givenConfig: micro.Config{
				Name:    "test_service!",
				Version: "abc",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(testHandler),
				},
			},
			withError: micro.ErrConfigValidation,
		},
		{
			name: "validation error, empty subject",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "",
					Handler: micro.HandlerFunc(testHandler),
				},
			},
			withError: micro.ErrConfigValidation,
		},
		{
			name: "validation error, no handler",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test_subject",
					Handler: nil,
				},
			},
			withError: micro.ErrConfigValidation,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunServerOnPort(-1)
			defer s.Shutdown()

			nc, err := nats.Connect(s.ClientURL(),
				nats.ErrorHandler(test.natsErrorHandler),
				nats.ClosedHandler(test.natsClosedHandler),
			)
			if err != nil {
				t.Fatalf("Expected to connect to server, got %v", err)
			}
			defer nc.Close()

			srv, err := micro.AddService(ctx, nc, test.givenConfig)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			info := srv.Info(ctx)
			pingSubject, err := micro.ControlSubject(micro.PingVerb, info.Name, info.ID)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			pingResp, err := nc.Request(pingSubject, nil, 1*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var ping micro.Ping
			if err := json.Unmarshal(pingResp.Data, &ping); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			test.expectedPing.ID = info.ID
			if test.expectedPing != ping {
				t.Fatalf("Invalid ping response; want: %+v; got: %+v", test.expectedPing, ping)
			}

			if test.givenConfig.DoneHandler != nil {
				go nc.Opts.ClosedCB(nc)
				select {
				case <-doneService:
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout on DoneHandler")
				}
				if test.natsClosedHandler != nil {
					select {
					case <-closedNats:
					case <-time.After(1 * time.Second):
						t.Fatalf("Timeout on ClosedHandler")
					}
				}
			}

			if test.givenConfig.ErrorHandler != nil {
				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: test.asyncErrorSubject}, fmt.Errorf("oops"))
				select {
				case <-errService:
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout on ErrorHandler")
				}
				if test.natsErrorHandler != nil {
					select {
					case <-errNats:
					case <-time.After(1 * time.Second):
						t.Fatalf("Timeout on AsyncErrHandler")
					}
				}
			}

			if err := srv.Stop(ctx); err != nil {
				t.Fatalf("Unexpected error when stopping the service: %v", err)
			}
			if test.natsClosedHandler != nil {
				go nc.Opts.ClosedCB(nc)
				select {
				case <-doneService:
					t.Fatalf("Expected to restore nats closed handler")
				case <-time.After(50 * time.Millisecond):
				}
				select {
				case <-closedNats:
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout on ClosedHandler")
				}
			}
			if test.natsErrorHandler != nil {
				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: test.asyncErrorSubject}, fmt.Errorf("oops"))
				select {
				case <-errService:
					t.Fatalf("Expected to restore nats error handler")
				case <-time.After(50 * time.Millisecond):
				}
				select {
				case <-errNats:
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout on AsyncErrHandler")
				}
			}
		})
	}
}

func TestMonitoringHandlers(t *testing.T) {
	ctx := context.Background()
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	asyncErr := make(chan struct{})
	errHandler := func(ctx context.Context, s micro.Service, n *micro.NATSError) {
		asyncErr <- struct{}{}
	}

	config := micro.Config{
		Name:    "test_service",
		Version: "0.1.0",
		Endpoint: micro.Endpoint{
			Subject: "test.sub",
			Handler: micro.HandlerFunc(func(context.Context, micro.Request) {}),
		},
		Schema: micro.Schema{
			Request: "some_schema",
		},
		ErrorHandler: errHandler,
	}
	srv, err := micro.AddService(ctx, nc, config)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer func() {
		srv.Stop(ctx)
		if !srv.Stopped(ctx) {
			t.Fatalf("Expected service to be stopped")
		}
	}()

	info := srv.Info(ctx)

	tests := []struct {
		name             string
		subject          string
		withError        bool
		expectedResponse interface{}
	}{
		{
			name:    "PING all",
			subject: "$SRV.PING",
			expectedResponse: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
			},
		},
		{
			name:    "PING name",
			subject: "$SRV.PING.test_service",
			expectedResponse: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
			},
		},
		{
			name:    "PING ID",
			subject: fmt.Sprintf("$SRV.PING.test_service.%s", info.ID),
			expectedResponse: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
			},
		},
		{
			name:    "INFO all",
			subject: "$SRV.INFO",
			expectedResponse: micro.Info{
				Type: micro.InfoResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
				Subject: "test.sub",
			},
		},
		{
			name:    "INFO name",
			subject: "$SRV.INFO.test_service",
			expectedResponse: micro.Info{
				Type: micro.InfoResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
				Subject: "test.sub",
			},
		},
		{
			name:    "INFO ID",
			subject: fmt.Sprintf("$SRV.INFO.test_service.%s", info.ID),
			expectedResponse: micro.Info{
				Type: micro.InfoResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
				Subject: "test.sub",
			},
		},
		{
			name:    "SCHEMA all",
			subject: "$SRV.SCHEMA",
			expectedResponse: micro.SchemaResp{
				Type: micro.SchemaResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
				Schema: micro.Schema{
					Request: "some_schema",
				},
			},
		},
		{
			name:    "SCHEMA name",
			subject: "$SRV.SCHEMA.test_service",
			expectedResponse: micro.SchemaResp{
				Type: micro.SchemaResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
				Schema: micro.Schema{
					Request: "some_schema",
				},
			},
		},
		{
			name:    "SCHEMA ID",
			subject: fmt.Sprintf("$SRV.SCHEMA.test_service.%s", info.ID),
			expectedResponse: micro.SchemaResp{
				Type: micro.SchemaResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
				Schema: micro.Schema{
					Request: "some_schema",
				},
			},
		},
		{
			name:      "PING error",
			subject:   "$SRV.PING",
			withError: true,
		},
		{
			name:      "INFO error",
			subject:   "$SRV.INFO",
			withError: true,
		},
		{
			name:      "STATS error",
			subject:   "$SRV.STATS",
			withError: true,
		},
		{
			name:      "SCHEMA error",
			subject:   "$SRV.SCHEMA",
			withError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.withError {
				// use publish instead of request, so Respond will fail inside the handler
				if err := nc.Publish(test.subject, nil); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				select {
				case <-asyncErr:
					return
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout waiting for async error")
				}
				return
			}

			resp, err := nc.Request(test.subject, nil, 1*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			respMap := make(map[string]interface{})
			if err := json.Unmarshal(resp.Data, &respMap); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectedResponseJSON, err := json.Marshal(test.expectedResponse)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectedRespMap := make(map[string]interface{})
			if err := json.Unmarshal(expectedResponseJSON, &expectedRespMap); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !reflect.DeepEqual(respMap, expectedRespMap) {
				t.Fatalf("Invalid response; want: %+v; got: %+v", expectedRespMap, respMap)
			}
		})
	}
}

func TestServiceStats(t *testing.T) {
	handler := func(ctx context.Context, r micro.Request) {
		r.Respond([]byte("ok"))
	}
	tests := []struct {
		name          string
		config        micro.Config
		expectedStats map[string]interface{}
	}{
		{
			name: "without schema or stats handler",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(handler),
				},
			},
		},
		{
			name: "with stats handler",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(handler),
				},
				StatsHandler: func(ctx context.Context, e micro.Endpoint) interface{} {
					return map[string]interface{}{
						"key": "val",
					}
				},
			},
			expectedStats: map[string]interface{}{
				"key": "val",
			},
		},
		{
			name: "with schema",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(handler),
				},
				Schema: micro.Schema{
					Request: "some_schema",
				},
			},
		},
		{
			name: "with schema and stats handler",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: micro.Endpoint{
					Subject: "test.sub",
					Handler: micro.HandlerFunc(handler),
				},
				Schema: micro.Schema{
					Request: "some_schema",
				},
				StatsHandler: func(ctx context.Context, e micro.Endpoint) interface{} {
					return map[string]interface{}{
						"key": "val",
					}
				},
			},
			expectedStats: map[string]interface{}{
				"key": "val",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s := RunServerOnPort(-1)
			defer s.Shutdown()

			nc, err := nats.Connect(s.ClientURL())
			if err != nil {
				t.Fatalf("Expected to connect to server, got %v", err)
			}
			defer nc.Close()

			srv, err := micro.AddService(ctx, nc, test.config)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer srv.Stop(ctx)
			for i := 0; i < 10; i++ {
				if _, err := nc.Request(srv.Info(ctx).Subject, []byte("msg"), time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}

			// Malformed request, missing reply subjtct
			// This should be reflected in errors
			if err := nc.Publish(srv.Info(ctx).Subject, []byte("err")); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			time.Sleep(10 * time.Millisecond)

			info := srv.Info(ctx)
			resp, err := nc.Request(fmt.Sprintf("$SRV.STATS.test_service.%s", info.ID), nil, 1*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var stats micro.Stats
			if err := json.Unmarshal(resp.Data, &stats); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if stats.Name != info.Name {
				t.Errorf("Unexpected service name; want: %s; got: %s", info.Name, stats.Name)
			}
			if stats.ID != info.ID {
				t.Errorf("Unexpected service name; want: %s; got: %s", info.ID, stats.ID)
			}
			if stats.NumRequests != 11 {
				t.Errorf("Unexpected num_requests; want: 11; got: %d", stats.NumRequests)
			}
			if stats.NumErrors != 1 {
				t.Errorf("Unexpected num_errors; want: 1; got: %d", stats.NumErrors)
			}
			if test.expectedStats != nil {
				var data map[string]interface{}
				if err := json.Unmarshal(stats.Data, &data); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if !reflect.DeepEqual(data, test.expectedStats) {
					t.Fatalf("Invalid data from stats handler; want: %v; got: %v", test.expectedStats, data)
				}
			}
		})
	}
}

func TestRequestRespond(t *testing.T) {
	type x struct {
		A string `json:"a"`
		B int    `json:"b"`
	}

	tests := []struct {
		name             string
		respondData      interface{}
		respondHeaders   micro.Headers
		errDescription   string
		errCode          string
		errData          []byte
		expectedMessage  string
		expectedCode     string
		expectedResponse []byte
		withRespondError error
	}{
		{
			name:             "byte response",
			respondData:      []byte("OK"),
			expectedResponse: []byte("OK"),
		},
		{
			name:             "byte response, with headers",
			respondHeaders:   micro.Headers{"key": []string{"value"}},
			respondData:      []byte("OK"),
			expectedResponse: []byte("OK"),
		},
		{
			name:             "byte response, connection closed",
			respondData:      []byte("OK"),
			withRespondError: micro.ErrRespond,
		},
		{
			name:             "struct response",
			respondData:      x{"abc", 5},
			expectedResponse: []byte(`{"a":"abc","b":5}`),
		},
		{
			name:             "invalid response data",
			respondData:      func() {},
			withRespondError: micro.ErrMarshalResponse,
		},
		{
			name:            "generic error",
			errDescription:  "oops",
			errCode:         "500",
			errData:         []byte("error!"),
			expectedMessage: "oops",
			expectedCode:    "500",
		},
		{
			name:            "generic error, with headers",
			respondHeaders:  micro.Headers{"key": []string{"value"}},
			errDescription:  "oops",
			errCode:         "500",
			errData:         []byte("error!"),
			expectedMessage: "oops",
			expectedCode:    "500",
		},
		{
			name:            "error without response payload",
			errDescription:  "oops",
			errCode:         "500",
			expectedMessage: "oops",
			expectedCode:    "500",
		},
		{
			name:             "missing error code",
			errDescription:   "oops",
			withRespondError: micro.ErrArgRequired,
		},
		{
			name:             "missing error description",
			errCode:          "500",
			withRespondError: micro.ErrArgRequired,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s := RunServerOnPort(-1)
			defer s.Shutdown()

			nc, err := nats.Connect(s.ClientURL())
			if err != nil {
				t.Fatalf("Expected to connect to server, got %v", err)
			}
			defer nc.Close()

			respData := test.respondData
			respError := test.withRespondError
			errCode := test.errCode
			errDesc := test.errDescription
			errData := test.errData
			handler := func(ctx context.Context, req micro.Request) {
				if errors.Is(test.withRespondError, micro.ErrRespond) {
					nc.Close()
				}
				if val := req.Headers().Get("key"); val != "value" {
					t.Fatalf("Expected headers in the request")
				}
				if errCode == "" && errDesc == "" {
					if resp, ok := respData.([]byte); ok {
						err := req.Respond(resp, micro.WithHeaders(test.respondHeaders))
						if respError != nil {
							if !errors.Is(err, respError) {
								t.Fatalf("Expected error: %v; got: %v", respError, err)
							}
							return
						}
						if err != nil {
							t.Fatalf("Unexpected error when sending response: %v", err)
						}
					} else {
						err := req.RespondJSON(respData, micro.WithHeaders(test.respondHeaders))
						if respError != nil {
							if !errors.Is(err, respError) {
								t.Fatalf("Expected error: %v; got: %v", respError, err)
							}
							return
						}
						if err != nil {
							t.Fatalf("Unexpected error when sending response: %v", err)
						}
					}
					return
				}

				err := req.Error(errCode, errDesc, errData, micro.WithHeaders(test.respondHeaders))
				if respError != nil {
					if !errors.Is(err, respError) {
						t.Fatalf("Expected error: %v; got: %v", respError, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Unexpected error when sending response: %v", err)
				}
			}

			svc, err := micro.AddService(ctx, nc, micro.Config{
				Name:        "CoolService",
				Version:     "0.1.0",
				Description: "test service",
				Endpoint: micro.Endpoint{
					Subject: "svc.test",
					Handler: micro.HandlerFunc(handler),
				},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer svc.Stop(ctx)

			resp, err := nc.RequestMsg(&nats.Msg{
				Subject: svc.Info(ctx).Subject,
				Data:    nil,
				Header:  nats.Header{"key": []string{"value"}},
			}, 50*time.Millisecond)
			if test.withRespondError != nil {
				return
			}
			if err != nil {
				t.Fatalf("request error: %v", err)
			}

			if test.errCode != "" {
				description := resp.Header.Get("Nats-Service-Error")
				if description != test.expectedMessage {
					t.Fatalf("Invalid response message; want: %q; got: %q", test.expectedMessage, description)
				}
				expectedHeaders := micro.Headers{
					"Nats-Service-Error-Code": []string{resp.Header.Get("Nats-Service-Error-Code")},
					"Nats-Service-Error":      []string{resp.Header.Get("Nats-Service-Error")},
				}
				for k, v := range test.respondHeaders {
					expectedHeaders[k] = v
				}
				if !reflect.DeepEqual(expectedHeaders, micro.Headers(resp.Header)) {
					t.Fatalf("Invalid response headers; want: %v; got: %v", test.respondHeaders, resp.Header)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !bytes.Equal(bytes.TrimSpace(resp.Data), bytes.TrimSpace(test.expectedResponse)) {
				t.Fatalf("Invalid response; want: %s; got: %s", string(test.expectedResponse), string(resp.Data))
			}

			if !reflect.DeepEqual(test.respondHeaders, micro.Headers(resp.Header)) {
				t.Fatalf("Invalid response headers; want: %v; got: %v", test.respondHeaders, resp.Header)
			}
		})
	}
}

func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	return RunServerWithOptions(&opts)
}

func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}

func TestControlSubject(t *testing.T) {
	tests := []struct {
		name            string
		verb            micro.Verb
		srvName         string
		id              string
		expectedSubject string
		withError       error
	}{
		{
			name:            "PING ALL",
			verb:            micro.PingVerb,
			expectedSubject: "$SRV.PING",
		},
		{
			name:            "PING name",
			verb:            micro.PingVerb,
			srvName:         "test",
			expectedSubject: "$SRV.PING.test",
		},
		{
			name:            "PING id",
			verb:            micro.PingVerb,
			srvName:         "test",
			id:              "123",
			expectedSubject: "$SRV.PING.test.123",
		},
		{
			name:      "invalid verb",
			verb:      micro.Verb(100),
			withError: micro.ErrVerbNotSupported,
		},
		{
			name:      "name not provided",
			verb:      micro.PingVerb,
			srvName:   "",
			id:        "123",
			withError: micro.ErrServiceNameRequired,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := micro.ControlSubject(test.verb, test.srvName, test.id)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if res != test.expectedSubject {
				t.Errorf("Invalid subject; want: %q; got: %q", test.expectedSubject, res)
			}
		})
	}
}
