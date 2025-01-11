# NATS micro [![GoDoc](https://pkg.go.dev/badge/github.com/nats-io/nats.go/micro.svg)](https://pkg.go.dev/github.com/nats-io/nats.go/micro)

- [Overview](#overview)
- [Basic usage](#basic-usage)
- [Endpoints and groups](#endpoints-and-groups)
- [Discovery and Monitoring](#discovery-and-monitoring)
- [Examples](#examples)
- [Documentation](#documentation)

## Overview

The `micro` package in the NATS.go library provides a simple way to create
microservices that leverage NATS for scalability, load management and
observability.

## Basic usage

To start using the `micro` package, import it in your application:

```go
import "github.com/nats-io/nats.go/micro"
```

The core of the `micro` package is the Service. A Service aggregates endpoints
for handling application logic. Services are named and versioned. You create a
Service using the `micro.NewService()` function, passing in the NATS connection
and Service configuration.

```go
nc, _ := nats.Connect(nats.DefaultURL)

// request handler
echoHandler := func(req micro.Request) {
    req.Respond(req.Data())
}

srv, err := micro.AddService(nc, micro.Config{
    Name:        "EchoService",
    Version:     "1.0.0",
    // base handler
    Endpoint: &micro.EndpointConfig{
        Subject: "svc.echo",
        Handler: micro.HandlerFunc(echoHandler),
    },
})
```

After creating the service, it can be accessed by publishing a request on
endpoint subject. For given configuration, run:

```sh
nats req svc.echo "hello!"
```

To get:

```sh
17:37:32 Sending request on "svc.echo"
17:37:32 Received with rtt 365.875µs
hello!
```

## Endpoints and groups

Base endpoint can be optionally configured on a service, but it is also possible
to add more endpoints after the service is created.

```go
srv, _ := micro.AddService(nc, config)

// endpoint will be registered under "svc.add" subject
err = srv.AddEndpoint("svc.add", micro.HandlerFunc(add))
```

In the above example `svc.add` is an endpoint name and subject. It is possible
have a different endpoint name then the endpoint subject by using
`micro.WithEndpointSubject()` option in `AddEndpoint()`.

```go
// endpoint will be registered under "svc.add" subject
err = srv.AddEndpoint("Adder", micro.HandlerFunc(echoHandler), micro.WithEndpointSubject("svc.add"))
```

Endpoints can also be aggregated using groups. A group represents a common
subject prefix used by all endpoints associated with it.

```go
srv, _ := micro.AddService(nc, config)

numbersGroup := srv.AddGroup("numbers")

// endpoint will be registered under "numbers.add" subject
_ = numbersGroup.AddEndpoint("add", micro.HandlerFunc(addHandler))
// endpoint will be registered under "numbers.multiply" subject
_ = numbersGroup.AddEndpoint("multiply", micro.HandlerFunc(multiplyHandler))
```

## Customizing queue groups

For each service, group and endpoint the queue group used to gather responses
can be customized. If not provided a default queue group will be used (`q`).
Customizing queue groups can be useful to e.g. implement fanout request pattern
or hedged request pattern (to reduce tail latencies by only waiting for the
first response for multiple service instances).

Let's say we have multiple services listening on the same subject, but with
different queue groups:

```go
for i := 0; i < 5; i++ {
  srv, _ := micro.AddService(nc, micro.Config{
    Name:        "EchoService",
    Version:     "1.0.0",
    QueueGroup:  fmt.Sprintf("q-%d", i),
    // base handler
    Endpoint: &micro.EndpointConfig{
        Subject: "svc.echo",
        Handler: micro.HandlerFunc(echoHandler),
    },
  })
}
```

In the client, we can send request to `svc.echo` to receive responses from all
services registered on this subject (or wait only for the first response):

```go
sub, _ := nc.SubscribeSync("rply")
nc.PublishRequest("svc.echo", "rply", nil)
for start := time.Now(); time.Since(start) < 5*time.Second; {
  msg, err := sub.NextMsg(1 * time.Second)
  if err != nil {
    break
  }
  fmt.Println("Received ", string(msg.Data))
}
```

Queue groups can be overwritten by setting them on groups and endpoints as well:

```go
  srv, _ := micro.AddService(nc, micro.Config{
    Name:        "EchoService",
    Version:     "1.0.0",
    QueueGroup:  "q1",
  })

  g := srv.AddGroup("g", micro.WithGroupQueueGroup("q2"))

  // will be registered with queue group 'q2' from parent group
  g.AddEndpoint("bar", micro.HandlerFunc(func(r micro.Request) {}))

  // will be registered with queue group 'q3'
  g.AddEndpoint("bar", micro.HandlerFunc(func(r micro.Request) {}), micro.WithEndpointQueueGroup("q3"))
```

## Discovery and Monitoring

Each service is assigned a unique ID on creation. A service instance is
identified by service name and ID. Multiple services with the same name, but
different IDs can be created.

Each service exposes 3 endpoints when created:

- PING - used for service discovery and RTT calculation
- INFO - returns service configuration details (used subjects, service metadata
  etc.)
- STATS - service statistics

Each of those operations can be performed on 3 subjects:

- all services: `$SRV.<operation>` - returns a response for each created service
  and service instance
- by service name: `$SRV.<operation>.<service_name>` - returns a response for
  each service with given `service_name`
- by service name and ID: `$SRV.<operation>.<service_name>.<service_id>` -
  returns a response for a service with given `service_name` and `service_id`

For given configuration

```go
nc, _ := nats.Connect("nats://localhost:4222")
echoHandler := func(req micro.Request) {
    req.Respond(req.Data())
}

config := micro.Config{
    Name:    "EchoService",
    Version: "1.0.0",
    Endpoint: &micro.EndpointConfig{
        Subject: "svc.echo",
        Handler: micro.HandlerFunc(echoHandler),
    },
}
for i := 0; i < 3; i++ {
    srv, err := micro.AddService(nc, config)
    if err != nil {
        log.Fatal(err)
    }
    defer srv.Stop()
}
```

Service IDs can be discovered by:

```sh
nats req '$SRV.PING.EchoService' '' --replies=3

13:03:04 Sending request on "$SRV.PING.EchoService"
13:03:04 Received with rtt 1.302208ms
{"name":"EchoService","id":"x3Yuiq7g7MoxhXdxk7i4K7","version":"1.0.0","metadata":{},"type":"io.nats.micro.v1.ping_response"}

13:03:04 Received with rtt 1.317ms
{"name":"EchoService","id":"x3Yuiq7g7MoxhXdxk7i4Kt","version":"1.0.0","metadata":{},"type":"io.nats.micro.v1.ping_response"}

13:03:04 Received with rtt 1.320291ms
{"name":"EchoService","id":"x3Yuiq7g7MoxhXdxk7i4Lf","version":"1.0.0","metadata":{},"type":"io.nats.micro.v1.ping_response"}
```

A specific service instance info can be retrieved:

```sh
nats req '$SRV.INFO.EchoService.x3Yuiq7g7MoxhXdxk7i4K7' '' | jq

13:04:19 Sending request on "$SRV.INFO.EchoService.x3Yuiq7g7MoxhXdxk7i4K7"
13:04:19 Received with rtt 318.875µs
{
  "name": "EchoService",
  "id": "x3Yuiq7g7MoxhXdxk7i4K7",
  "version": "1.0.0",
  "metadata": {},
  "type": "io.nats.micro.v1.info_response",
  "description": "",
  "endpoints": [
    {
      "name": "default",
      "subject": "svc.echo",
      "queue_group": "q",
      "metadata": null
    }
  ]
}
```

To get statistics for this service:

```sh
nats req '$SRV.STATS.EchoService.x3Yuiq7g7MoxhXdxk7i4K7' '' | jq

13:04:46 Sending request on "$SRV.STATS.EchoService.x3Yuiq7g7MoxhXdxk7i4K7"
13:04:46 Received with rtt 678.25µs
{
  "name": "EchoService",
  "id": "x3Yuiq7g7MoxhXdxk7i4K7",
  "version": "1.0.0",
  "metadata": {},
  "type": "io.nats.micro.v1.stats_response",
  "started": "2024-09-24T11:02:55.564771Z",
  "endpoints": [
    {
      "name": "default",
      "subject": "svc.echo",
      "queue_group": "q",
      "num_requests": 0,
      "num_errors": 0,
      "last_error": "",
      "processing_time": 0,
      "average_processing_time": 0
    }
  ]
}
```

## Examples

For more detailed examples, refer to the `./test/example_test.go` directory in
this package.

## Documentation

The complete documentation is available on
[GoDoc](https://godoc.org/github.com/nats-io/nats.go/micro).
