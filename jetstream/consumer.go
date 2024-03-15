// Copyright 2022-2024 The NATS Authors
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

package jetstream

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/internal/syncx"
	"github.com/nats-io/nuid"
)

type (

	// Consumer contains methods for fetching/processing messages from a stream,
	// as well as fetching consumer info.
	//
	// This package provides two implementations of Consumer interface:
	//
	// - Standard named/ephemeral pull consumers. These consumers are created using
	//   CreateConsumer method on Stream or JetStream interface. They can be
	//   explicitly configured (using [ConsumerConfig]) and managed by the user,
	//   either from this package or externally.
	//
	// - Ordered consumers. These consumers are created using OrderedConsumer
	//   method on Stream or JetStream interface. They are managed by the library
	//   and provide a simple way to consume messages from a stream. Ordered
	//   consumers are ephemeral in-memory pull consumers and are resilient to
	//   deletes and restarts. They provide limited configuration options
	//   using [OrderedConsumerConfig].
	//
	// Consumer provides method for optimized continuous consumption of messages
	// using Consume and Messages methods, as well as simple one-off messages
	// retrieval using Fetch and Next methods.
	Consumer interface {
		// Fetch is used to retrieve up to a provided number of messages from a
		// stream. This method will send a single request and deliver either all
		// requested messages unless time out is met earlier. Fetch timeout
		// defaults to 30 seconds and can be configured using FetchMaxWait
		// option.
		//
		// By default, Fetch uses a 5s idle heartbeat for requests longer than
		// 10 seconds. For shorter requests, the idle heartbeat is disabled.
		// This can be configured using FetchHeartbeat option. If a client does
		// not receive a heartbeat message from a stream for more than 2 times
		// the idle heartbeat setting, Fetch will return [ErrNoHeartbeat].
		//
		// Fetch is non-blocking and returns MessageBatch, exposing a channel
		// for delivered messages.
		//
		// Messages channel is always closed, thus it is safe to range over it
		// without additional checks.
		Fetch(batch int, opts ...FetchOpt) (MessageBatch, error)

		// FetchBytes is used to retrieve up to a provided bytes from the
		// stream. This method will send a single request and deliver the
		// provided number of bytes unless time out is met earlier. FetchBytes
		// timeout defaults to 30 seconds and can be configured using
		// FetchMaxWait option.
		//
		// By default, FetchBytes uses a 5s idle heartbeat for requests longer than
		// 10 seconds. For shorter requests, the idle heartbeat is disabled.
		// This can be configured using FetchHeartbeat option. If a client does
		// not receive a heartbeat message from a stream for more than 2 times
		// the idle heartbeat setting, Fetch will return ErrNoHeartbeat.
		//
		// FetchBytes is non-blocking and returns MessageBatch, exposing a channel
		// for delivered messages.
		//
		// Messages channel is always closed, thus it is safe to range over it
		// without additional checks.
		FetchBytes(maxBytes int, opts ...FetchOpt) (MessageBatch, error)

		// FetchNoWait is used to retrieve up to a provided number of messages
		// from a stream. Unlike Fetch, FetchNoWait will only deliver messages
		// that are currently available in the stream and will not wait for new
		// messages to arrive, even if batch size is not met.
		//
		// FetchNoWait is non-blocking and returns MessageBatch, exposing a
		// channel for delivered messages.
		//
		// Messages channel is always closed, thus it is safe to range over it
		// without additional checks.
		FetchNoWait(batch int) (MessageBatch, error)

		// Consume will continuously receive messages and handle them
		// with the provided callback function. Consume can be configured using
		// PullConsumeOpt options:
		//
		// - Error handling and monitoring can be configured using ConsumeErrHandler
		//   option, which provides information about errors encountered during
		//   consumption (both transient and terminal)
		// - Consume can be configured to stop after a certain number of
		//   messages is received using StopAfter option.
		// - Consume can be optimized for throughput or memory usage using
		//   PullExpiry, PullMaxMessages, PullMaxBytes and PullHeartbeat options.
		//   Unless there is a specific use case, these options should not be used.
		//
		// Consume returns a ConsumeContext, which can be used to stop or drain
		// the consumer.
		Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error)

		// Messages returns MessagesContext, allowing continuously iterating
		// over messages on a stream. Messages can be configured using
		// PullMessagesOpt options:
		//
		// - Messages can be optimized for throughput or memory usage using
		//   PullExpiry, PullMaxMessages, PullMaxBytes and PullHeartbeat options.
		//   Unless there is a specific use case, these options should not be used.
		// - WithMessagesErrOnMissingHeartbeat can be used to enable/disable
		//   erroring out on MessagesContext.Next when a heartbeat is missing.
		//   This option is enabled by default.
		Messages(opts ...PullMessagesOpt) (MessagesContext, error)

		// Next is used to retrieve the next message from the consumer. This
		// method will block until the message is retrieved or timeout is
		// reached.
		Next(opts ...FetchOpt) (Msg, error)

		// Info fetches current ConsumerInfo from the server.
		Info(context.Context) (*ConsumerInfo, error)

		// CachedInfo returns ConsumerInfo currently cached on this consumer.
		// This method does not perform any network requests. The cached
		// ConsumerInfo is updated on every call to Info and Update.
		CachedInfo() *ConsumerInfo
	}

	createConsumerRequest struct {
		Stream string          `json:"stream_name"`
		Config *ConsumerConfig `json:"config"`
		Action string          `json:"action"`
	}
)

// Info fetches current ConsumerInfo from the server.
func (p *pullConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	infoSubject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiConsumerInfoT, p.stream, p.name))
	var resp consumerInfoResponse

	if _, err := p.jetStream.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}
	if resp.Error == nil && resp.ConsumerInfo == nil {
		return nil, ErrConsumerNotFound
	}

	p.info = resp.ConsumerInfo
	return resp.ConsumerInfo, nil
}

// CachedInfo returns ConsumerInfo currently cached on this consumer.
// This method does not perform any network requests. The cached
// ConsumerInfo is updated on every call to Info and Update.
func (p *pullConsumer) CachedInfo() *ConsumerInfo {
	return p.info
}

func upsertConsumer(ctx context.Context, js *jetStream, stream string, cfg ConsumerConfig, action string) (Consumer, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	req := createConsumerRequest{
		Stream: stream,
		Config: &cfg,
		Action: action,
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	consumerName := cfg.Name
	if consumerName == "" {
		if cfg.Durable != "" {
			consumerName = cfg.Durable
		} else {
			consumerName = generateConsName()
		}
	}
	if err := validateConsumerName(consumerName); err != nil {
		return nil, err
	}

	var ccSubj string
	if cfg.FilterSubject != "" && len(cfg.FilterSubjects) == 0 {
		if err := validateSubject(cfg.FilterSubject); err != nil {
			return nil, err
		}
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerCreateWithFilterSubjectT, stream, consumerName, cfg.FilterSubject))
	} else {
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerCreateT, stream, consumerName))
	}
	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, ccSubj, &resp, reqJSON); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeStreamNotFound {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}

	// check whether multiple filter subjects (if used) are reflected in the returned ConsumerInfo
	if len(cfg.FilterSubjects) != 0 && len(resp.Config.FilterSubjects) == 0 {
		return nil, ErrConsumerMultipleFilterSubjectsNotSupported
	}

	return &pullConsumer{
		jetStream: js,
		stream:    stream,
		name:      resp.Name,
		durable:   cfg.Durable != "",
		info:      resp.ConsumerInfo,
		subs:      syncx.Map[string, *pullSubscription]{},
	}, nil
}

const (
	consumerActionCreate         = "create"
	consumerActionUpdate         = "update"
	consumerActionCreateOrUpdate = ""
)

func generateConsName() string {
	name := nuid.Next()
	sha := sha256.New()
	sha.Write([]byte(name))
	b := sha.Sum(nil)
	for i := 0; i < 8; i++ {
		b[i] = rdigits[int(b[i]%base)]
	}
	return string(b[:8])
}

func getConsumer(ctx context.Context, js *jetStream, stream, name string) (Consumer, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	if err := validateConsumerName(name); err != nil {
		return nil, err
	}
	infoSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerInfoT, stream, name))

	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}
	if resp.Error == nil && resp.ConsumerInfo == nil {
		return nil, ErrConsumerNotFound
	}

	cons := &pullConsumer{
		jetStream: js,
		stream:    stream,
		name:      name,
		durable:   resp.Config.Durable != "",
		info:      resp.ConsumerInfo,
		subs:      syncx.Map[string, *pullSubscription]{},
	}

	return cons, nil
}

func deleteConsumer(ctx context.Context, js *jetStream, stream, consumer string) error {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	if err := validateConsumerName(consumer); err != nil {
		return err
	}
	deleteSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerDeleteT, stream, consumer))

	var resp consumerDeleteResponse

	if _, err := js.apiRequestJSON(ctx, deleteSubject, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return ErrConsumerNotFound
		}
		return resp.Error
	}
	return nil
}

func pauseConsumer(ctx context.Context, js *jetStream, stream, consumer string, pauseUntil *time.Time) (*ConsumerPauseResponse, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	if err := validateConsumerName(consumer); err != nil {
		return nil, err
	}
	subject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerPauseT, stream, consumer))

	var resp consumerPauseApiResponse
	req, err := json.Marshal(consumerPauseRequest{
		PauseUntil: pauseUntil,
	})
	if err != nil {
		return nil, err
	}
	if _, err := js.apiRequestJSON(ctx, subject, &resp, req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}
	return &ConsumerPauseResponse{
		Paused:         resp.Paused,
		PauseUntil:     resp.PauseUntil,
		PauseRemaining: resp.PauseRemaining,
	}, nil
}

func resumeConsumer(ctx context.Context, js *jetStream, stream, consumer string) (*ConsumerPauseResponse, error) {
	return pauseConsumer(ctx, js, stream, consumer, nil)
}

func validateConsumerName(dur string) error {
	if dur == "" {
		return fmt.Errorf("%w: '%s'", ErrInvalidConsumerName, "name is required")
	}
	if strings.ContainsAny(dur, ">*. /\\") {
		return fmt.Errorf("%w: '%s'", ErrInvalidConsumerName, dur)
	}
	return nil
}
