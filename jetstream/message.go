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
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/parser"
)

type (
	// Msg contains methods to operate on a JetStream message. Metadata, Data,
	// Headers, Subject and Reply can be used to retrieve the specific parts of
	// the underlying message. Ack, DoubleAck, Nak, NakWithDelay, InProgress and
	// Term are various flavors of ack requests.
	Msg interface {
		// Metadata returns [MsgMetadata] for a JetStream message.
		Metadata() (*MsgMetadata, error)

		// Data returns the message body.
		Data() []byte

		// Headers returns a map of headers for a message.
		Headers() nats.Header

		// Subject returns a subject on which a message was published/received.
		Subject() string

		// Reply returns a reply subject for a message.
		Reply() string

		// Ack acknowledges a message. This tells the server that the message was
		// successfully processed and it can move on to the next message.
		Ack() error

		// DoubleAck acknowledges a message and waits for ack reply from the server.
		// While it impacts performance, it is useful for scenarios where
		// message loss is not acceptable.
		DoubleAck(context.Context) error

		// Nak negatively acknowledges a message. This tells the server to
		// redeliver the message.
		//
		// Nak does not adhere to AckWait or Backoff configured on the consumer
		// and triggers instant redelivery. For a delayed redelivery, use
		// NakWithDelay.
		Nak() error

		// NakWithDelay negatively acknowledges a message. This tells the server
		// to redeliver the message after the given delay.
		NakWithDelay(delay time.Duration) error

		// InProgress tells the server that this message is being worked on. It
		// resets the redelivery timer on the server.
		InProgress() error

		// Term tells the server to not redeliver this message, regardless of
		// the value of MaxDeliver.
		Term() error

		// TermWithReason tells the server to not redeliver this message, regardless of
		// the value of MaxDeliver. The provided reason will be included in JetStream
		// advisory event sent by the server.
		//
		// Note: This will only work with JetStream servers >= 2.10.4.
		// For older servers, TermWithReason will be ignored by the server and the message
		// will not be terminated.
		TermWithReason(reason string) error
	}

	// MsgMetadata is the JetStream metadata associated with received messages.
	MsgMetadata struct {
		// Sequence is the sequence information for the message.
		Sequence SequencePair

		// NumDelivered is the number of times this message was delivered to the
		// consumer.
		NumDelivered uint64

		// NumPending is the number of messages that match the consumer's
		// filter, but have not been delivered yet.
		NumPending uint64

		// Timestamp is the time the message was originally stored on a stream.
		Timestamp time.Time

		// Stream is the stream name this message is stored on.
		Stream string

		// Consumer is the consumer name this message was delivered to.
		Consumer string

		// Domain is the domain this message was received on.
		Domain string
	}

	// SequencePair includes the consumer and stream sequence numbers for a
	// message.
	SequencePair struct {
		// Consumer is the consumer sequence number for message deliveries. This
		// is the total number of messages the consumer has seen (including
		// redeliveries).
		Consumer uint64 `json:"consumer_seq"`

		// Stream is the stream sequence number for a message.
		Stream uint64 `json:"stream_seq"`
	}

	jetStreamMsg struct {
		msg  *nats.Msg
		ackd bool
		js   *jetStream
		sync.Mutex
	}

	ackOpts struct {
		nakDelay   time.Duration
		termReason string
	}

	ackType []byte
)

const (
	controlMsg       = "100"
	badRequest       = "400"
	noMessages       = "404"
	reqTimeout       = "408"
	maxBytesExceeded = "409"
	noResponders     = "503"
)

// Headers used when publishing messages.
const (
	// MsgIdHeader is used to specify a user-defined message ID. It can be used
	// e.g. for deduplication in conjunction with the Duplicates duration on
	// ConsumerConfig or to provide optimistic concurrency safety together with
	// [ExpectedLastMsgIDHeader].
	//
	// This can be set when publishing messages using [WithMsgID] option.
	MsgIDHeader = "Nats-Msg-Id"

	// ExpectedStreamHeader contains stream name and is used to assure that the
	// published message is received by expected stream. Server will reject the
	// message if it is not the case.
	//
	// This can be set when publishing messages using [WithExpectStream] option.
	ExpectedStreamHeader = "Nats-Expected-Stream"

	// ExpectedLastSeqHeader contains the expected last sequence number of the
	// stream and can be used to apply optimistic concurrency control at stream
	// level. Server will reject the message if it is not the case.
	//
	// This can be set when publishing messages using [WithExpectLastSequence]
	// option. option.
	ExpectedLastSeqHeader = "Nats-Expected-Last-Sequence"

	// ExpectedLastSubjSeqHeader contains the expected last sequence number on
	// the subject and can be used to apply optimistic concurrency control at
	// subject level. Server will reject the message if it is not the case.
	//
	// This can be set when publishing messages using
	// [WithExpectLastSequencePerSubject] option.
	ExpectedLastSubjSeqHeader = "Nats-Expected-Last-Subject-Sequence"

	// ExpectedLastSubjSeqSubjHeader contains the subject to be used when
	// the subject expected last sequence number is set in ExpectedLastSubjSeqHeader.
	//
	// This can be set when publishing messages using
	// [WithExpectLastSequencePerSubjectForSubject] option.
	ExpectedLastSubjSeqSubjHeader = "Nats-Expected-Last-Subject-Sequence-Subject"

	// ExpectedLastMsgIDHeader contains the expected last message ID on the
	// subject and can be used to apply optimistic concurrency control at
	// stream level. Server will reject the message if it is not the case.
	//
	// This can be set when publishing messages using [WithExpectLastMsgID]
	// option.
	ExpectedLastMsgIDHeader = "Nats-Expected-Last-Msg-Id"

	// MsgRollup is used to apply a purge of all prior messages in the stream
	// ("all") or at the subject ("sub") before this message.
	MsgRollup = "Nats-Rollup"
)

// Headers for republished messages and direct gets. Those headers are set by
// the server and should not be set by the client.
const (
	// StreamHeader contains the stream name the message was republished from or
	// the stream name the message was retrieved from using direct get.
	StreamHeader = "Nats-Stream"

	// SequenceHeader contains the original sequence number of the message.
	SequenceHeader = "Nats-Sequence"

	// TimeStampHeader contains the original timestamp of the message.
	TimeStampHeaer = "Nats-Time-Stamp"

	// SubjectHeader contains the original subject the message was published to.
	SubjectHeader = "Nats-Subject"

	// LastSequenceHeader contains the last sequence of the message having the
	// same subject, otherwise zero if this is the first message for the
	// subject.
	LastSequenceHeader = "Nats-Last-Sequence"
)

// Rollups, can be subject only or all messages.
const (
	// MsgRollupSubject is used to purge all messages before this message on the
	// message subject.
	MsgRollupSubject = "sub"

	// MsgRollupAll is used to purge all messages before this message on the
	// stream.
	MsgRollupAll = "all"
)

var (
	ackAck      ackType = []byte("+ACK")
	ackNak      ackType = []byte("-NAK")
	ackProgress ackType = []byte("+WPI")
	ackTerm     ackType = []byte("+TERM")
)

// Metadata returns [MsgMetadata] for a JetStream message.
func (m *jetStreamMsg) Metadata() (*MsgMetadata, error) {
	if err := m.checkReply(); err != nil {
		return nil, err
	}

	tokens, err := parser.GetMetadataFields(m.msg.Reply)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrNotJSMessage, err)
	}

	meta := &MsgMetadata{
		Domain:       tokens[parser.AckDomainTokenPos],
		NumDelivered: parser.ParseNum(tokens[parser.AckNumDeliveredTokenPos]),
		NumPending:   parser.ParseNum(tokens[parser.AckNumPendingTokenPos]),
		Timestamp:    time.Unix(0, int64(parser.ParseNum(tokens[parser.AckTimestampSeqTokenPos]))),
		Stream:       tokens[parser.AckStreamTokenPos],
		Consumer:     tokens[parser.AckConsumerTokenPos],
	}
	meta.Sequence.Stream = parser.ParseNum(tokens[parser.AckStreamSeqTokenPos])
	meta.Sequence.Consumer = parser.ParseNum(tokens[parser.AckConsumerSeqTokenPos])
	return meta, nil
}

// Data returns the message body.
func (m *jetStreamMsg) Data() []byte {
	return m.msg.Data
}

// Headers returns a map of headers for a message.
func (m *jetStreamMsg) Headers() nats.Header {
	return m.msg.Header
}

// Subject returns a subject on which a message is published.
func (m *jetStreamMsg) Subject() string {
	return m.msg.Subject
}

// Reply returns a reply subject for a JetStream message.
func (m *jetStreamMsg) Reply() string {
	return m.msg.Reply
}

// Ack acknowledges a message. This tells the server that the message was
// successfully processed and it can move on to the next message.
func (m *jetStreamMsg) Ack() error {
	return m.ackReply(context.Background(), ackAck, false, ackOpts{})
}

// DoubleAck acknowledges a message and waits for ack reply from the server.
// While it impacts performance, it is useful for scenarios where
// message loss is not acceptable.
func (m *jetStreamMsg) DoubleAck(ctx context.Context) error {
	return m.ackReply(ctx, ackAck, true, ackOpts{})
}

// Nak negatively acknowledges a message. This tells the server to
// redeliver the message.
func (m *jetStreamMsg) Nak() error {
	return m.ackReply(context.Background(), ackNak, false, ackOpts{})
}

// NakWithDelay negatively acknowledges a message. This tells the server
// to redeliver the message after the given delay.
func (m *jetStreamMsg) NakWithDelay(delay time.Duration) error {
	return m.ackReply(context.Background(), ackNak, false, ackOpts{nakDelay: delay})
}

// InProgress tells the server that this message is being worked on. It
// resets the redelivery timer on the server.
func (m *jetStreamMsg) InProgress() error {
	return m.ackReply(context.Background(), ackProgress, false, ackOpts{})
}

// Term tells the server to not redeliver this message, regardless of
// the value of MaxDeliver.
func (m *jetStreamMsg) Term() error {
	return m.ackReply(context.Background(), ackTerm, false, ackOpts{})
}

// TermWithReason tells the server to not redeliver this message, regardless of
// the value of MaxDeliver. The provided reason will be included in JetStream
// advisory event sent by the server.
//
// Note: This will only work with JetStream servers >= 2.10.4.
// For older servers, TermWithReason will be ignored by the server and the message
// will not be terminated.
func (m *jetStreamMsg) TermWithReason(reason string) error {
	return m.ackReply(context.Background(), ackTerm, false, ackOpts{termReason: reason})
}

func (m *jetStreamMsg) ackReply(ctx context.Context, ackType ackType, sync bool, opts ackOpts) error {
	err := m.checkReply()
	if err != nil {
		return err
	}

	m.Lock()
	if m.ackd {
		m.Unlock()
		return ErrMsgAlreadyAckd
	}
	m.Unlock()

	if sync {
		var cancel context.CancelFunc
		ctx, cancel = wrapContextWithoutDeadline(ctx)
		if cancel != nil {
			defer cancel()
		}
	}

	var body []byte
	if opts.nakDelay > 0 {
		body = []byte(fmt.Sprintf("%s {\"delay\": %d}", ackType, opts.nakDelay.Nanoseconds()))
	} else if opts.termReason != "" {
		body = []byte(fmt.Sprintf("%s %s", ackType, opts.termReason))
	} else {
		body = ackType
	}

	if sync {
		_, err = m.js.conn.RequestWithContext(ctx, m.msg.Reply, body)
	} else {
		err = m.js.conn.Publish(m.msg.Reply, body)
	}
	if err != nil {
		return err
	}

	// Mark that the message has been acked unless it is ackProgress
	// which can be sent many times.
	if !bytes.Equal(ackType, ackProgress) {
		m.Lock()
		m.ackd = true
		m.Unlock()
	}
	return nil
}

func (m *jetStreamMsg) checkReply() error {
	if m == nil || m.msg.Sub == nil {
		return ErrMsgNotBound
	}
	if m.msg.Reply == "" {
		return ErrMsgNoReply
	}
	return nil
}

// Returns if the given message is a user message or not, and if
// checkSts() is true, returns appropriate error based on the
// content of the status (404, etc..)
func checkMsg(msg *nats.Msg) (bool, error) {
	// If payload or no header, consider this a user message
	if len(msg.Data) > 0 || len(msg.Header) == 0 {
		return true, nil
	}
	// Look for status header
	val := msg.Header.Get("Status")
	descr := msg.Header.Get("Description")
	// If not present, then this is considered a user message
	if val == "" {
		return true, nil
	}

	switch val {
	case badRequest:
		return false, ErrBadRequest
	case noResponders:
		return false, nats.ErrNoResponders
	case noMessages:
		// 404 indicates that there are no messages.
		return false, ErrNoMessages
	case reqTimeout:
		return false, nats.ErrTimeout
	case controlMsg:
		return false, nil
	case maxBytesExceeded:
		if strings.Contains(strings.ToLower(descr), "message size exceeds maxbytes") {
			return false, ErrMaxBytesExceeded
		}
		if strings.Contains(strings.ToLower(descr), "consumer deleted") {
			return false, ErrConsumerDeleted
		}
		if strings.Contains(strings.ToLower(descr), "leadership change") {
			return false, ErrConsumerLeadershipChanged
		}
	}
	return false, fmt.Errorf("nats: %s", msg.Header.Get("Description"))
}

func parsePending(msg *nats.Msg) (int, int, error) {
	msgsLeftStr := msg.Header.Get("Nats-Pending-Messages")
	var msgsLeft int
	var err error
	if msgsLeftStr != "" {
		msgsLeft, err = strconv.Atoi(msgsLeftStr)
		if err != nil {
			return 0, 0, errors.New("nats: invalid format of Nats-Pending-Messages")
		}
	}
	bytesLeftStr := msg.Header.Get("Nats-Pending-Bytes")
	var bytesLeft int
	if bytesLeftStr != "" {
		bytesLeft, err = strconv.Atoi(bytesLeftStr)
		if err != nil {
			return 0, 0, errors.New("nats: invalid format of Nats-Pending-Bytes")
		}
	}
	return msgsLeft, bytesLeft, nil
}

// toJSMsg converts core [nats.Msg] to [jetStreamMsg], exposing JetStream-specific operations
func (js *jetStream) toJSMsg(msg *nats.Msg) *jetStreamMsg {
	return &jetStreamMsg{
		msg: msg,
		js:  js,
	}
}
