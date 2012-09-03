// Copyright 2012 Apcera Inc. All rights reserved.

// A Go client for the NATS messaging system (https://github.com/derekcollison/nats).
package nats

import (
	"fmt"
	"net"
	"net/url"
	"io"
	"bufio"
	"strings"
	"sync/atomic"
	"encoding/json"
	"time"
	"runtime"
	"errors"
	"sync"
	"strconv"
	"encoding/hex"
	"crypto/rand"
	"crypto/tls"
	"unsafe"
)

const (
	Version               = "0.32"
	DefaultURL            = "nats://localhost:4222"
	DefaultPort           = 4222
	DefaultMaxReconnect   = 10
	DefaultReconnectWait  = 2*time.Second
	DefaultTimeout        = 2*time.Second
)

var (
	ErrConnectionClosed   = errors.New("nats: Connection closed")
	ErrSecureConnRequired = errors.New("nats: Secure connection required")
	ErrSecureConnWanted   = errors.New("nats: Secure connection not available")
	ErrBadSubscription    = errors.New("nats: Invalid Subscription")
	ErrSlowConsumer       = errors.New("nats: Slow consumer, messages dropped")
	ErrTimeout            = errors.New("nats: Timeout")
)

var DefaultOptions = Options {
	AllowReconnect : true,
	MaxReconnect   : DefaultMaxReconnect,
	ReconnectWait  : DefaultReconnectWait,
	Timeout        : DefaultTimeout,
}

// Do we care?
type Status int
const (
	DISCONNECTED Status = iota
	CONNECTED    Status = iota
	CLOSED       Status = iota
	RECONNECTING Status = iota
)

type Handler func(*Conn)

// Options can be used to create a customized Connection.
type Options struct {
	Url            string
	Verbose        bool
	Pedantic       bool
	Secure         bool
	AllowReconnect bool
	MaxReconnect   uint
	ReconnectWait  time.Duration
	Timeout        time.Duration
	ClosedCB       Handler
	DisconnectedCB Handler
}

// Msg is a structure used by Subscribers and PublishMsg().
type Msg struct {
	Subject string
	Reply   string
	Data    []byte
	Sub     *Subscription
}

// MsgHandler is a callback function that processes messages delivered to
// asynchronous subscribers.
type MsgHandler func(msg *Msg)

// Connect will attempt to connect to the NATS server.
// The url can contain username/password semantics.
func Connect(url string) (*Conn, error) {
	opts := DefaultOptions
	opts.Url = url
	return opts.Connect()
}

// SecureConnect will attempt to connect to the NATS server using TLS.
// The url can contain username/password semantics.
func SecureConnect(url string) (*Conn, error) {
	opts := DefaultOptions
	opts.Url = url
	opts.Secure = true
	return opts.Connect()
}

// Connect will attempt to connect to a NATS server with multiple options.
func (o Options) Connect() (*Conn, error) {
	nc := &Conn{opts:o}
	var err error
	nc.url, err = url.Parse(o.Url)
	if err != nil {
		return nil, err
	}
	err = nc.connect()
	if err != nil {
		return nil, err
	}
	return nc, nil
}

// A Conn represents a connection to a nats-server
type Conn struct {
	sync.Mutex
	Stats
	url     *url.URL
	opts    Options
	conn    net.Conn
	bw      *bufio.Writer
	br      *bufio.Reader
	fch     chan bool
	info    serverInfo
	ssid    uint64
	subs    map[uint64]*Subscription
	mch     chan *Msg
	pongs   []chan bool
	status  Status
	sc      bool
	err     error
	closed  bool
}

// Tracks various stats received and sent on this connection,
// including message and bytes counts.
type Stats struct {
	InMsgs, OutMsgs, InBytes, OutBytes uint64
}

type serverInfo struct {
	Id           string `json:"server_id"`
	Host         string `json:"host"`
	Port         uint   `json:"port"`
	Version      string `json:"version"`
	AuthRequired bool   `json:"auth_required"`
	SslRequired  bool   `json:"ssl_required"`
	MaxPayload   int64  `json:"max_payload"`
}

type connectInfo struct {
	Verbose  bool   `json:"verbose"`
	Pedantic bool   `json:"pedantic"`
	User     string `json:"user,omitempty"`
	Pass     string `json:"pass,omitempty"`
	Ssl      bool   `json:"ssl_required"`
}

const (
	_CRLF_    = "\r\n"
	_EMPTY_   = ""
	_SPC_     = " "
)

const (
	_OK_OP_   = "+OK"
	_ERR_OP_  = "-ERR"
	_MSG_OP_  = "MSG"
	_PING_OP_ = "PING"
	_PONG_OP_ = "PONG"
	_INFO_OP_ = "INFO"
)

const (
	conProto   = "CONNECT %s"   + _CRLF_
	pingProto  = "PING"         + _CRLF_
	pongProto  = "PONG"         + _CRLF_
	pubProto   = "PUB %s %s %d" + _CRLF_
	subProto   = "SUB %s %s %d" + _CRLF_
	unsubProto = "UNSUB %d %s"  + _CRLF_
)

// The size of the buffered channel used between the socket
// go routine and the message delivery or sync subscription.
const maxChanLen     = 8192

// The size of the bufio reader/writer on top of the socket.
const defaultBufSize = 32768

// Main connect function. Will connect to the nats-server
func (nc *Conn) connect() error {

	// FIXME: Check for 0 Timeout
	nc.conn, nc.err = net.DialTimeout("tcp", nc.url.Host, nc.opts.Timeout)
	if nc.err != nil {
		return nc.err
	}

	nc.subs  = make(map[uint64]*Subscription)
	nc.mch   = make(chan *Msg, maxChanLen)
	nc.pongs = make([] chan bool, 0, 8)

	nc.bw = bufio.NewWriterSize(nc.conn, defaultBufSize)
	nc.br = bufio.NewReaderSize(nc.conn, defaultBufSize)

	nc.fch = make(chan bool, 1024) //FIXME, need to define

	// Make sure to process the INFO inline here.
	if nc.err = nc.processExpectedInfo(); nc.err != nil {
		return nc.err
	}

	go nc.readLoop()
	go nc.deliverMsgs()
	go nc.flusher()

	runtime.SetFinalizer(nc, fin)
	return nc.sendConnect()
}

// This will check to see if the connection should be
// secure. This can be dictated from either end and should
// only be called after the INIT protocol has been received.
func (nc *Conn) checkForSecure() error {
	// Check to see if we need to engage TLS
	o := nc.opts

	// Check for mismatch in setups
	if o.Secure && !nc.info.SslRequired {
		return ErrSecureConnWanted
	} else if nc.info.SslRequired && !o.Secure {
		return ErrSecureConnRequired
	}

	if o.Secure {
		nc.conn = tls.Client(nc.conn, &tls.Config{InsecureSkipVerify: true})
		nc.bw = bufio.NewWriterSize(nc.conn, defaultBufSize)
		nc.br = bufio.NewReaderSize(nc.conn, defaultBufSize)
	}
	return nil
}

// processExpectedInfo will look for the expected first INFO message
// sent when a connection is established.
func (nc *Conn) processExpectedInfo() error {
	nc.conn.SetReadDeadline(time.Now().Add(100*time.Millisecond))
	defer nc.conn.SetReadDeadline(time.Time{})

	c := &control{}
	if err := nc.readOp(c); err != nil {
		fmt.Printf("Received an err inside first INFO: %v\n", err)
	}
	// The nats protocol should send INFO forst always.
	if c.op != _INFO_OP_ {
		e := errors.New("nats: Protocol exception, INFO not received")
		nc.processReadOpErr(e)
		return e
	}
	nc.processInfo(c.args)
	return nc.checkForSecure()
}

// Sends a protocol control message by queueing into the bufio writer
// and kicking the flush go routine.  These writes are protected.
func (nc *Conn) sendProto(proto string) {
	nc.Lock()
	nc.bw.WriteString(proto)
	nc.Unlock()
	nc.fch <- true
}

// Send a connect protocol message to the server, issuing user/password if
// applicable. Will wait for a flush to return from the server for error
// processing.
func (nc *Conn) sendConnect() error {
	o := nc.opts
	var user, pass string
	u := nc.url.User
	if u != nil {
		user = u.Username()
		pass, _ = u.Password()
	}

	cinfo := connectInfo{o.Verbose, o.Pedantic, user, pass, o.Secure}
	b, err := json.Marshal(cinfo)
	if err != nil {
		nc.err = errors.New("nats: Connection message, json parse failed")
		return nc.err
	}
	nc.sendProto(fmt.Sprintf(conProto, b))


	err = nc.FlushTimeout(DefaultTimeout)
	if err != nil {
		nc.err = err
		return err
	} else if nc.closed {
		return nc.err
	}
	nc.status = CONNECTED
	return nil
}

// A control protocol line.
type control struct {
	op, args string
}

// Read a control line and process the intended op.
func (nc *Conn) readOp(c *control) error {
	if nc.closed {
		return ErrConnectionClosed
	}
	b, pre, err := nc.br.ReadLine()
	if err != nil {
		return err
	}
	if pre {
		// FIXME: Be more specific here?
		return errors.New("nats: Line too long")
	}
	// Do straight move to string rep
	line := *(*string)(unsafe.Pointer(&b))
	parseControl(line, c)
	return nil
}

// Parse a control line from the server.
func parseControl(line string, c *control) {
	toks := strings.SplitN(line, _SPC_, 2)
	if len(toks) == 1 {
		c.op   = strings.TrimSpace(toks[0])
		c.args = _EMPTY_
	} else if len(toks) == 2 {
		c.op, c.args = strings.TrimSpace(toks[0]), strings.TrimSpace(toks[1])
	} else {
		c.op = _EMPTY_
	}
}

func (nc *Conn) processDisconnect() {
	nc.status = DISCONNECTED
	if nc.err != nil { return }
	if nc.info.SslRequired {
		nc.err = ErrSecureConnRequired
	} else {
		nc.err = ErrConnectionClosed
	}
}

// processReadOpErr handles errors from readOp.
func (nc *Conn) processReadOpErr(err error) {
	if err == io.EOF {
		nc.processDisconnect()
	}
	nc.err = err
	nc.Close()
}

// readLoop() will sit on the buffered socket reading and processing the protocol
// from the server. It will dispatch appropriately based on the op type.
func (nc *Conn) readLoop() {
	c := &control{}
	for !nc.closed {
		if err := nc.readOp(c); err != nil {
			nc.processReadOpErr(err)
			break
		}
		switch c.op {
		case _MSG_OP_:
			nc.processMsg(c.args)
		case _OK_OP_:
			processOK()
		case _PING_OP_:
			nc.processPing()
		case _PONG_OP_:
			nc.processPong()
		case _INFO_OP_:
			nc.processInfo(c.args)
		case _ERR_OP_:
			nc.processErr(c.args)
		}
	}
}

// deliverMsgs waits on the delivery channel shared with readLoop and processMsg.
// It is used to deliver messages to asynchronous subscribers.
func (nc *Conn) deliverMsgs() {
	for !nc.closed {
		m, ok := <- nc.mch
		if !ok { break }
		s := m.Sub
		if (!s.IsValid() || s.mcb == nil) { continue }
		// FIXME, race on compare
		s.delivered = atomic.AddUint64(&s.delivered, 1)
		if s.max <= 0 || s.delivered <= s.max {
			s.mcb(m)
		}
	}
}

// processMsg is called by readLoop and will parse a message and place it on
// the appropriate channel for processing. Asynchronous subscribers will all
// share the channel that is processed by deliverMsgs. Sync subscribers have
// their own channel. If either channel is full, the connection is considered
// a slow subscriber.
func (nc *Conn) processMsg(args string) {
	var subj, reply  string
	var sid          uint64
	var n, blen      int
	var err          error

	num := strings.Count(args, _SPC_) + 1

	switch num {
	case 3:
		n, err = fmt.Sscanf(args, "%s %d %d", &subj, &sid, &blen)
	case 4:
		n, err = fmt.Sscanf(args, "%s %d %s %d", &subj, &sid, &reply, &blen)
	}
	if err != nil || n != num {
		nc.err = errors.New("nats: Parse exception processing msg")
		nc.Close()
		return
	}

	// Grab payload here.
	buf := make([]byte, blen)
	n, err = io.ReadFull(nc.br, buf)
	if err != nil || n != blen {
		nc.err = err
		nc.Close()
		return
	}

	// FIXME, locks?
	nc.Lock()
	// Stats
	nc.InMsgs  += 1
	nc.InBytes += uint64(blen)
	sub := nc.subs[sid]
	defer nc.Unlock()

	if sub == nil || (sub.max > 0 && sub.msgs > sub.max) {
		return
	}
	sub.Lock()
	defer sub.Unlock()
	sub.msgs += 1

	// FIXME: Should we recycle these containers
	m := &Msg{Data:buf, Subject:subj, Reply:reply, Sub:sub}

	if sub.mcb != nil {
		if len(nc.mch) >= maxChanLen {
			nc.sc = true
		} else {
			nc.mch <- m
		}
	} else if sub.mch != nil {
		if len(sub.mch) >= maxChanLen {
			sub.sc = true
		} else {
			sub.mch <- m
		}
	}
}

// flusher is a separate go routine that will process flush requests for the write
// bufio. This allows coalescing of writes to the underlying socket.
func (nc *Conn) flusher() {
	var b int
	var ok bool

	for !nc.closed {
		_, ok = <- nc.fch
		if !ok { return }

		nc.Lock()
		b = nc.bw.Buffered()
		if b > 0 {
			nc.bw.Flush()
		}
		nc.Unlock()
	}
}

// processPing will send an immediate pong protocol response to the
// server. The server uses this mechanism to detect dead clients.
func (nc *Conn) processPing() {
	nc.sendProto(pongProto)
}

// processPong is used to process responses to the client's ping
// messages. We use pings for the flush mechanism as well.
func (nc *Conn) processPong() {
	nc.Lock()
	ch := nc.pongs[0]
	nc.pongs = nc.pongs[1:]
	nc.Unlock()
	if ch != nil {
		ch <- true
	}
}

// processOK is a placeholder for processing ok messages.
func processOK() {
}

// processInfo is used to parse the info messages sent
// from the server.
func (nc *Conn) processInfo(info string) {
	if info == _EMPTY_ { return }
	err := json.Unmarshal([]byte(info), &nc.info)
	if err != nil {
		// FIXME HERE TOO ?
	}
}

// LastError reports the last error encountered via the Connection.
func (nc *Conn) LastError() error {
	return nc.err
}

// processErr processes any error messages from the server and
// sets the connection's lastError.
func (nc *Conn) processErr(e string) {
	nc.err = errors.New("nats: " + e)
	nc.Close()
}

// publish is the internal function to publish messages to a nats-server.
// Sends a protocol data message by queueing into the bufio writer
// and kicking the flush go routine. These writes are protected.
func (nc *Conn) publish(subj, reply string, data []byte) error {
	nc.Lock()
	if nc.closed {
		nc.Unlock()
		return ErrConnectionClosed
	}
	nc.bw.WriteString(fmt.Sprintf(pubProto, subj, reply, len(data)))
	nc.bw.Write(data)
	nc.bw.WriteString(_CRLF_)
	nc.OutMsgs  += 1
	nc.OutBytes += uint64(len(data))
	nc.Unlock()
	nc.fch <- true
	return nil
}

// Publish publishes the data argument to the given subject.
func (nc *Conn) Publish(subj string, data []byte) error {
	return nc.publish(subj, _EMPTY_, data)
}

// PublishMsg publishes the Msg structure, which includes the
// Subject, and optional Reply, and Optional Data fields.
func (nc *Conn) PublishMsg(m *Msg) error {
	return nc.publish(m.Subject, m.Reply, m.Data)
}

// Request will perform and Publish() call with an auto generated Inbox
// reply and return the first reply received. This is optimized for the
// case of multiple responses.
func (nc *Conn) Request(subj string, data []byte, timeout time.Duration) (*Msg, error) {
	inbox := NewInbox()
	s, err := nc.SubscribeSync(inbox)
	if err != nil { return nil, err }
	s.AutoUnsubscribe(1)
	defer s.Unsubscribe()
	err = nc.publish(subj, inbox, data)
	if err != nil { return nil, err }
	return s.NextMsg(timeout)
}

// A Subscription represents interest in a given subject.
type Subscription struct {
	sync.Mutex
	sid           uint64

	// Subject that represents this subscription. This can be different
	// then the received subject inside a Msg if this is a wildcard.
	Subject       string

	// Optional queue group name. If present, all subscriptions with the
	// same name will form a distributed queue, and each message will
	// only be processed by one member of the group.
	Queue         string

	msgs          uint64
	delivered     uint64
	bytes         uint64
	max           uint64
	conn          *Conn
	mcb           MsgHandler
	mch           chan *Msg
	sc            bool
}

// NewInbox will return an inbox string which can be used for directed replies from
// subscribers. These are guaranteed to be unique, but can be shared and subscribed
// to by others.
func NewInbox() string {
	u := make([]byte, 13)
	io.ReadFull(rand.Reader, u)
	return fmt.Sprintf("_INBOX.%s", hex.EncodeToString(u))
}

// subscribe is the internal subscribe function that indicates interest in subjects.
func (nc *Conn) subscribe(subj, queue string, cb MsgHandler) (*Subscription, error) {
	nc.Lock()
	if nc.closed {
		nc.Unlock()
		return nil, ErrConnectionClosed
	}

	sub := &Subscription{Subject: subj, mcb: cb, conn:nc}
	if cb == nil {
		// Indicates a sync subscription
		sub.mch = make(chan *Msg, maxChanLen)
	}
	sub.sid = atomic.AddUint64(&nc.ssid, 1)
	nc.subs[sub.sid] = sub
	nc.bw.WriteString(fmt.Sprintf(subProto, subj, queue, sub.sid))
	nc.Unlock()
	nc.fch <- true
	return sub, nil
}

// Subscribe will express interest in a given subject. The subject
// can have wildcards (partial:*, full:>). Messages will be delivered
// to the associated MsgHandler. If no MsgHandler is given, the
// subscription is a synchronous subscription and can be polled via
// Subscription.NextMsg()
func (nc *Conn) Subscribe(subj string, cb MsgHandler) (*Subscription, error) {
	return nc.subscribe(subj, _EMPTY_, cb)
}

// SubscribeSync is syntactic sugar for Subscribe(subject, nil)
func (nc *Conn) SubscribeSync(subj string) (*Subscription, error) {
	return nc.subscribe(subj, _EMPTY_, nil)
}

// QueueSubscribe creates a queue subscriber on the given subject. All
// subscribers with the same queue name will form the queue group, and
// only one member of the group will be selected to receive any given
// message.
func (nc *Conn) QueueSubscribe(subj, queue string, cb MsgHandler) (*Subscription, error) {
	return nc.subscribe(subj, queue, cb)
}

// QueueSubscribeSync creates a synchronous queue subscriber on the given
// subject. All subscribers with the same queue name will form the queue
// group, and only one member of the group will be selected to receive any
// given message.
func (nc *Conn) QueueSubscribeSync(subj, queue string) (*Subscription, error) {
	return nc.subscribe(subj, queue, nil)
}

// unsubscribe performs the low level unsubscribe to the server.
// Use Subscription.Unsubscribe()
func (nc *Conn) unsubscribe(sub *Subscription, max int) error {
	nc.Lock()
	if nc.closed {
		nc.Unlock()
		return ErrConnectionClosed
	}
	s := nc.subs[sub.sid]
	// Already unsubscribed
	if s == nil {
		nc.Unlock()
		return nil
	}
	maxStr := _EMPTY_
	if max > 0 {
		s.max = uint64(max)
		maxStr = strconv.Itoa(max)
	} else {
		delete(nc.subs, s.sid)
		s.Lock()
		if s.mch != nil {
			close(s.mch)
			s.mch = nil
		}
		// Mark as invalid
		s.conn = nil
		s.Unlock()
	}
	nc.bw.WriteString(fmt.Sprintf(unsubProto, s.sid, maxStr))
	nc.Unlock()
	nc.fch <- true
	return nil
}

// IsValid returns a boolean indicating whether the subscription
// is still active. This will return false if the subscription has
// already been closed.
func (s *Subscription) IsValid() bool {
	s.Lock()
	defer s.Unlock()
	return s.conn != nil
}

// Unsubscribe will remove interest in a given subject.
func (s *Subscription) Unsubscribe() error {
	s.Lock()
	conn := s.conn
	s.Unlock()
	if conn == nil {
		return ErrBadSubscription
	}
	return conn.unsubscribe(s, 0)
}

// AutoUnsubscribe will issue an automatic Unsubscribe that is
// processed by the server when max messages have been received.
// This can be useful when sending a request to an unknown number
// of subscribers. Request() uses this functionality.
func (s *Subscription) AutoUnsubscribe(max int) error {
	s.Lock()
	conn := s.conn
	s.Unlock()
	if conn == nil {
		return ErrBadSubscription
	}
	return conn.unsubscribe(s, max)
}

// NextMsg() will return the next message available to a synchronous subscriber,
// or block until one is available. A timeout can be used to return when no
// message has been delivered.
func (s *Subscription) NextMsg(timeout time.Duration) (msg *Msg, err error) {
	s.Lock()
	if s.mch == nil {
		s.Unlock()
		return nil, ErrConnectionClosed
	}
	if s.mcb != nil {
		s.Unlock()
		return nil, errors.New("nats: Illegal call on an async Subscription")
	}
	if s.conn == nil {
		s.Unlock()
		return nil, ErrBadSubscription
	}
	if s.sc {
		s.sc = false
		s.Unlock()
		return nil, ErrSlowConsumer
	}

	mch := s.mch
	s.Unlock()

	var ok bool
	t := time.NewTimer(timeout)
	defer t.Stop()

	select {
	case msg, ok = <- mch:
		if !ok {
			return nil, ErrConnectionClosed
		}
		s.delivered = atomic.AddUint64(&s.delivered, 1)
		if s.max > 0 && s.delivered > s.max {
			return nil, errors.New("nats: Max messages delivered")
		}
	case <-t.C:
		return nil, ErrTimeout
	}
	return
}

// FIXME: This is a hack
// removeFlushEntry is needed when we need to discard queued up responses
// for our pings, as part of a flush call. This happens when we have a flush
// call outstanding and we call close.
func (nc *Conn) removeFlushEntry(ch chan bool) bool {
	nc.Lock()
	defer nc.Unlock()
	if nc.pongs == nil { return false }
	for i,c := range nc.pongs {
		if c == ch {
			nc.pongs[i] = nil
			return true
		}
	}
	return false
}

// FlushTimeout allows a Flush operation to have an associated timeout.
func (nc *Conn) FlushTimeout(timeout time.Duration) (err error) {
	if (timeout <= 0) {
		return errors.New("nats: Bad timeout value")
	}

	nc.Lock()
	if nc.closed {
		nc.Unlock()
		return ErrConnectionClosed
	}
	t := time.NewTimer(timeout)
	defer t.Stop()

	ch := make(chan bool) // FIXME Inefficient?
	defer close(ch)

	nc.pongs = append(nc.pongs, ch)
	nc.bw.WriteString(pingProto)
	nc.bw.Flush()
	nc.Unlock()

	select {
	case _, ok := <-ch:
		if !ok {
			err = ErrConnectionClosed
		}
		if nc.sc {
			err = ErrSlowConsumer
		}
	case <-t.C:
		err = ErrTimeout
	}

	if err != nil {
		nc.removeFlushEntry(ch)
	}
	return
}

// Flush will perform a round trip to the server and return when it
// receives the internal reply.
func (nc *Conn) Flush() error {
	return nc.FlushTimeout(60*time.Second)
}

// Close will close the connection to the server. This call will release
// all blocking calls, such as Flush() and NextMsg()
func (nc *Conn) Close() {
	nc.Lock()
	if nc.closed {
		nc.Unlock()
		return
	}
	// FIXME, use status?
	nc.closed = true
	nc.Unlock()

	// Kick the go routines so they fall out.
	// fch will be closed on finalizer
	nc.fch <- true
	close(nc.mch)

	// Clear any queued pongs, e.g. pending flush calls.
	for _, ch := range nc.pongs {
		if ch != nil {
			ch <- true
		}
	}
	nc.pongs = nil

	// Close synch subscriber channels and release any
	// pending NextMsg() calls.
	for _, s := range nc.subs {
		if s.mch != nil {
			close(s.mch)
			s.mch = nil
		}
	}
	nc.subs = nil

	// Perform appropriate callback if needed for a disconnect.
	if nc.opts.DisconnectedCB != nil {
		nc.opts.DisconnectedCB(nc)
	}

	// Go ahead and make sure we have flushed the outbound buffer.
	nc.Lock()
	nc.bw.Flush()
	nc.conn.Close()
	nc.Unlock()

	nc.status = CLOSED

	// Perform appropriate callback if needed for a connection closed.
	if nc.opts.ClosedCB != nil {
		nc.opts.ClosedCB(nc)
	}
}

// Used for a garbage collection finalizer on dangling connections.
// Should not be needed as Close() should be called, but here for
// completeness.
func fin(nc *Conn) {
	nc.Close()
	close(nc.fch)
}
