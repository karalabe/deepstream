// Copyright 2016 Péter Szilágyi. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package deepstream implements a deepstream.io client.
package deepstream

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"golang.org/x/net/websocket"
)

// Client is an remote interface to a deepstream server.
type Client struct {
	conn *websocket.Conn // Underlying websocket connection to the deepstream server

	readBuf []byte    // Raw message currently being processed (reused between reads)
	oldBuf  []byte    // Old read buffer locked until batched messages exhaust
	msgBuf  []message // Buffer of messages currently processing (batched arrivals)

	loginRes  chan error // Result channel for a pending login operation (single allowed)
	loginLock sync.Mutex // Lock protecting the login channel

	eventOps  map[string]chan error   // Pending event operations waiting for a server ack or error
	eventSubs map[string]subscription // Currently live subscriptions that can receive published events
	eventLock sync.RWMutex            // Lock protecting the event operations and subscriptions

	quit chan chan error // Quit channel to synchronize protocol teardown with
}

// Dial connects to remote deepstream.io server. The returned connection is not
// authenticated and can only access publicly available data.
func Dial(url string) (*Client, error) {
	// Connect to the remote server and create a client
	conn, err := websocket.Dial(url, "", "http://localhost")
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:      conn,
		readBuf:   make([]byte, 0, 64*1024),
		eventOps:  make(map[string]chan error),
		eventSubs: make(map[string]subscription),
		quit:      make(chan chan error),
	}
	// Execute a protocol handshake, start the client and return
	if err = c.handshake(); err != nil {
		conn.Close()
		return nil, err
	}
	go c.loop()

	return c, err
}

// handshake waits for a connection challenge, responds to it and finally waits
// for the connection acknowledgement.
func (c *Client) handshake() error {
	// Wait for and validate the connection challenge
	topic, action, data, err := c.decode()
	if err != nil {
		return err
	}
	if topic != topicConnection || action != actionChallenge || len(data) != 0 {
		return fmt.Errorf("invalid challenge: %s|%s|%v+", topic, action, data)
	}
	// Respond to the challenge
	if err = c.encode(topicConnection, actionChallengeResponse, "localhost:6020"); err != nil {
		return err
	}
	// Wait for and validate the connection acknowledgement
	topic, action, data, err = c.decode()
	if err != nil {
		return err
	}
	if topic != topicConnection || action != actionAck || len(data) != 0 {
		return fmt.Errorf("invalid challenge ack: %s|%s|%v+", topic, action, data)
	}
	return nil
}

// Close terminates the remote connection.
func (c *Client) Close() error {
	// Close the network connection to unblock any reads
	c.conn.Close()

	// Wait until all message processing is done
	errc := make(chan error)
	c.quit <- errc
	fail := <-errc

	// Close all remaining channels
	c.loginLock.Lock()
	if c.loginRes != nil {
		close(c.loginRes)
	}
	c.loginLock.Unlock()

	// Finally return any failures
	return fail
}

// Login tries to authenticate the client with the provided credentials. On auth
// failure, the reason will be returned.
func (c *Client) Login(auth Authenticator) error {
	// Create a login result channel and make sure it's cleaned up
	c.loginLock.Lock()
	if c.loginRes != nil {
		c.loginLock.Unlock()
		return ErrPendingLogin
	}
	c.loginRes = make(chan error)
	c.loginLock.Unlock()

	defer func() {
		c.loginLock.Lock()
		c.loginRes = nil
		c.loginLock.Unlock()
	}()

	// Serialize the login credentials and send te authentication request
	if auth == nil {
		// No credentials given, use anonymous login
		auth = new(PasswordAuth)
	}
	// Encode the auth credentials and send to server
	blob, err := auth.EncodeAuth()
	if err != nil {
		return err
	}
	if err := c.encode(topicAuth, actionRequest, blob); err != nil {
		return err
	}
	// Wait for the login result and return
	res, ok := <-c.loginRes
	if !ok {
		return ErrClosed
	}
	return res
}

// loop spins until the connection is alive, reading messages and forwarding it
// to the appropriate channel.
func (c *Client) loop() {
	var errc chan error
	var fail error

	// Loop until we receive a quit request or hit an error
	for fail == nil && errc == nil {
		// Decode the next message from the data stream
		topic, action, data, err := c.decode()
		if err != nil {
			if err != io.EOF {
				fail = err
			}
			break
		}
		// Depending on its content, forward to the appropriate channel
		switch topic {
		case topicConnection:
			switch {
			case action == actionPing:
				if err := c.encode(topicConnection, actionPong); err != nil {
					fail = err
					break
				}
			default:
				fail = fmt.Errorf("unknown connecrtion action: %s", action)
			}
			continue

		case topicAuth:
			// If a login response came, send success or failure back
			c.loginLock.Lock()
			resc := c.loginRes
			c.loginLock.Unlock()

			switch {
			case resc == nil:
				fail = errors.New("no login pending")
			case action == actionAck:
				resc <- nil
			case action == actionError:
				resc <- fmt.Errorf("%s", data)
			default:
				fail = fmt.Errorf("unknown auth action: %s", action)
			}
			continue

		case topicEvent:
			// An event topic may either receive an operation result or a publish
			switch action {
			case actionAck, actionError:
				fail = c.finishEventOperation(action, data)
			case actionEvent:
				fail = c.processEvent(data)
			default:
				fail = fmt.Errorf("unknown event action: %s", action)
			}
			continue
		}
		fmt.Println(topic, action, data)
	}
	// Client terminating, wait for quit channel and return
	if errc == nil {
		errc = <-c.quit
	}
	errc <- fail
}
