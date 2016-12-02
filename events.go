// Copyright 2016 Péter Szilágyi. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Contains the event handling code of the deepstream client.

package deepstream

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// subscription contains some otimizations fields for subscriptions to avoid costly
// reflection operations.
type subscription struct {
	sink  reflect.Value        // Sink channel for some data type
	maker func() reflect.Value // Creates an instance of the sink item type
	dead  bool                 // Flag whether the subscription died (overflow)
}

// Subscribe executes a subscripion request to the server, returning whether it
// succeeded or not. The caller is required to provide an input channel to feed
// arriving events into. The channel will be automatically closed upon unsubscribe
// or connection termination.
func (c *Client) Subscribe(topic string, sinkChan interface{}) error {
	// Ensure the user provided channel is valid
	if sinkChan == nil {
		return ErrNilChannel
	}
	if reflect.TypeOf(sinkChan).Kind() != reflect.Chan {
		return ErrNotChannel
	}
	// Create an event subscription result channel and make sure it's cleaned up
	id := "s:" + topic

	c.eventLock.Lock()
	if _, exists := c.eventSubs[id]; exists || c.eventOps[id] != nil {
		c.eventLock.Unlock()
		return ErrAlreadySubscribed
	}
	subRes := make(chan error)
	c.eventOps[id] = subRes
	c.eventLock.Unlock()

	defer func() {
		c.eventLock.Lock()
		delete(c.eventOps, id)
		c.eventLock.Unlock()
	}()
	// Send subscription request and wait for response
	if err := c.encode(topicEvent, actionSubscribe, topic); err != nil {
		return err
	}
	res, ok := <-subRes
	if !ok {
		return ErrClosed
	}
	if res == nil {
		elem := reflect.TypeOf(sinkChan).Elem()

		c.eventLock.Lock()
		c.eventSubs[topic] = subscription{
			sink: reflect.ValueOf(sinkChan),
			maker: func() reflect.Value {
				return reflect.New(elem)
			},
		}
		c.eventLock.Unlock()
	}
	return res
}

// Unsubscribe executes an unsubscripion request to the server, returning whether
// it succeeded or not. On success, the original channel the client subscribed
// with will be also closed.
func (c *Client) Unsubscribe(topic string) error {
	// Create an event subscription result channel and make sure it's cleaned up
	id := "u:" + topic

	c.eventLock.Lock()
	if _, exists := c.eventSubs[id]; exists || c.eventOps[id] != nil {
		c.eventLock.Unlock()
		return ErrNotSubscribed
	}
	unsubRes := make(chan error)
	c.eventOps[id] = unsubRes
	c.eventLock.Unlock()

	defer func() {
		c.eventLock.Lock()
		delete(c.eventOps, id)
		c.eventLock.Unlock()
	}()
	// Send unsubscription request and wait for response
	if err := c.encode(topicEvent, actionUnsubscribe, topic); err != nil {
		return err
	}
	res, ok := <-unsubRes
	if !ok {
		return ErrClosed
	}
	if res == nil {
		c.eventLock.Lock()
		c.eventSubs[topic].sink.Close()
		delete(c.eventSubs, topic)
		c.eventLock.Unlock()
	}
	return res
}

// finishEventOperation is called by the read loop when it detects a pending
// event operation completion data packet.
func (c *Client) finishEventOperation(action action, data [][]byte) error {
	// Make sure the reply contains all the necessary fields (type, name)
	if len(data) < 2 {
		return fmt.Errorf("missing event completion fields: %v", data)
	}
	// Construct the operation ID to acknowledge
	var id string

	switch actionMapping[string(data[0])] {
	case actionSubscribe:
		id = "s:" + string(data[1])
	case actionUnsubscribe:
		id = "u:" + string(data[1])
	default:
		return fmt.Errorf("unknown event completion action: %s", data[0])
	}
	// Make sure the operation exists and notify
	c.eventLock.RLock()
	resc := c.eventOps[id]
	c.eventLock.RUnlock()

	if resc == nil {
		return fmt.Errorf("no event operation pending: %s", id)
	}
	if action == actionAck {
		resc <- nil
		return nil
	}
	// Make sure the reply contains all the necessary fields (type, name)
	if len(data) < 3 {
		return fmt.Errorf("missing event failure fields: %v", data)
	}
	resc <- fmt.Errorf("%s", data[2])
	return nil
}

// Publish injects a new event into a topic, returning whether the server confirmed
// the insertion or not.
func (c *Client) Publish(topic string, event interface{}) error {
	blob, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := c.encode(topicEvent, actionEvent, topic, typed{blob}); err != nil {
		return err
	}
	return nil
}

// processEvent is called by the read loop when it detects a new event being
// delivered on a subscribed channel.
func (c *Client) processEvent(data [][]byte) error {
	// Make sure the event contains all the necessary fields (name, data)
	if len(data) < 2 {
		return fmt.Errorf("missing event fields: %v", data)
	}
	// Strip out the type prefix, we don't allow dynamics anyway
	if len(data[1]) > 0 {
		data[1] = data[1][1:]
	}
	// Look up the event sink for the subscription
	c.eventLock.RLock()
	sub, ok := c.eventSubs[string(data[0])]
	c.eventLock.RUnlock()

	if ok && !sub.dead {
		// Explicit topic subscription live, deliver into it
		item := sub.maker()
		if err := json.Unmarshal(data[1], item.Interface()); err != nil {
			return err
		}
		if !sub.sink.TrySend(item.Elem()) {
			// Subscription is full, kill and unsubscribe
			sub.dead = true
			go c.Unsubscribe(string(data[0]))
		}
	}
	return nil
}
