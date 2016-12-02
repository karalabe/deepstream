// Copyright 2016 Péter Szilágyi. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package deepstream

import (
	"bytes"
	"fmt"
	"sync"
)

// message is a simple container for an incomming message to buffer up batched
// ones into individual deliverables.
type message struct {
	topic  topic
	action action
	data   [][]byte
}

// typed is a data blob wrapper to signal whether a data item needs to be type
// prefixed.
type typed struct {
	Content interface{}
}

// blobCache is a pool of blobs used for encoding and decoding in order to prevent
// insane number of memory allocations and frees.
var blobCache = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 64*1024)
	},
}

// encode encodes a new message in the deepstream format and sends it into the
// underlying network connection.
func (c *Client) encode(topic topic, action action, data ...interface{}) error {
	//fmt.Printf("Sending %s %s %v\n", topic, action, data)

	// Construct the header of the message
	msg := blobCache.Get().([]byte)
	defer blobCache.Put(msg[:0])

	msg = append(msg, topicLabels[topic]...)
	msg = append(msg, messagePartSeparator)
	msg = append(msg, actionLabels[action]...)

	// If data content is specified, encode that too
	for _, item := range data {
		msg = append(msg, messagePartSeparator)

		switch item := item.(type) {
		case string:
			msg = append(msg, []byte(item)...)
		case []byte:
			msg = append(msg, item...)
		case typed:
			// A typed data item needs a proper type prefix prepended
			switch item := item.Content.(type) {
			case string:
				msg = append(msg, append([]byte("S"), []byte(item)...)...)
			case []byte:
				msg = append(msg, append([]byte("S"), item...)...)
			default:
				return fmt.Errorf("unknown typed data type: %t", item)
			}
		default:
			return fmt.Errorf("unknown untyped data type: %t", item)
		}
	}
	// Terminate the message, send and return to cache
	msg = append(msg, messageSeparator)
	_, err := c.conn.Write(msg)
	return err
}

// decode retrieves the next message from the underlying network connection and
// returns the parsed components.
func (c *Client) decode() (topic, action, [][]byte, error) {
	// If we have messages buffered up, deliver from those
	if len(c.msgBuf) > 0 {
		msg := c.msgBuf[0]
		c.msgBuf = c.msgBuf[1:]
		return msg.topic, msg.action, msg.data, nil
	}
	// If an old memory buffer was locked due to queued messages, return it now
	if c.oldBuf != nil {
		blobCache.Put(c.oldBuf)
		c.oldBuf = nil
	}
	// Retrieve the message from the server
	var raw []byte
	if len(c.readBuf) != 0 {
		// We had some partial messag leftovers, reuse that as our initial data
		raw = append(raw, c.readBuf...)
	}
	c.readBuf = c.readBuf[:cap(c.readBuf)]

	for {
		// Fetch the next message from the deepstream server
		n, err := c.conn.Read(c.readBuf)
		if err != nil {
			return 0, 0, nil, err
		}
		// If the buffer was filled and incomplete, expand it and read remainder
		if n == cap(c.readBuf) && c.readBuf[len(c.readBuf)-1] != messageSeparator {
			// Copy the contents of the buffer to an accumulator
			if raw == nil {
				raw = c.readBuf
			} else {
				raw = append(raw, c.readBuf...)
			}
			// Expand the buffer to cover larger message chunks
			c.readBuf = append(c.readBuf, c.readBuf...)
			continue
		}
		// Cut off the unused suffix and move back any previously accumulated data
		c.readBuf = c.readBuf[:n]
		if raw != nil {
			raw = append(raw, c.readBuf...)
		} else {
			raw = c.readBuf
		}
		break
	}
	// Split the packet up into messages and leave partial leftovers in the buffer
	msgs := bytes.Split(raw, []byte{messageSeparator})

	if last := msgs[len(msgs)-1]; len(last) != 0 {
		// Last message is incomplete, retain as a leftover in a new buffer
		c.oldBuf = c.readBuf[:0]
		c.readBuf = append(blobCache.Get().([]byte), last...)
	} else {
		// All messages complete, reuse same read buffer
		c.readBuf = c.readBuf[:0]
	}
	msgs = msgs[:len(msgs)-1]

	// If we received a batch of messages, buffer up for individual delivery
	if len(msgs) > 1 {
		c.msgBuf = make([]message, 0, len(msgs))
	}
	for _, msg := range msgs {
		// Split the message up into it's components and return
		parts := bytes.Split(msg, []byte{messagePartSeparator})
		if len(parts) < 2 {
			return 0, 0, nil, fmt.Errorf("not enough message parts: count=%d", len(parts))
		}
		topic, ok := topicMapping[string(parts[0])]
		if !ok {
			return 0, 0, nil, fmt.Errorf("unknown topic: %s", parts[0])
		}
		action, ok := actionMapping[string(parts[1])]
		if !ok {
			return 0, 0, nil, fmt.Errorf("unknown action: %s", parts[1])
		}
		//fmt.Printf("Received %s %s %s\n", topic, action, parts[2:])

		// If we have a single message return, otherwise buffer
		if len(msgs) == 1 {
			return topic, action, parts[2:], nil
		}
		c.msgBuf = append(c.msgBuf, message{topic: topic, action: action, data: parts[2:]})
	}
	// We buffered up message, recurse to exhaust
	return c.decode()
}
