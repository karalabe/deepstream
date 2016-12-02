// Copyright 2016 Péter Szilágyi. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package deepstream

const (
	// messageSeparator is the control character separating individual messages in
	// the textual wire protocol.
	messageSeparator = byte(30)

	// messagePartSeparator is the control character separating individual parts of
	// a single message in the textual wire protocol.
	messagePartSeparator = byte(31)
)

// connectionState represents the current state of a connection.
type connectionState int

const ()

type topic int

func (t topic) String() string {
	return string(topicLabels[t])
}

const (
	topicConnection topic = iota
	topicAuth
	topicError
	topicEvent
	topicRecord
	topicRPC
	topicPrivate
)

// topicLabels contains the textual representation of different topics in the
// underlying wire protocol.
var topicLabels = map[topic][]byte{
	topicConnection: []byte("C"),
	topicAuth:       []byte("A"),
	topicError:      []byte("X"),
	topicEvent:      []byte("E"),
	topicRecord:     []byte("R"),
	topicRPC:        []byte("P"),
	topicPrivate:    []byte("PRIVATE/"),
}

type action int

func (a action) String() string {
	return string(actionLabels[a])
}

const (
	actionAck action = iota
	actionRead
	actionRedirect
	actionChallenge
	actionChallengeResponse
	actionPing
	actionPong
	actionCreate
	actionUpdate
	actionPatch
	actionDelete
	actionSubscribe
	actionUnsubscribe
	actionHas
	actionSnapshot
	actionListen
	actionUnlisten
	actionListenAccept
	actionListenReject
	actionSubscriptionHasProvider
	actionProviderUpdate
	actionQuery
	actionCreateorread
	actionEvent
	actionError
	actionRequest
	actionResponse
	actionRejection
)

// actionLabels contains the textual representation of different actions in the
// underlying wire protocol.
var actionLabels = map[action][]byte{
	actionAck:                     []byte("A"),
	actionRead:                    []byte("R"),
	actionRedirect:                []byte("RED"),
	actionChallenge:               []byte("CH"),
	actionChallengeResponse:       []byte("CHR"),
	actionPing:                    []byte("PI"),
	actionPong:                    []byte("PO"),
	actionCreate:                  []byte("C"),
	actionUpdate:                  []byte("U"),
	actionPatch:                   []byte("P"),
	actionDelete:                  []byte("D"),
	actionSubscribe:               []byte("S"),
	actionUnsubscribe:             []byte("US"),
	actionHas:                     []byte("H"),
	actionSnapshot:                []byte("SN"),
	actionListen:                  []byte("L"),
	actionUnlisten:                []byte("UL"),
	actionListenAccept:            []byte("LA"),
	actionListenReject:            []byte("LR"),
	actionSubscriptionHasProvider: []byte("SH"),
	actionProviderUpdate:          []byte("PU"),
	actionQuery:                   []byte("Q"),
	actionCreateorread:            []byte("CR"),
	actionEvent:                   []byte("EVT"),
	actionError:                   []byte("E"),
	actionRequest:                 []byte("REQ"),
	actionResponse:                []byte("RES"),
	actionRejection:               []byte("REJ"),
}

type kind int

func (k kind) String() string {
	return string(kindLabels[k])
}

const (
	kindString kind = iota
	kindObject
	kindNumber
	kindNull
	kindTrue
	kindFalse
	kindUndefined
)

// actionLabels contains the textual representation of different kinds in the
// underlying wire protocol.
var kindLabels = map[kind][]byte{
	kindString:    []byte("S"),
	kindObject:    []byte("O"),
	kindNumber:    []byte("N"),
	kindNull:      []byte("L"),
	kindTrue:      []byte("T"),
	kindFalse:     []byte("F"),
	kindUndefined: []byte("U"),
}

var (
	topicMapping  = make(map[string]topic)
	actionMapping = make(map[string]action)
	kindMapping   = make(map[string]kind)
)

func init() {
	for topic, label := range topicLabels {
		topicMapping[string(label)] = topic
	}
	for action, label := range actionLabels {
		actionMapping[string(label)] = action
	}
	for kind, label := range kindLabels {
		kindMapping[string(label)] = kind
	}
}
