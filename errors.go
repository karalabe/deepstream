// Copyright 2016 Péter Szilágyi. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Contains the various error types and constants.

package deepstream

import "errors"

// ErrClosed is returned when an operation tries to access the server but the
// network connection was already torn down.
var ErrClosed = errors.New("connection closed")

// ErrPendingLogin is returned when a client attempts to log in while a concurrent
// login attempt is already underway.
var ErrPendingLogin = errors.New("login already pending")

// ErrNilChannel is returned by a subscription attempt if the provided receiving
// channel into which arriving messages are to be placed is nil.
var ErrNilChannel = errors.New("nil recipient channel")

// ErrNotChannel is returned by a subscription attempt if the provided receiving
// data structure is not an actual channel.
var ErrNotChannel = errors.New("sink not channel")

// ErrAlreadySubscribed is returned by a subscription attempt if the requested
// topic or regexp is already subscribed to (or pending subscription).
var ErrAlreadySubscribed = errors.New("pending or already subscribed")

// ErrNotSubscribed is returned by an unsubscription attempt if the requested
// topic or regexp is not yet subscribed to (or pending unsubscription).
var ErrNotSubscribed = errors.New("not subscribed or pending unsubscription")
