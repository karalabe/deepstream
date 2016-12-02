// Copyright 2016 Péter Szilágyi. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package deepstream

import "encoding/json"

// Authenticator is a login credential generator that supports a single operation,
// namely returning the auth string to send to the server on login.
type Authenticator interface {
	// EncodeAuth generates a single string to send to the deepstream server upon
	// login as the credentials. It is up to the server and/or associated plugins
	// to make heads or tails of it.
	EncodeAuth() (string, error)
}

// PasswordAuth is the simplest built-in authentication method that sends a user
// and a password tuple to the server (encoded as a JSON object).
type PasswordAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// EncodeAuth implements Authenticator, generating an athentication string from
// the provided username and password tuple.
func (a *PasswordAuth) EncodeAuth() (string, error) {
	auth := &JSONAuth{Credentials: a}
	return auth.EncodeAuth()
}

// JSONAuth is a more generic authentication method where the user can supply and
// arbitrary object as the login credentials and that will be serailized into a
// JSON object and sent to the server for authentication.
type JSONAuth struct {
	Credentials interface{}
}

// EncodeAuth implements Authenticator, generating an athentication string from
// the provided aribtrary JSON object contained within.
func (a *JSONAuth) EncodeAuth() (string, error) {
	blob, err := json.Marshal(a.Credentials)
	return string(blob), err
}
