// Copyright 2016 Péter Szilágyi. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package deepstream

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	docker "github.com/fsouza/go-dockerclient"
)

// server is a deepstream test server backed by a docker container.
type server struct {
	url       string
	daemon    *docker.Client
	container *docker.Container
	waiter    docker.CloseWaiter
	workdir   string
}

// newTestServer creates a new deepstream test server backed by a docker container.
func newTestServer(t *testing.T, configs map[string]string) *server {
	server, err := newServer(configs, false)
	if err != nil {
		t.Fatalf("failed to create test server: %v", err)
	}
	return server
}

// newBenchServer creates a new deepstream benchmark server backed by a docker container.
func newBenchServer(b *testing.B, configs map[string]string) *server {
	server, err := newServer(configs, false)
	if err != nil {
		b.Fatalf("failed to create benchmark server: %v", err)
	}
	return server
}

// newServer creates a new deepstream server backed by a docker container.
func newServer(configs map[string]string, attach bool) (*server, error) {
	var (
		server = &server{
			url: "ws://localhost:6020/deepstream",
		}
		err error
	)
	// If we're using a live deepstream server, don't bother with docker
	if url := os.Getenv("DEEPSTREAM_SERVER_URL"); url != "" {
		server.url = url
		return server, nil
	}
	// Connect to docker and ensure the deepstream image is downloaded
	if server.daemon, err = docker.NewClient("unix:///var/run/docker.sock"); err != nil {
		return nil, fmt.Errorf("failed to connect to docker daemon: %v", err)
	}
	// Create the deepstream config folder and contents
	if server.workdir, err = ioutil.TempDir("", ""); err != nil {
		return nil, fmt.Errorf("failed to create temporary workspace: %v", err)
	}
	for name, content := range configs {
		if err = ioutil.WriteFile(filepath.Join(server.workdir, name), []byte(content), os.ModePerm); err != nil {
			return nil, fmt.Errorf("failed to create config file: %v", err)
		}
	}
	// Create the deepstream container, forward the ports and clean up afterwards
	if server.container, err = server.daemon.CreateContainer(docker.CreateContainerOptions{
		Config: &docker.Config{
			Image: "deepstreamio/deepstream.io",
		},
		HostConfig: &docker.HostConfig{
			PortBindings: map[docker.Port][]docker.PortBinding{
				"6020/tcp": {docker.PortBinding{HostIP: "127.0.0.1", HostPort: "6020"}},
				"6021/tcp": {docker.PortBinding{HostIP: "127.0.0.1", HostPort: "6021"}},
			},
			AutoRemove: true,
			Binds: []string{
				fmt.Sprintf("%s:/etc/deepstream:ro", server.workdir),
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("failed to create deepstream container: %v", err)
	}
	// Attach to the container and start it up
	if server.waiter, err = server.daemon.AttachToContainerNonBlocking(docker.AttachToContainerOptions{
		Container:    server.container.ID,
		OutputStream: os.Stdout,
		ErrorStream:  os.Stderr,
		Stream:       attach,
		Stdout:       attach,
		Stderr:       attach,
	}); err != nil {
		return nil, fmt.Errorf("failed to attach to deepstream container: %v", err)
	}
	if err := server.daemon.StartContainer(server.container.ID, nil); err != nil {
		return nil, fmt.Errorf("failed to start deepstream container: %v", err)
	}
	// Wait for the listener port to open and return
	for {
		// If the container died, bail out
		container, err := server.daemon.InspectContainer(server.container.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect deepstream container: %v", err)
		}
		if !container.State.Running {
			return nil, fmt.Errorf("unexpectedly terminated deepstream container")
		}
		// Container seems to be alive, check whether the listener socket is accepting connections
		if conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", container.NetworkSettings.IPAddress, 6020)); err == nil {
			conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return server, nil
}

// close terminates the deepstream container.
func (s *server) close() {
	// Skip closing if there's no backing docker container
	if s.waiter == nil {
		return
	}
	// Otherwise detach console, stop container and remove working directory
	s.waiter.Close()
	s.daemon.StopContainer(s.container.ID, 0)
	os.RemoveAll(s.workdir)
}

// Tests that logins work for various configuration and credential combinations.
func TestAuths(t *testing.T) {
	tests := []struct {
		configs map[string]string
		auth    Authenticator
		fail    bool
	}{
		// Default or disabled auth allows anyone to login, missing auth token result in an empty user/pass combo
		{configs: map[string]string{
			"config.yml": "",
		}, auth: nil, fail: false},
		{configs: map[string]string{
			"config.yml": "auth:\n  type: none\n",
		}, auth: nil, fail: false},

		// Default allows anyone to login, but a malformed request should still fail
		// {configs: nil, auth: &JSONAuth{Credentials: nil}, fail: false}, // Crashes deepstream

		// Default or disabled auth allows anyone to login, so an empty user/pass should succeed
		{configs: map[string]string{
			"config.yml": "",
		}, auth: &PasswordAuth{}, fail: false},
		{configs: map[string]string{
			"config.yml": "auth:\n  type: none\n",
		}, auth: &PasswordAuth{}, fail: false},

		// File based authentication requires valid password
		{configs: map[string]string{
			"config.yml": "auth:\n  type: file\n  options:\n    path: /etc/deepstream/users.yml\n",
			"users.yml":  "johndoe:\n  password: uY2zMQZXcFuWKeX/6eY43w==9wSp046KHAQfbvKcKgvwNA==\n",
		}, auth: &PasswordAuth{Username: "johndoe", Password: ""}, fail: true},
		{configs: map[string]string{
			"config.yml": "auth:\n  type: file\n  options:\n    path: /etc/deepstream/users.yml\n",
			"users.yml":  "johndoe:\n  password: uY2zMQZXcFuWKeX/6eY43w==9wSp046KHAQfbvKcKgvwNA==\n",
		}, auth: &PasswordAuth{Username: "johndoe", Password: "uY2zMQZXcFuWKeX/6eY43w==9wSp046KHAQfbvKcKgvwNA=="}, fail: false},
	}
	for i, tt := range tests {
		server := newTestServer(t, tt.configs)

		client, err := Dial(server.url)
		if err != nil {
			server.close()
			t.Fatalf("test %d: failed to connect: %v", i, err)
		}
		if err := client.Login(tt.auth); (err != nil) != tt.fail {
			t.Errorf("test %d: login failure mismatch: have %v, want %v", i, err, tt.fail)
		}
		client.Close()
		server.close()
	}
}
