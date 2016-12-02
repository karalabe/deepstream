// Copyright 2016 Péter Szilágyi. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package deepstream

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

// Tests that topic subscriptions, event publishing and topic unsubscription
// work in single threaded, multi threaded and massively concurrent scenarios.
func TestEventPubSub1Clients1Topics(t *testing.T)     { testEventPubSub(t, 1, 1) }
func TestEventPubSub10Clients1Topics(t *testing.T)    { testEventPubSub(t, 10, 1) }
func TestEventPubSub1Clients10Topics(t *testing.T)    { testEventPubSub(t, 1, 10) }
func TestEventPubSub10Clients10Topics(t *testing.T)   { testEventPubSub(t, 10, 10) }
func TestEventPubSub25Clients25Topics(t *testing.T)   { testEventPubSub(t, 25, 25) }
func TestEventPubSub100Clients100Topics(t *testing.T) { testEventPubSub(t, 100, 100) }

func testEventPubSub(t *testing.T, clientCount int, topicCount int) {
	// Launch a test server and connect a number of clients to it
	server := newTestServer(t, map[string]string{"config.yml": ""})
	defer server.close()

	clients := make([]*Client, clientCount)

	var pend sync.WaitGroup
	pend.Add(len(clients))

	for i := 0; i < len(clients); i++ {
		go func(idx int) {
			defer pend.Done()

			client, err := Dial(server.url)
			if err != nil {
				t.Fatalf("client %d: failed to connect: %v", idx, err)
			}
			if err := client.Login(nil); err != nil {
				t.Errorf("client %d: failed to login: %v", idx, err)
			}
			clients[idx] = client
		}(i)
	}
	pend.Wait()

	// Concurrently subscribe with all clients to a number of event topics
	topics := make([]string, topicCount)
	for i := 0; i < len(topics); i++ {
		topics[i] = fmt.Sprintf("topic#%d", i)
	}
	subs := make([][]chan string, len(clients))
	for i := 0; i < len(clients); i++ {
		subs[i] = make([]chan string, len(topics))
		for j := 0; j < len(subs[i]); j++ {
			subs[i][j] = make(chan string, len(clients)-1)
		}
	}
	pend.Add(len(clients) * len(topics))
	for i := 0; i < len(clients); i++ {
		for j := 0; j < len(subs[i]); j++ {
			go func(cidx int, tidx int) {
				defer pend.Done()
				if err := clients[cidx].Subscribe(fmt.Sprintf("topic#%d", tidx), subs[cidx][tidx]); err != nil {
					t.Errorf("client %d, topic %d: failed to subscribe: %v", cidx, tidx, err)
				}
			}(i, j)
		}
	}
	pend.Wait()

	// Concurrently inject events with all clients to all channels
	pend.Add(len(clients))
	for i := 0; i < len(clients); i++ {
		go func(idx int) {
			defer pend.Done()

			for j := 0; j < len(topics); j++ {
				if err := clients[idx].Publish(fmt.Sprintf("topic#%d", j), fmt.Sprintf("client#%d", idx)); err != nil {
					t.Errorf("client %d, topic %d: failed to publish: %v", idx, j, err)
				}
			}
		}(i)
	}
	pend.Wait()

	// Concurrently wait for all events to arrive and verify them individually too
	done := make(map[int]map[int]map[string]struct{})
	for i := 0; i < len(clients); i++ {
		done[i] = make(map[int]map[string]struct{})
		for j := 0; j < len(topics); j++ {
			done[i][j] = make(map[string]struct{})
		}
	}

	pend.Add(len(clients) * len(topics))
	for i := 0; i < len(clients); i++ {
		for j := 0; j < len(topics); j++ {
			go func(c int, t int) {
				defer pend.Done()
				for k := 0; k < len(clients)-1; k++ {
					done[c][t][<-subs[c][t]] = struct{}{}
				}
			}(i, j)
		}
	}
	pend.Wait()

	for i := 0; i < len(clients); i++ {
		for j := 0; j < len(topics); j++ {
			for k := 0; k < len(clients); k++ {
				if i != k { // Self events aren't delivered
					if _, ok := done[i][j][fmt.Sprintf("client#%d", k)]; !ok {
						t.Errorf("client %d, topic %d: missing event from client %d", i, j, k)
					}
				}
			}
		}
	}
	// Concurrently unsubscribe with all clients from all topics and ensure all
	// previous subscription channels have been closed
	pend.Add(len(clients) * len(topics))
	for i := 0; i < len(clients); i++ {
		for j := 0; j < len(subs[i]); j++ {
			go func(cidx int, tidx int) {
				defer pend.Done()

				if err := clients[cidx].Unsubscribe(fmt.Sprintf("topic#%d", tidx)); err != nil {
					t.Errorf("client %d, topic %d: failed to unsubscribe: %v", cidx, tidx, err)
				}
			}(i, j)
		}
	}
	pend.Wait()

	for i := 0; i < len(clients); i++ {
		for j := 0; j < len(topics); j++ {
			select {
			case data, ok := <-subs[i][j]:
				if ok {
					t.Errorf("client %d, topic %d: event arrived after unsubscribe: %v", i, j, data)
				}
			default:
				t.Errorf("client %d, topic %d: subscription channel open after unsubscribe", i, j)
			}
		}
	}
}

// Benchmarks the pubsub throughput between two clients.
func BenchmarkEventPubSubLatency1B(b *testing.B)    { benchmarkEventPubSubLatency(b, 1) }
func BenchmarkEventPubSubLatency1KB(b *testing.B)   { benchmarkEventPubSubLatency(b, 1024) }
func BenchmarkEventPubSubLatency8KB(b *testing.B)   { benchmarkEventPubSubLatency(b, 8192) }
func BenchmarkEventPubSubLatency64KB(b *testing.B)  { benchmarkEventPubSubLatency(b, 64*1024) }
func BenchmarkEventPubSubLatency256KB(b *testing.B) { benchmarkEventPubSubLatency(b, 256*1024) }
func BenchmarkEventPubSubLatency1MB(b *testing.B)   { benchmarkEventPubSubLatency(b, 1024*1024) }

func benchmarkEventPubSubLatency(b *testing.B, chunk int) {
	// Launch a test server and connect a sender and a recipent to it
	server := newBenchServer(b, map[string]string{"config.yml": "logger:\n  name: default\n  options:\n    logLevel: OFF\n"})
	defer server.close()

	sender, err := Dial(server.url)
	if err != nil {
		b.Fatalf("sender failed to connect: %v", err)
	}
	defer sender.Close()

	if err = sender.Login(nil); err != nil {
		b.Fatalf("sender failed to log in: %v", err)
	}

	recipient, err := Dial(server.url)
	if err != nil {
		b.Fatalf("recipient failed to connect: %v", err)
	}
	defer recipient.Close()

	if err = recipient.Login(nil); err != nil {
		b.Fatalf("recipient failed to log in: %v", err)
	}
	// Subscribe with the recipient to the benchmark topic
	ch := make(chan string, b.N)
	if err := recipient.Subscribe("b", ch); err != nil {
		b.Fatalf("recipient failed to subscribe: %v", err)
	}
	// Create a random chunk of data to pass around
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	blob := make([]byte, chunk)
	for i := range blob {
		blob[i] = letters[rand.Intn(len(letters))]
	}
	data := string(blob)

	// Reset the timer and start pushing and reading messages
	b.ResetTimer()
	b.SetBytes(int64(chunk))

	for i := 0; i < b.N; i++ {
		sender.Publish("b", data)
		<-ch
	}
	b.StopTimer()
}

// Benchmarks the pubsub throughput between two clients.
func BenchmarkEventPubSubThroughput1B(b *testing.B)    { benchmarkEventPubSubThroughput(b, 1) }
func BenchmarkEventPubSubThroughput1KB(b *testing.B)   { benchmarkEventPubSubThroughput(b, 1024) }
func BenchmarkEventPubSubThroughput8KB(b *testing.B)   { benchmarkEventPubSubThroughput(b, 8192) }
func BenchmarkEventPubSubThroughput64KB(b *testing.B)  { benchmarkEventPubSubThroughput(b, 64*1024) }
func BenchmarkEventPubSubThroughput256KB(b *testing.B) { benchmarkEventPubSubThroughput(b, 256*1024) }
func BenchmarkEventPubSubThroughput1MB(b *testing.B)   { benchmarkEventPubSubThroughput(b, 1024*1024) }

func benchmarkEventPubSubThroughput(b *testing.B, chunk int) {
	// Launch a test server and connect a sender and a recipent to it
	server := newBenchServer(b, map[string]string{"config.yml": "logger:\n  name: default\n  options:\n    logLevel: OFF\n"})
	defer server.close()

	sender, err := Dial(server.url)
	if err != nil {
		b.Fatalf("sender failed to connect: %v", err)
	}
	defer sender.Close()

	if err = sender.Login(nil); err != nil {
		b.Fatalf("sender failed to log in: %v", err)
	}

	recipient, err := Dial(server.url)
	if err != nil {
		b.Fatalf("recipient failed to connect: %v", err)
	}
	defer recipient.Close()

	if err = recipient.Login(nil); err != nil {
		b.Fatalf("recipient failed to log in: %v", err)
	}
	// Subscribe with the recipient to the benchmark topic
	ch := make(chan string, b.N)
	if err := recipient.Subscribe("b", ch); err != nil {
		b.Fatalf("recipient failed to subscribe: %v", err)
	}
	// Create a random chunk of data to pass around
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	blob := make([]byte, chunk)
	for i := range blob {
		blob[i] = letters[rand.Intn(len(letters))]
	}
	data := string(blob)

	// Reset the timer and start pushing and reading messages
	b.ResetTimer()
	b.SetBytes(int64(chunk))

	threads := runtime.NumCPU()
	for t := 0; t < threads; t++ {
		go func(idx int) {
			iters := b.N / threads
			if idx == threads-1 {
				iters = (b.N - iters*(threads-1))
			}
			for i := 0; i < iters; i++ {
				sender.Publish("b", data)
			}
		}(t)
	}
	for i := 0; i < b.N; i++ {
		<-ch
	}
	b.StopTimer()
}
