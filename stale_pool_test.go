package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// All initial sockets are warmed and then closed by the server while idle.
// A bounded retry must reconnect, even though every other hot socket is stale.
func TestStatReconnectsEntirelyStalePool(t *testing.T) {
	for _, ln := range []lane{laneNormal, lanePriority, laneBackground} {
		for _, prewarm := range []bool{false, true} {
			t.Run(fmt.Sprintf("lane=%d/prewarm=%v", ln, prewarm), func(t *testing.T) {
				const slots = 16
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var dials atomic.Int32
				var authentications atomic.Int32
				var mu sync.Mutex
				var servers []net.Conn
				ready := make(chan struct{})
				factory := func(ctx context.Context) (net.Conn, error) {
					listener, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						return nil, err
					}
					defer listener.Close()
					client, err := net.Dial("tcp", listener.Addr().String())
					if err != nil {
						return nil, err
					}
					server, err := listener.Accept()
					if err != nil {
						client.Close()
						return nil, err
					}
					n := dials.Add(1)
					mu.Lock()
					servers = append(servers, server)
					mu.Unlock()
					go func() {
						defer server.Close()
						fmt.Fprint(server, "200 ready\r\n")
						scanner := bufio.NewScanner(server)
						authenticated := false
						for scanner.Scan() {
							switch scanner.Text() {
							case "AUTHINFO USER testuser":
								fmt.Fprint(server, "381 password required\r\n")
								continue
							case "AUTHINFO PASS testpass":
								authenticated = true
								if authentications.Add(1) == slots {
									close(ready)
								}
								fmt.Fprint(server, "281 authenticated\r\n")
								continue
							}
							if !authenticated {
								return
							}
							if !strings.HasPrefix(scanner.Text(), "STAT ") {
								return
							}
							if n <= slots {
								select {
								case <-ready:
								case <-ctx.Done():
									return
								}
								fmt.Fprint(server, "223 1 <test@invalid> exists\r\n")
							} else {
								fmt.Fprint(server, "430 no such article\r\n")
							}
						}
					}()
					return client, nil
				}
				min := 0
				if prewarm {
					min = slots
				}
				c, err := NewClient(ctx, []Provider{{Name: "test", Factory: factory, Auth: Auth{Username: "testuser", Password: "testpass"}, Connections: slots, MinConnections: min, StatInflight: 1, SkipPing: true, AttemptTimeout: time.Second}})
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				ids := make([]string, slots)
				for i := range ids {
					ids[i] = "test@invalid"
				}
				warm := 0
				for r := range c.StatMany(ctx, ids, StatManyOptions{Provider: "test", Concurrency: slots}) {
					if r.Err != nil {
						t.Fatalf("warmup: %v", r.Err)
					}
					warm++
				}
				if warm != slots || dials.Load() != slots {
					t.Fatalf("warm=%d dials=%d, want %d", warm, dials.Load(), slots)
				}
				mu.Lock()
				for _, server := range servers {
					server.Close()
				}
				mu.Unlock()
				g := (*c.mainGroups.Load())[0]
				result := c.statOne(ctx, "missing@invalid", g, nil, ln)
				if !errors.Is(result.Err, ErrArticleNotFound) {
					t.Fatalf("confirmation: %v, want definitive ErrArticleNotFound; dials=%d", result.Err, dials.Load())
				}
				if got := dials.Load(); got <= slots {
					t.Fatalf("dials=%d: retry did not establish a fresh connection", got)
				}
				if authentications.Load() != dials.Load() {
					t.Fatalf("fresh connections must authenticate: dials=%d authenticated=%d", dials.Load(), authentications.Load())
				}
			})
		}
	}
}

func TestStreamEOFAfterDeliveryDoesNotRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("Q"), 64*1024)
	encoded := yencSinglePart(payload, "partial.bin")
	var dials atomic.Int32
	factory := func(context.Context) (net.Conn, error) {
		dials.Add(1)
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			fmt.Fprint(server, "200 ready\r\n")
			bufio.NewReader(server).ReadString('\n')
			server.Write(encoded[:len(encoded)/2])
		}()
		return client, nil
	}
	c, err := NewClient(ctx, []Provider{{Factory: factory, Connections: 2, SkipPing: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var sink bytes.Buffer
	_, err = c.BodyStream(ctx, "test@invalid", &sink)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("error=%v, want EOF", err)
	}
	if sink.Len() == 0 || sink.Len() >= len(payload) {
		t.Fatalf("delivered=%d, want one partial body", sink.Len())
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials=%d, must not replay a partially streamed body", got)
	}
}

func TestPipelinedStatReconnectsAfterEOF(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const count = 16
	var dials atomic.Int32
	factory := func(context.Context) (net.Conn, error) {
		n := dials.Add(1)
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			fmt.Fprint(server, "200 ready\r\n")
			scanner := bufio.NewScanner(server)
			if n == 1 {
				for i := 0; i < count; i++ {
					if !scanner.Scan() {
						return
					}
				}
				return // all requests were pipelined; close without any replies
			}
			for scanner.Scan() {
				fmt.Fprint(server, "223 1 <test@invalid> exists\r\n")
			}
		}()
		return client, nil
	}
	c, err := NewClient(ctx, []Provider{{Name: "test", Factory: factory, Connections: 1, StatInflight: count, SkipPing: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = "test@invalid"
	}
	done := 0
	for result := range c.StatMany(ctx, ids, StatManyOptions{Provider: "test", Concurrency: count, Background: true}) {
		if result.Err != nil {
			t.Fatalf("queued request did not recover: %v", result.Err)
		}
		done++
	}
	if done != count {
		t.Fatalf("completed=%d, want %d", done, count)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("dials=%d, want one replacement for the failed socket", got)
	}
}

func TestConnectionEOFRetiresSocketAndDrainsPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const count = 8
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		fmt.Fprint(server, "200 ready\r\n")
		scanner := bufio.NewScanner(server)
		for range count {
			if !scanner.Scan() {
				return
			}
		}
	}()
	reqCh := make(chan *Request, count)
	c, err := newNNTPConnectionFromConn(ctx, client, count, reqCh, nil, Auth{}, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	go c.Run()
	defer c.Close()
	requests := make([]*Request, count)
	for i := range requests {
		requests[i] = &Request{Ctx: ctx, Payload: statPayload("test@invalid"), RespCh: make(chan Response, 1)}
		reqCh <- requests[i]
	}
	for _, req := range requests {
		select {
		case resp := <-req.RespCh:
			if !isConnectionDeathError(resp.Err) {
				t.Fatalf("pending response=%v, want transport death", resp.Err)
			}
			if resp.Request != req {
				t.Fatal("failed request lost its connection identity")
			}
		case <-ctx.Done():
			t.Fatal("pending request was stranded")
		}
	}
	select {
	case <-c.done:
	case <-time.After(time.Second):
		t.Fatal("EOF left the reader/writer and socket alive")
	}
}

func TestFreshConnectionRetriesRemainBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var dials atomic.Int32
	factory := func(context.Context) (net.Conn, error) {
		dials.Add(1)
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			fmt.Fprint(server, "200 ready\r\n")
			bufio.NewReader(server).ReadString('\n')
		}()
		return client, nil
	}
	c, err := NewClient(ctx, []Provider{{Name: "test", Factory: factory, Connections: 8, SkipPing: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for r := range c.StatMany(ctx, []string{"test@invalid"}, StatManyOptions{Provider: "test", Background: true}) {
		if !errors.Is(r.Err, io.EOF) {
			t.Fatalf("result=%v, want EOF after bounded fresh retries", r.Err)
		}
	}
	if got := dials.Load(); got != 1+maxConnDiedRetries {
		t.Fatalf("dials=%d, want %d bounded attempts", got, 1+maxConnDiedRetries)
	}
}
