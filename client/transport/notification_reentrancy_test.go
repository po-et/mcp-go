package transport_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
)

const notificationResponses = `{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":0}}
{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":1}}
{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":2}}
{"jsonrpc":"2.0","id":7,"result":{"ok":true}}
`

func notificationSSE() string {
	var body strings.Builder
	for line := range strings.SplitSeq(strings.TrimSpace(notificationResponses), "\n") {
		fmt.Fprintf(&body, "event: message\ndata: %s\n\n", line)
	}
	return body.String()
}

func TestStreamableHTTPNotificationHandlerCanReplaceItself(t *testing.T) {
	testNotificationHandlerChanges(t, func(t *testing.T) transport.Interface {
		httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, notificationSSE())
		}))
		t.Cleanup(httpServer.Close)
		tr, err := transport.NewStreamableHTTP(httpServer.URL)
		require.NoError(t, err)
		return tr
	})
}

func TestSSENotificationHandlerCanReplaceItself(t *testing.T) {
	testNotificationHandlerChanges(t, func(t *testing.T) transport.Interface {
		requested := make(chan struct{})
		httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				close(requested)
				w.WriteHeader(http.StatusAccepted)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "event: endpoint\ndata: /messages\n\n")
			w.(http.Flusher).Flush()
			select {
			case <-requested:
				_, _ = io.WriteString(w, notificationSSE())
				w.(http.Flusher).Flush()
			case <-r.Context().Done():
				return
			}
			<-r.Context().Done()
		}))
		t.Cleanup(httpServer.Close)
		tr, err := transport.NewSSE(httpServer.URL)
		require.NoError(t, err)
		return tr
	})
}

func TestStdioNotificationHandlerCanReplaceItself(t *testing.T) {
	testNotificationHandlerChanges(t, func(t *testing.T) transport.Interface {
		clientInput, serverOutput := io.Pipe()
		serverInput, clientOutput := io.Pipe()
		t.Cleanup(func() {
			_ = clientInput.Close()
			_ = serverInput.Close()
			_ = clientOutput.Close()
			_ = serverOutput.Close()
		})
		go func() {
			if _, err := bufio.NewReader(serverInput).ReadString('\n'); err == nil {
				_, _ = io.WriteString(serverOutput, notificationResponses)
			}
		}()
		return transport.NewIO(clientInput, clientOutput, nil)
	})
}

func testNotificationHandlerChanges(t *testing.T, newTransport func(*testing.T) transport.Interface) {
	t.Helper()
	for _, clear := range []bool{false, true} {
		name := "replace"
		if clear {
			name = "clear"
		}
		t.Run(name, func(t *testing.T) {
			tr := newTransport(t)
			defer tr.Close()

			var originalCalls, replacementCalls atomic.Int32
			returned := make(chan struct{})
			tr.SetNotificationHandler(func(mcp.JSONRPCNotification) {
				originalCalls.Add(1)
				if clear {
					tr.SetNotificationHandler(nil)
				} else {
					tr.SetNotificationHandler(func(mcp.JSONRPCNotification) {
						replacementCalls.Add(1)
					})
				}
				close(returned)
			})

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.NoError(t, tr.Start(ctx))
			result, err := tr.SendRequest(ctx, transport.JSONRPCRequest{
				JSONRPC: mcp.JSONRPC_VERSION,
				ID:      mcp.NewRequestId(int64(7)),
				Method:  "ping",
			})
			require.NoError(t, err, "replacing a handler must not block the following RPC response")
			require.NotNil(t, result)
			require.Nil(t, result.Error)
			require.Equal(t, int64(7), result.ID.Value())
			require.JSONEq(t, `{"ok":true}`, string(result.Result))
			select {
			case <-returned:
			default:
				t.Fatal("notification callback did not finish")
			}
			require.Equal(t, int32(1), originalCalls.Load())
			if clear {
				require.Zero(t, replacementCalls.Load())
			} else {
				require.Equal(t, int32(2), replacementCalls.Load())
			}
		})
	}
}
