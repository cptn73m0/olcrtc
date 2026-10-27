package server

import (
	"context"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/handshake"
)

// TestAcceptHandshakeSkipsDataStreams reproduces the production stall.
//
// After a session reinstall the peer keeps opening streams that carry
// application JSON for a few seconds; their first four bytes look like a huge
// frame header, so the handshake treats them as stale and burns its retry
// budget. The server then resets the transport and reinstalls again, which is
// what leaves the tunnel dead for 55-56 seconds on every rotation.
//
// The behaviour that matters to a user: a real handshake that arrives after a
// burst of data streams must still be accepted on the same smux session.
func TestAcceptHandshakeSkipsDataStreams(t *testing.T) {
	serverSess, clientSess, cleanup := smuxPair(t)
	defer cleanup()

	s := newHandshakeServer()
	s.ln = &serverLinkStub{}
	s.keys = newServerTestKeys(t)

	const dataStreams = 5
	payload := []byte(`{"addr":"example.com","port":443}`)

	go func() {
		for range dataStreams {
			stream, err := clientSess.OpenStream()
			if err != nil {
				return
			}
			_, _ = stream.Write(payload)
			_ = stream.Close()
		}
		stream, err := clientSess.OpenStream()
		if err != nil {
			return
		}
		_, _, _ = handshake.Client(stream, "device-C", nil)
	}()

	stream, res, ok := s.acceptHandshake(context.Background(), serverSess)
	if !ok {
		t.Fatal("acceptHandshake() rejected the session after data streams")
	}
	defer func() { _ = stream.Close() }()
	if res.deviceID != "device-C" {
		t.Fatalf("deviceID = %q, want device-C", res.deviceID)
	}
}
