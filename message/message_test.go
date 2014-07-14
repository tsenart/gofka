package message_test

import (
	"testing"

	. "github.com/tsenart/gofka/message"
	. "github.com/tsenart/gofka/testing"
)

func TestMessage_ChecksumHashValid(t *testing.T) {
	m := NewMessage([]byte("key"), []byte("value"), NoCodec)
	Equals(t, m.Checksum(), m.Hash())
	Assert(t, m.Valid(), "Valid: want: true, got: false")
	copy(m[CrcOffset:], []byte{0, 0, 0, 0})
	Assert(t, !m.Valid(), "Valid: want: false, got: true")
}
