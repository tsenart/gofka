package message_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	. "github.com/tsenart/gofka/message"
	. "github.com/tsenart/gofka/testing"
)

func TestMessageSet_NewMessageSet(t *testing.T) {
	msg := NewMessage([]byte("k"), []byte("v"), NoCodec)

	_, err := NewMessageSet(0, nil)
	Equals(t, err, ErrNilMessages)

	_, err = NewMessageSet(0, msg, msg, nil)
	Equals(t, err, ErrNilMessages)

	_, err = NewMessageSet(0, []Message{}...)
	Equals(t, err, ErrNoMessages)

	_, err = NewMessageSet(0,
		NewMessage([]byte("k"), []byte("v"), NoCodec),
		NewMessage([]byte("k"), []byte("v"), GZIPCodec),
	)
	Equals(t, err, ErrMultipleCodecs)

	_, err = NewMessageSet(0, NewMessage([]byte("k"), []byte("v"), NoCodec))
	OK(t, err)

	payload := make([]byte, 1024*1024)
	_, err = rand.Read(payload)
	OK(t, err)

	plain, err := NewMessageSet(0, NewMessage(payload, payload, NoCodec))
	OK(t, err)

	for _, codec := range []Codec{GZIPCodec} {
		comp, err := NewMessageSet(0, NewMessage(payload, payload, codec))
		OK(t, err)
		msg := fmt.Sprintf("compressed = %d bytes, uncompressed = %d bytes", comp.Size(), plain.Size())
		t.Log(msg)
		Assert(t, comp.Size() < plain.Size(), msg)
	}
}

func BenchmarkMessageSet_NewMessageSetWithNoCodec(b *testing.B) {
	benchmarkMessageSet_NewMessageSet(b, NoCodec)
}

func BenchmarkMessageSet_NewMessageSetWithGZIPCodec(b *testing.B) {
	benchmarkMessageSet_NewMessageSet(b, GZIPCodec)
}

func benchmarkMessageSet_NewMessageSet(b *testing.B, codec Codec) {
	key, value := make([]byte, 1024*1024), make([]byte, 1024*1024)
	_, err := rand.Read(key)
	OK(b, err)
	_, err = rand.Read(value)
	OK(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewMessageSet(0,
			NewMessage(key, value, codec),
			NewMessage(value, key, codec),
			NewMessage(key, value, codec),
			NewMessage(value, key, codec),
			NewMessage(key, value, codec),
			NewMessage(value, key, codec),
			NewMessage(key, value, codec),
			NewMessage(value, key, codec),
			NewMessage(key, value, codec),
			NewMessage(value, key, codec),
		)
	}
}
