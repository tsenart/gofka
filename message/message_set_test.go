package message_test

import (
	"crypto/rand"
	"fmt"
	. "github.com/tsenart/gofka/message"
	. "github.com/tsenart/gofka/testing"
	"testing"
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

}

func TestMessageSet_Codecs(t *testing.T) {
	payload := make([]byte, 1024*1024)
	_, err := rand.Read(payload)
	OK(t, err)

	plain, err := NewMessageSet(0, NewMessage(payload, payload, NoCodec))
	OK(t, err)

	for _, codec := range []Codec{GZIPCodec} {
		comp, err := NewMessageSet(0, NewMessage(payload, payload, codec))
		OK(t, err)
		msg := fmt.Sprintf(
			"%s compressed = %d bytes, uncompressed = %d bytes",
			codec,
			comp.Size(),
			plain.Size(),
		)
		t.Log(msg)
		t.Log(plain)
		t.Log(comp)
		Assert(t, comp.Size() < plain.Size(), msg)
	}
}

func BenchmarkNewMessageSetWithNoCodec(b *testing.B) {
	benchmarkNewMessageSet(b, NoCodec)
}

func BenchmarkNewMessageSetWithGZIPCodec(b *testing.B) {
	benchmarkNewMessageSet(b, GZIPCodec)
}

func benchmarkNewMessageSet(b *testing.B, codec Codec) {
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
