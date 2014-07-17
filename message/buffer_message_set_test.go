package message_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	. "github.com/tsenart/gofka/message"
	. "github.com/tsenart/gofka/testing"
)

func TestBufferMessageSet_NewBufferMessageSet(t *testing.T) {
	msg := NewMessage([]byte("k"), []byte("v"), NoCodec)

	_, err := NewBufferMessageSet(0, nil)
	Equals(t, err, ErrNilMessages)

	_, err = NewBufferMessageSet(0, msg, msg, nil)
	Equals(t, err, ErrNilMessages)

	_, err = NewBufferMessageSet(0, []Message{}...)
	Equals(t, err, ErrNoMessages)

	_, err = NewBufferMessageSet(0,
		NewMessage([]byte("k"), []byte("v"), NoCodec),
		NewMessage([]byte("k"), []byte("v"), GZIPCodec),
	)
	Equals(t, err, ErrMultipleCodecs)

	_, err = NewBufferMessageSet(0, NewMessage([]byte("k"), []byte("v"), NoCodec))
	OK(t, err)

}

func TestBufferMessageSet_Codecs(t *testing.T) {
	payload := make([]byte, 1024*1024)
	_, err := rand.Read(payload)
	OK(t, err)

	plain, err := NewBufferMessageSet(0, NewMessage(payload, payload, NoCodec))
	OK(t, err)

	for _, codec := range []Codec{GZIPCodec} {
		comp, err := NewBufferMessageSet(0, NewMessage(payload, payload, codec))
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

func BenchmarkNewBufferMessageSetWithNoCodec(b *testing.B) {
	benchmarkNewBufferMessageSet(b, NoCodec)
}

func BenchmarkNewBufferMessageSetWithGZIPCodec(b *testing.B) {
	benchmarkNewBufferMessageSet(b, GZIPCodec)
}

func benchmarkNewBufferMessageSet(b *testing.B, codec Codec) {
	key, value := make([]byte, 1024*1024), make([]byte, 1024*1024)
	_, err := rand.Read(key)
	OK(b, err)
	_, err = rand.Read(value)
	OK(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewBufferMessageSet(0,
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
