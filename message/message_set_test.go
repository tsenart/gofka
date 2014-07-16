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

}

func TestMessageSet_Set(t *testing.T) { t.Skip("TODO") }

func TestMessageSet_Get(t *testing.T) {
	want := NewMessage([]byte("key"), []byte("value"), NoCodec)
	ms, err := NewMessageSet(100, NewMessage(nil, nil, NoCodec), want)
	OK(t, err)

	msg, last := ms.Get(99)
	Equals(t, uint32(0), last)
	Equals(t, Message(nil), msg)

	msg, last = ms.Get(101)
	Equals(t, ms.Size()-want.Size()-MsgOverhead, last)
	Equals(t, want, msg)
}

func TestMessageSet_Compress(t *testing.T) { t.Skip("TODO") }

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
