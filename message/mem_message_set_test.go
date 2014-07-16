package message_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	. "github.com/tsenart/gofka/message"
	. "github.com/tsenart/gofka/testing"
)

func TestMemMessageSet_NewMemMessageSet(t *testing.T) {
	msg := NewMessage([]byte("k"), []byte("v"), NoCodec)

	_, err := NewMemMessageSet(0, nil)
	Equals(t, err, ErrNilMessages)

	_, err = NewMemMessageSet(0, msg, msg, nil)
	Equals(t, err, ErrNilMessages)

	_, err = NewMemMessageSet(0, []Message{}...)
	Equals(t, err, ErrNoMessages)

	_, err = NewMemMessageSet(0,
		NewMessage([]byte("k"), []byte("v"), NoCodec),
		NewMessage([]byte("k"), []byte("v"), GZIPCodec),
	)
	Equals(t, err, ErrMultipleCodecs)

	_, err = NewMemMessageSet(0, NewMessage([]byte("k"), []byte("v"), NoCodec))
	OK(t, err)

}

func TestMemMessageSet_Set(t *testing.T) { t.Skip("TODO") }

func TestMemMessageSet_Get(t *testing.T) {
	want := NewMessage([]byte("key"), []byte("value"), NoCodec)
	ms, err := NewMemMessageSet(100, NewMessage(nil, nil, NoCodec), want)
	OK(t, err)

	msg, last := ms.Get(99)
	Equals(t, uint32(0), last)
	Equals(t, Message(nil), msg)

	msg, last = ms.Get(101)
	Equals(t, ms.Size()-want.Size()-MsgOverhead, last)
	Equals(t, want, msg)
}

func TestMemMessageSet_Compress(t *testing.T) { t.Skip("TODO") }

func TestMemMessageSet_Codecs(t *testing.T) {
	payload := make([]byte, 1024*1024)
	_, err := rand.Read(payload)
	OK(t, err)

	plain, err := NewMemMessageSet(0, NewMessage(payload, payload, NoCodec))
	OK(t, err)

	for _, codec := range []Codec{GZIPCodec} {
		comp, err := NewMemMessageSet(0, NewMessage(payload, payload, codec))
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

func BenchmarkMemMessageSet_NewMemMessageSetWithNoCodec(b *testing.B) {
	benchmarkMemMessageSet_NewMemMessageSet(b, NoCodec)
}

func BenchmarkMemMessageSet_NewMemMessageSetWithGZIPCodec(b *testing.B) {
	benchmarkMemMessageSet_NewMemMessageSet(b, GZIPCodec)
}

func benchmarkMemMessageSet_NewMemMessageSet(b *testing.B, codec Codec) {
	key, value := make([]byte, 1024*1024), make([]byte, 1024*1024)
	_, err := rand.Read(key)
	OK(b, err)
	_, err = rand.Read(value)
	OK(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewMemMessageSet(0,
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
