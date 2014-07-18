package message_test

import (
	"crypto/rand"
	"testing"

	. "github.com/tsenart/gofka/message"
	. "github.com/tsenart/gofka/testing"
)

func TestSizeOf(t *testing.T) {
	msg := NewMessage([]byte("k"), []byte("v"))
	Equals(t, SizeOf(msg, msg), uint32(28+28))
}

func TestMessageSet_NewMessageSet(t *testing.T) {
	_, err := NewMessageSet(0, NoCodec, []Message{}...)
	Equals(t, err, ErrNoMessages)

	_, err = NewMessageSet(0, NoCodec, NewMessage([]byte("k"), []byte("v")))
	OK(t, err)
}

func TestMessageSet_Iterator(t *testing.T) { t.Skip("TODO") }
func TestMessageSet_Size(t *testing.T)     { t.Skip("TODO") }
func TestMessageSet_Equal(t *testing.T)    { t.Skip("TODO") }
func TestMessageSet_WriteTo(t *testing.T)  { t.Skip("TODO") }
func TestMessageSet_String(t *testing.T)   { t.Skip("TODO") }

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
		NewMessageSet(0, codec,
			NewMessage(key, value),
			NewMessage(value, key),
			NewMessage(key, value),
			NewMessage(value, key),
			NewMessage(key, value),
			NewMessage(value, key),
			NewMessage(key, value),
			NewMessage(value, key),
			NewMessage(key, value),
			NewMessage(value, key),
		)
	}
}
