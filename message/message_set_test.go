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

func TestMessageSet_Iterator(t *testing.T) {
	ms := makeMessageSet(t, 100, NoCodec, 50)
	for _, lv := range []Level{Header, Full} {
		for i, iter := 0, ms.Iterator(); i < 50; i++ {
			msg, err := iter.Next(lv)
			if err != nil {
				t.Fatal(err)
			} else if msg == nil && i != 49 {
				t.Fatalf("not enough Messages in MessageSet: want %d, got %d", 50, i)
			} else if want := uint64(100 + i); msg.Offset != want {
				t.Fatalf("bad offset for Message %d: want %d, got %d", i, want, msg.Offset)
			} else if want := uint32(i) * (MsgOverhead + MsgHeaderSize); want != msg.Pos {
				t.Fatalf("bad pos for Message %d: want %d, got %d", i, want, msg.Pos)
			} else if want := uint32(MsgHeaderSize); want != msg.MsgSize {
				t.Fatalf("bad size for Message %d: want %d, got %d", i, want, msg.MsgSize)
			} else if lv == Header && msg.Message != nil {
				t.Fatalf("expected Message to be nil with Header level. got %s", msg.Message)
			} else if lv == Full && msg.Equal(Message("")) {
				t.Fatalf("expected Message to not be nil with Full level. got %s", msg.Message)
			}
		}
	}
}

func TestMessageSet_Size(t *testing.T)    { t.Skip("TODO") }
func TestMessageSet_Equal(t *testing.T)   { t.Skip("TODO") }
func TestMessageSet_WriteTo(t *testing.T) { t.Skip("TODO") }
func TestMessageSet_String(t *testing.T)  { t.Skip("TODO") }

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

func makeMessageSet(tb testing.TB, offset uint64, codec Codec, items int) *MessageSet {
	msgs := make([]Message, 0, items)
	for i := 0; i < items; i++ {
		msgs = append(msgs, NewMessage([]byte(""), []byte("")))
	}
	ms, err := NewMessageSet(offset, codec, msgs...)
	if err != nil {
		tb.Fatal(err)
	}
	return ms
}
