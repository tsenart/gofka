package message_test

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	. "github.com/tsenart/gofka/message"
	. "github.com/tsenart/gofka/testing"
)

func TestFileMessageSet_Append(t *testing.T) {
	f := tempFile(t, "test-fms-append")
	defer f.Close()

	fms, err := NewFileMessageSet(f, 0, -1)
	OK(t, err)

	ms := makeMessageSet(t, 100, NoCodec, 5)
	OK(t, fms.Append(ms))
	Equals(t, fms.Size(), ms.Size())

	OK(t, fms.Append(ms))
	Equals(t, fms.Size(), ms.Size()*2)

	msgsOffs := make([]*MessageOffset, 5)
	msgs := make([]Message, len(msgsOffs))
	iter := fms.Iterator()
	for i := 0; i < cap(msgs); i++ {
		msg, err := iter.Next(Full)
		OK(t, err)
		msgsOffs[i] = msg
		msgs[i] = msg.Message
	}

	ms2, err := NewMessageSet(msgsOffs[0].Offset, NoCodec, msgs...)
	OK(t, err)
	Equals(t, ms, ms2)
}

func TestFileMessageSet_Iterator(t *testing.T) {
	f := tempFile(t, "test-fms-iterator")
	defer f.Close()

	fms, err := NewFileMessageSet(f, 0, -1)
	OK(t, err)

	ms := makeMessageSet(t, 100, NoCodec, 50)
	OK(t, fms.Append(ms))
	Equals(t, fms.Size(), ms.Size())

	for _, lv := range []Level{Header, Full} {
		for i, iter := 0, fms.Iterator(); i < 50; i++ {
			msg, err := iter.Next(lv)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			} else if err == io.EOF && msg == nil && i != 49 {
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

func tempFile(t *testing.T, prefix string) *os.File {
	filename := fmt.Sprintf("%s.%d.bin", prefix, rand.Int31())
	path := filepath.Join(os.TempDir(), filename)
	t.Logf("tempFile: %s", path)
	f, err := os.Create(path)
	OK(t, err)
	return f
}
