package message

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Errors returned by methods on a MessageSet.
var (
	ErrNoMessages = errors.New("no messages provided")
)

// MessageSet is an in-memory sequential Message container with a fixed
// serialization format.
//
// With no compression enabled, each Message is laid out sequentially
// and preceded by a header containing its offset and size.
//
//  -----------------------------------------------------------------
//  | offset | size | message | ... | offset+N | size N | message N |
//  -----------------------------------------------------------------
//
// With compression enabled, the previous byte slice is compressed and set as
// the value of a single Message within the MessageSet.
//
//  -----------------------------
//  | offset+N | size | message |
//  -----------------------------
//
type MessageSet struct{ buf []byte }

// NewMessageSet returns a MessageSet containing the provided Messages.
// The first offset is set to the provided one and increments from there for
// each Message. Codec will be used to compress the messages.
func NewMessageSet(offset uint64, codec Codec, msgs ...Message) (*MessageSet, error) {
	ms := &MessageSet{buf: make([]byte, SizeOf(msgs...))}

	if ms.Size() == 0 {
		return nil, ErrNoMessages
	}

	ms.set(offset, codec, msgs...)
	if err := ms.compress(offset, codec); err != nil {
		return nil, err
	}

	return ms, nil
}

// SizeOf computes the byte size of a MessageSet containing msgs Messages.
func SizeOf(msgs ...Message) (size uint32) {
	for i := range msgs {
		if msgs[i] == nil {
			continue
		}
		size += MsgOverhead + msgs[i].Size()
	}
	return
}

// Iterator implements the Iterator interface.
func (ms *MessageSet) Iterator() Iterator {
	var pos uint32
	return IteratorFunc(func(lv Level) (*MessageOffset, error) {
		if pos >= ms.Size()-MsgOverhead {
			return nil, io.EOF
		}

		msg := &MessageOffset{
			Pos:     pos,
			Offset:  binary.BigEndian.Uint64(ms.buf[pos : pos+msgOffsetSize]),
			MsgSize: binary.BigEndian.Uint32(ms.buf[pos+msgOffsetSize:]),
		}

		if lv == Full {
			msg.Message = Message(ms.buf[pos+MsgOverhead : pos+MsgOverhead+msg.MsgSize])
		}

		pos += msg.Size()

		return msg, nil
	})
}

// Size returns the byte size of the MessageSet.
func (ms *MessageSet) Size() uint32 {
	return uint32(len(ms.buf))
}

// Equal returns whether other MessageSet is equal to ms.
func (ms *MessageSet) Equal(other MessageSet) bool {
	return bytes.Equal(ms.buf, other.buf)
}

// WriteTo implements the io.WriterTo interface.
func (ms *MessageSet) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(ms.buf)
	return int64(n), err
}

// String implements the fmt.Stringer interface.
func (ms *MessageSet) String() string {
	var (
		str  bytes.Buffer
		iter = ms.Iterator()
		msg  *MessageOffset
		i    int
	)

	fmt.Fprintln(&str, "MessageSet{")
	for i = 0; i <= 100; i++ {
		if msg, _ = iter.Next(Full); msg == nil {
			break
		}
		fmt.Fprintf(&str, "  %d: %s,\n", msg.Offset, msg)
	}

	if i == 100 { // has more
		fmt.Fprintln(&str, "  ...")
	}

	fmt.Fprint(&str, "}")

	return str.String()
}

// set writes the provided Messages to the MessageSet
// starting with the provided offset.
func (ms *MessageSet) set(offset uint64, codec Codec, msgs ...Message) {
	var n uint32
	for i, msg := range msgs {
		msg.SetCodec(codec)
		binary.BigEndian.PutUint64(ms.buf[n:], offset+uint64(i))
		binary.BigEndian.PutUint32(ms.buf[n+msgOffsetSize:], msg.Size())
		n += uint32(MsgOverhead + copy(ms.buf[n+MsgOverhead:], msg))
	}
	ms.buf = ms.buf[:n]
}

// compress reduces the MessageSet to a single Message which holds the
// compressed payload in its value.
// It returns an error when the Codec fails to compress.
func (ms *MessageSet) compress(offset uint64, codec Codec) error {
	if codec == NoCodec {
		return nil
	}

	value, err := codec.Compress(ms.buf)
	if err != nil {
		return err
	}

	ms.set(offset-1, codec, NewMessage(nil, value))

	return nil
}
