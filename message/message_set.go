package message

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Errors used by implementers of the MessageSet interface.
var (
	ErrMultipleCodecs = errors.New("multiple codecs found")
	ErrNoMessages     = errors.New("no messages provided")
	ErrNilMessages    = errors.New("nil messages provided")
)

// Constants used by implementers of the MessageSet interface.
const (
	OffsetLength  = 8
	MsgSizeLength = 4
	MsgOverhead   = MsgSizeLength + OffsetLength
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
// each Message.
//
// The compression Codec is found by iterating over all passed Messages and
// verifying that they all have the same Codec, or an error is returned.
// In case the Codec is valid, the resulting MessageSet will have a single
// Message with its value set to the compressed original MessageSet.
// If the compression fails an error will be returned.
func NewMessageSet(offset uint64, msgs ...Message) (*MessageSet, error) {
	if len(msgs) == 0 {
		return nil, ErrNoMessages
	} else if msgs[0] == nil {
		return nil, ErrNilMessages
	}
	codec, size := msgs[0].Codec(), MsgOverhead+msgs[0].Size()

	for i := 1; i < len(msgs); i++ {
		if msgs[i] == nil {
			return nil, ErrNilMessages
		} else if msgs[i].Codec() != codec {
			return nil, ErrMultipleCodecs
		}
		size += MsgOverhead + msgs[i].Size()
	}

	ms := &MessageSet{make([]byte, size)}
	ms.set(offset, msgs...)
	if err := ms.compress(offset, codec); err != nil {
		return nil, err
	}
	return ms, nil
}

// SizeOf computes the byte size of a MessageSet containing msgs Messages.
func SizeOf(msgs ...Message) (size uint32) {
	for i := range msgs {
		size += MsgOverhead + msgs[i].Size()
	}
	return
}

// Iterator implements the Iterator interface.
func (ms *MessageSet) Iterator() Iterator {
	var pos, size uint32
	return IteratorFunc(func(lv Level) (*MessageOffset, error) {
		if pos >= ms.Size()-MsgOverhead {
			return nil, nil
		}

		msg := &MessageOffset{
			Offset: binary.BigEndian.Uint64(ms.buf[pos : pos+OffsetLength]),
		}

		if lv < Full {
			return msg, nil
		}

		size = binary.BigEndian.Uint32(ms.buf[pos+OffsetLength:])
		msg.Message = Message(ms.buf[pos+MsgOverhead : pos+MsgOverhead+size])
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
func (ms *MessageSet) set(offset uint64, msgs ...Message) {
	var n uint32
	for i, msg := range msgs {
		binary.BigEndian.PutUint64(ms.buf[n:], offset+uint64(i))
		binary.BigEndian.PutUint32(ms.buf[n+OffsetLength:], msg.Size())
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

	ms.set(offset-1, NewMessage(nil, value, codec))

	return nil
}
