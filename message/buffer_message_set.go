package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// BufferMessageSet is an in-memory implementation of the MessageSet interface.
type BufferMessageSet struct{ buf []byte }

var _ MessageSet = (*BufferMessageSet)(nil)

// NewBufferMessageSet returns a BufferMessageSet containing the provided Messages.
// The first offset is set to the provided one and increments from there for
// each Message.
//
// The compression Codec is found by iterating over all passed Messages and
// verifying that they all have the same Codec, or an error is returned.
// In case the Codec is valid, the resulting BufferMessageSet will have a single
// Message with its value set to the compressed original BufferMessageSet.
// If the compression fails an error will be returned.
func NewBufferMessageSet(offset uint64, msgs ...Message) (*BufferMessageSet, error) {
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

	ms := &BufferMessageSet{make([]byte, size)}
	ms.set(offset, msgs...)
	if err := ms.compress(offset, codec); err != nil {
		return nil, err
	}
	return ms, nil
}

// Iterate calls fn for each Message in the MessageSet.
// TODO: Support decompression.
func (ms *BufferMessageSet) Iterate(fn Iterator) bool {
	var (
		offset uint64
		size   uint32
		msg    Message
	)
	for i := 0; i < ms.Size(); i += int(MsgOverhead + size) {
		offset = binary.BigEndian.Uint64(ms.buf[i : i+OffsetLength])
		size = binary.BigEndian.Uint32(ms.buf[i+OffsetLength : i+MsgOverhead])
		msg = Message(ms.buf[i+MsgOverhead : i+int(MsgOverhead+size)])

		if fn(MessageAndOfset{Offset: offset, Message: msg}) {
			return true // Halt iteration
		}
	}
	return false
}

// Size returns the byte size of the BufferMessageSet.
func (ms *BufferMessageSet) Size() int {
	return len(ms.buf)
}

// Equal returns whether other BufferMessageSet is equal to ms.
func (ms *BufferMessageSet) Equal(other BufferMessageSet) bool {
	return bytes.Equal(ms.buf, other.buf)
}

// WriteTo implements the io.WriterTo interface.
func (ms *BufferMessageSet) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(ms.buf)
	return int64(n), err
}

// String implements the fmt.Stringer interface.
func (ms *BufferMessageSet) String() string {
	var (
		str bytes.Buffer
		lim = 100
	)

	fmt.Fprintln(&str, "BufferMessageSet{")
	halted := ms.Iterate(func(msg MessageAndOfset) bool {
		if lim -= 1; lim == 0 {
			return true
		}
		fmt.Fprintln(&str, "  ", msg, ",")
		return false
	})

	if halted {
		fmt.Fprintln(&str, "  ...")
	}

	fmt.Fprint(&str, "}")

	return str.String()
}

// set writes the provided Messages to the MessageSet
// starting with the provided offset.
func (ms *BufferMessageSet) set(offset uint64, msgs ...Message) {
	var n uint32
	for i, msg := range msgs {
		binary.BigEndian.PutUint64(ms.buf[n:], offset+uint64(i))
		binary.BigEndian.PutUint32(ms.buf[n+OffsetLength:], msg.Size())
		n += uint32(MsgOverhead + copy(ms.buf[n+MsgOverhead:], msg))
	}
	ms.buf = ms.buf[:n]
}

// compress reduces the BufferMessageSet to a single Message which holds the
// compressed payload in its value.
// It returns an error when the Codec fails to compress.
func (ms *BufferMessageSet) compress(offset uint64, codec Codec) error {
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
