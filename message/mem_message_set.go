package message

import (
	"bytes"
	"encoding/binary"
)

// MemMessageSet is an in-memory implementation of the MessageSet interface.
type MemMessageSet struct{ buf []byte }

// NewMemMessageSet returns a MemMessageSet containing the provided Messages.
// The first offset is set to the provided one and increments from there for
// each Message.
//
// The compression Codec is found by iterating over all passed Messages and
// verifying that they all have the same Codec, or an error is returned.
// In case the Codec is valid, the resulting MemMessageSet will have a single
// Message with its value set to the compressed original MemMessageSet.
// If the compression fails an error will be returned.
func NewMemMessageSet(offset uint64, msgs ...Message) (*MemMessageSet, error) {
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

	ms := &MemMessageSet{make([]byte, size)}
	ms.Set(offset, msgs...)
	if err := ms.Compress(offset, codec); err != nil {
		return nil, err
	}
	return ms, nil
}

// Set appends the provided Messages starting at the provided logical offset.
func (ms *MemMessageSet) Set(offset uint64, msgs ...Message) {
	_, n := ms.Get(offset)

	for i, msg := range msgs {
		binary.BigEndian.PutUint64(ms.buf[n:], offset+uint64(i))
		binary.BigEndian.PutUint32(ms.buf[n+OffsetLength:], msg.Size())
		n += uint32(MsgOverhead + copy(ms.buf[n+MsgOverhead:], msg))
	}

	ms.buf = ms.buf[:n]
}

// Get returns the Message with the provided logical offset in MemMessageSet as
// well as the position where it was found.
func (ms *MemMessageSet) Get(offset uint64) (Message, uint32) {
	var (
		offs    uint64
		size, i uint32
	)

	for i = 0; i < ms.Size(); i += MsgOverhead + size {
		offs = binary.BigEndian.Uint64(ms.buf[i : i+OffsetLength])
		size = binary.BigEndian.Uint32(ms.buf[i+OffsetLength : i+MsgOverhead])

		if offs == offset {
			return Message(ms.buf[i+MsgOverhead : i+MsgOverhead+size]), i
		}
	}

	return nil, 0
}

// Compress reduces the MemMessageSet to a single Message which holds the
// compressed payload in its value. It returns the position of the last byte
// written as well as an error when the Codec fails to compress.
func (ms *MemMessageSet) Compress(offset uint64, codec Codec) error {
	if codec == NoCodec {
		return nil
	}

	value, err := codec.Compress(ms.buf)
	if err != nil {
		return err
	}

	ms.Set(offset-1, NewMessage(nil, value, codec))

	return nil
}

// Size returns the byte size of the MemMessageSet.
func (ms *MemMessageSet) Size() uint32 {
	return uint32(len(ms.buf))
}

// Equal returns whether other MemMessageSet is equal to ms.
func (ms *MemMessageSet) Equal(other MemMessageSet) bool {
	return bytes.Equal(ms.buf, other.buf)
}

const (
	MsgSizeLength = 4
	OffsetLength  = 8
	MsgOverhead   = MsgSizeLength + OffsetLength
)
