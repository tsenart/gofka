package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// MessageSet represents an in-memory container for multiple Messages
// with a fixed serialization format.
type MessageSet []byte

var (
	// ErrMultipleCodecs is an error returned by NewMessageSet
	// when the provided slice of Messages contains more than one Codec.
	ErrMultipleCodecs = fmt.Errorf("multiple codecs found in msgs")
	// ErrNoMessages is an error returned by NewMessageSet
	// when the provided Message slice is empty.
	ErrNoMessages = fmt.Errorf("no messages provided")
	// ErrNilMessages is an error returned by NewMessageSet
	// when the provided Message slice includes nil Messages.
	ErrNilMessages = fmt.Errorf("nil messages provided")
)

// NewMessageSet returns a MessageSet containing the provided Messages.
// The first offset is set to the provided one and increments from there for
// each Message.
//
// The compression Codec is found by iterating over all passed Messages and
// verifying that they all have the same Codec, or an error is returned.
// In case the Codec is valid, the resulting MessageSet will have a single
// Message with its value set to the compressed original MessageSet.
// If the compression fails an error will be returned.
//
// The following diagrams describe the MessageSet memory layout.
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
// The byte sizes of each of these fields are: offset: 8, size: 4, message: size
func NewMessageSet(offset uint64, msgs ...Message) (MessageSet, error) {
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

	ms := make(MessageSet, size)
	ms = ms[:ms.Set(offset, msgs...)]
	n, err := ms.Compress(offset, codec)
	if err != nil {
		return nil, err
	}
	return ms[:n], nil
}

// Set sets the provided Messages starting at the provided logical offset and
// returns the position of the last byte written.
func (ms MessageSet) Set(offset uint64, msgs ...Message) uint32 {
	_, n := ms.Get(offset)

	for i, msg := range msgs {
		binary.BigEndian.PutUint64(ms[n:], offset+uint64(i))
		binary.BigEndian.PutUint32(ms[n+OffsetLength:], msg.Size())
		n += uint32(MsgOverhead + copy(ms[n+MsgOverhead:], msg))
	}

	return n
}

// Get returns the Message with the provided logical offset in MessageSet as
// well as the position where it was found.
func (ms MessageSet) Get(offset uint64) (Message, uint32) {
	var (
		offs    uint64
		size, i uint32
	)

	for i = 0; i < ms.Size(); i += MsgOverhead + size {
		offs = binary.BigEndian.Uint64(ms[i : i+OffsetLength])
		size = binary.BigEndian.Uint32(ms[i+OffsetLength : i+MsgOverhead])

		if offs == offset {
			return Message(ms[i+MsgOverhead : i+MsgOverhead+size]), i
		}
	}

	return nil, 0
}

// Compress reduces the MessageSet to a single Message which holds the
// compressed payload in its value. It returns the position of the last byte
// written as well as an error when the Codec fails to compress.
func (ms MessageSet) Compress(offset uint64, codec Codec) (uint32, error) {
	if codec == NoCodec {
		return uint32(len(ms)), nil
	}

	value, err := codec.Compress(ms)
	if err != nil {
		return 0, err
	}

	return ms.Set(offset-1, NewMessage(nil, value, codec)), nil
}

// Size returns the byte size of the MessageSet.
func (ms MessageSet) Size() uint32 {
	return uint32(len(ms))
}

// Equal returns whether other MessageSet is equal to ms.
func (ms MessageSet) Equal(other MessageSet) bool {
	return bytes.Equal(ms, other)
}

const (
	MsgSizeLength = 4
	OffsetLength  = 8
	MsgOverhead   = MsgSizeLength + OffsetLength
)
