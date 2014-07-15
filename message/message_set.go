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
// each Message. Nil Messages will be ignored.
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
	codec, size := msgs[0].Codec(), msgOverhead+msgs[0].Size()

	for i := 1; i < len(msgs); i++ {
		if msgs[i] == nil {
			return nil, ErrNilMessages
		} else if msgs[i].Codec() != codec {
			return nil, ErrMultipleCodecs
		}
		size += msgOverhead + msgs[i].Size()
	}

	var (
		err error
		ms  = make(MessageSet, size).fill(offset, msgs...)
	)

	if ms, err = ms.compress(offset, codec); err != nil {
		return nil, err
	}

	return ms, nil
}

// Size returns the byte size of the MessageSet.
func (ms MessageSet) Size() uint32 {
	return uint32(len(ms))
}

// Equal returns whether other MessageSet is equal to ms.
func (ms MessageSet) Equal(other MessageSet) bool {
	return bytes.Equal(ms, other)
}

func (ms MessageSet) fill(offset uint64, msgs ...Message) MessageSet {
	var (
		cursor    []byte
		low, high int
	)
	for _, m := range msgs {
		offset++
		low = len(cursor)
		high = low + msgOverhead + len(m)
		cursor = ms[low:high]
		binary.BigEndian.PutUint64(cursor, offset)
		binary.BigEndian.PutUint32(cursor[offsetLength:], m.Size())
		copy(cursor[msgOverhead:], m)
	}
	return ms[:high]
}

func (ms MessageSet) compress(offset uint64, codec Codec) (MessageSet, error) {
	if codec == NoCodec {
		return ms, nil
	}
	value, err := codec.Compress(ms)
	if err != nil {
		return ms, err
	}
	return ms.fill(offset-1, NewMessage(nil, value, codec)), nil
}

const (
	msgSizeLength = 4
	offsetLength  = 8
	msgOverhead   = msgSizeLength + offsetLength
)
