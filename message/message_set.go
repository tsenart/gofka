package message

import (
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

// MessageSet defines an interface for a flat Message container with a fixed
// serialization format, whether in-memory or on-disk.
//
// The following diagrams describe the MessageSet byte layout.
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
type MessageSet interface {
	Iterate(fn Iterator) (halted bool)
	Size() int
	io.WriterTo
	fmt.Stringer
}

// Iterator defines an utility type defining a MessageSet iterator
type Iterator func(offset uint64, msg Message) (halt bool)
