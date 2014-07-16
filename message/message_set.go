package message

import (
	"fmt"
	"io"
)

// Errors used by implementers of the MessageSet interface.
var (
	ErrMultipleCodecs = fmt.Errorf("multiple codecs found")
	ErrNoMessages     = fmt.Errorf("no messages provided")
	ErrNilMessages    = fmt.Errorf("nil messages provided")
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
// the value of a single Message within the MemMessageSet.
//
//  -----------------------------
//  | offset+N | size | message |
//  -----------------------------
//
// The byte sizes of each of these fields are: offset: 8, size: 4, message: size
type MessageSet interface {
	io.WriterTo
	Has(msg Message) bool
	Offset(msg Message) uint64
	Get(offset, limit uint64) ([]Message, error)
	Set(offset uint64, msgs ...Message) error
	Each(offset uint64, fn func(msg Message))
}
