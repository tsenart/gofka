package message

import (
	"fmt"
	"io"
)

var (
	// ErrMultipleCodecs is an error returned by NewMemMessageSet
	// when the provided slice of Messages contains more than one Codec.
	ErrMultipleCodecs = fmt.Errorf("multiple codecs found in msgs")
	// ErrNoMessages is an error returned by NewMemMessageSet
	// when the provided Message slice is empty.
	ErrNoMessages = fmt.Errorf("no messages provided")
	// ErrNilMessages is an error returned by NewMemMessageSet
	// when the provided Message slice includes nil Messages.
	ErrNilMessages = fmt.Errorf("nil messages provided")
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
