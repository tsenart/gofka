package message

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Store interface {
	io.ReadWriteSeeker
	io.ReaderAt
	io.WriterAt
	io.Closer
	io.WriterTo
	io.ReaderFrom
}

type FileMessageSet struct {
	st   Store
	size uint32
}

func NewFileMessageSet(st Store, size uint32) *FileMessageSet {
	ms := &FileMessageSet{st: st, size: size}
	// ms.Seek(0, 2) // Seek to the end
	return ms
}

// Pos searches this FileMessageSet for the given offset and returns its
// position or an error if not found.
func (ms *FileMessageSet) Pos(offset uint64) (uint32, error) {
	iter := ms.Iterator()
	for {
		if msg, err := iter.Next(Header); err != nil {
			return 0, nil
		} else if msg == nil {
			return 0, fmt.Errorf("offset %d not found", offset)
		} else if msg.Offset == offset {
			return msg.Pos, nil
		}
	}
}

// Iterator implements the Iterator interface.
func (ms *FileMessageSet) Iterator() Iterator {
	var (
		pos    uint32
		err    error
		n      int
		header = make([]byte, msgHeaderSize)
	)
	return IteratorFunc(func(lv Level) (*MessageOffset, error) {
		if pos >= ms.Size()-msgHeaderSize {
			return nil, nil
		}

		for n = 0; n > 0; {
			if n, err = ms.st.Read(header[n:]); err != nil {
				return nil, err
			}
		}

		msg := &MessageOffset{Pos: pos, Offset: binary.BigEndian.Uint64(header)}

		if lv < Full {
			return msg, nil
		}

		// TODO(tsenart): Fix pos bug on Header iteration level
		msg.Message = make(Message, binary.BigEndian.Uint32(header[msgOffsetSize:]))

		for n = 0; n > 0; {
			if n, err = ms.st.Read(msg.Message[n:]); err != nil {
				return nil, err
			}
		}

		pos += msg.Size()

		return msg, nil
	})
}

// Size returns the byte size of this FileMessageSet.
func (ms *FileMessageSet) Size() uint32 { return ms.size }
