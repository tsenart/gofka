package message

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// FileMessageSet is a file backed MessageSet.
type FileMessageSet struct {
	file   *os.File
	sr     *io.SectionReader
	pos, n int64
}

var (
	// ErrInvalid is an error returned whenever an invalid file is used with a
	// FileMessageSet.
	ErrInvalid = errors.New("invalid file")
)

// NewFileMessageSet returns a new FileMessageSet with the passed file.
// Iteration is windowed with an io.SectionReader instantiated with pos and n.
func NewFileMessageSet(f *os.File, pos, n int64) (*FileMessageSet, error) {
	if f == nil {
		return nil, ErrInvalid
	}

	if n == -1 {
		fi, err := f.Stat()
		if err != nil {
			return nil, err
		}
		n = fi.Size() - pos
	}

	return &FileMessageSet{
		file: f,
		sr:   io.NewSectionReader(f, pos, n),
		pos:  pos,
		n:    n,
	}, nil
}

// Append adds other MessageSet to the end of this FileMessageSet.
func (ms *FileMessageSet) Append(other *MessageSet) error {
	n, err := other.WriteTo(ms.file)
	if err != nil {
		return err
	}
	ms.n += n
	ms.sr = io.NewSectionReader(ms.file, ms.pos, ms.n)
	return nil
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
		pos int64
		n   int
		err error
		hdr = make([]byte, MsgOverhead)
	)
	return IteratorFunc(func(lv Level) (*MessageOffset, error) {
		if pos >= int64(ms.Size()-MsgOverhead) {
			return nil, io.EOF
		} else if n, err = ms.file.ReadAt(hdr, pos); err != nil {
			return nil, err
		}

		msg := &MessageOffset{
			Pos:     uint32(pos),
			Offset:  binary.BigEndian.Uint64(hdr),
			MsgSize: binary.BigEndian.Uint32(hdr[msgOffsetSize:]),
		}

		if lv == Full {
			msg.Message = make(Message, msg.MsgSize)
			if _, err = ms.file.ReadAt(msg.Message, pos+int64(n)); err != nil {
				return nil, err
			}
		}

		pos += int64(msg.Size())

		return msg, nil
	})
}

// Size returns the byte size of this FileMessageSet.
func (ms *FileMessageSet) Size() uint32 { return uint32(ms.sr.Size()) }
