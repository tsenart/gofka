package message

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// Magic is a type representing a Message's magic byte which indicates the
// wire-format version.
type Magic byte

const (
	// One is a Magic byte representing version one of the Message's wire-format.
	One Magic = 1
)

// Codec is a type representing a Message's compression codec.
type Codec byte

const (
	// NoCodec is a Codec representing no compression.
	NoCodec = iota
	// GZIPCodec is a Codec representing GZIP compression.
	GZIPCodec
)

// Message is the individual datum handled throughout the system.
type Message []byte

const (
	CrcOffset       = 0
	CrcLength       = 4
	MagicOffset     = CrcOffset + CrcLength
	MagicLength     = 1
	AttrsOffset     = MagicOffset + MagicLength
	AttrsLength     = 1
	KeySizeOffset   = AttrsOffset + AttrsLength
	KeySizeLength   = 4
	KeyOffset       = KeySizeOffset + KeySizeLength
	ValueSizeLength = 4
	MinHeaderSize   = CrcLength + MagicLength + AttrsLength + KeySizeLength + ValueSizeLength
	CodecMask       = 0x07
)

// NewMessage returns a new Message with the given parameters.
func NewMessage(key, value []byte, codec Codec) Message {
	if key == nil {
		key = []byte{}
	}

	if value == nil {
		value = []byte{}
	}

	m := make(Message, MinHeaderSize+len(key)+len(value))
	m.SetMagic(One)
	m.SetCodec(codec)
	m.SetKey(key)
	m.SetValue(value)
	m.SetChecksum()

	return m
}

// Checksum returns the Message's CRC32 checksum.
func (m Message) Checksum() uint32 {
	return binary.BigEndian.Uint32(m[CrcOffset:])
}

// SetChecksum computes and saves the Message's CRC32 checksum.
func (m Message) SetChecksum() {
	binary.BigEndian.PutUint32(m[CrcOffset:], m.Hash())
}

// Hash computes and returns the Message's CRC32 checksum.
func (m Message) Hash() uint32 {
	return crc32.ChecksumIEEE(m[MagicOffset:])
}

// Valid returns whether the Message's integrity is intact by comparing the
// saved checksum field with a recomputed checksum.
func (m Message) Valid() bool {
	return m.Checksum() == m.Hash()
}

// Magic returns a Magic byte representing the Message's version.
func (m Message) Magic() Magic {
	return Magic(m[MagicOffset])
}

// SetMagic sets the Message's magic byte.
func (m Message) SetMagic(magic Magic) {
	m[MagicOffset] = byte(magic)
}

// SetCodec sets the Message's compression codec.
func (m Message) SetCodec(codec Codec) {
	m[AttrsOffset] = CodecMask & byte(codec)
}

// Codec returns a Codec byte representing the Message's compression codec.
func (m Message) Codec() Codec {
	return Codec(m[AttrsOffset] & CodecMask)
}

// Key returns the Message's key.
func (m Message) Key() []byte {
	keyLength := binary.BigEndian.Uint32(m[KeySizeOffset:])
	return m[KeyOffset : KeyOffset+keyLength]
}

// SetKey sets the Message's key.
func (m Message) SetKey(key []byte) {
	binary.BigEndian.PutUint32(m[KeySizeOffset:], uint32(len(key)))
	copy(m[KeyOffset:], key)
}

// Value returns the Message's value.
func (m Message) Value() []byte {
	keyLength := binary.BigEndian.Uint32(m[KeySizeOffset:])
	return m[KeyOffset+keyLength+ValueSizeLength:]
}

// SetValue sets the Message's value.
func (m Message) SetValue(value []byte) {
	keyLength := binary.BigEndian.Uint32(m[KeySizeOffset:])
	binary.BigEndian.PutUint32(m[KeyOffset+keyLength:], uint32(len(value)))
	copy(m[KeyOffset+keyLength+ValueSizeLength:], value)
}

// Equal returns whether other Message is equal to m.
func (m Message) Equal(other Message) bool {
	return bytes.Equal(m, other)
}
