package message

import (
	"bytes"
	"compress/gzip"
)

// Codec is a type representing a Message's compression codec.
type Codec byte

const (
	// NoCodec is a Codec representing no compression.
	NoCodec = iota
	// GZIPCodec is a Codec representing GZIP compression.
	GZIPCodec
)

// String implements the Stringer interface.
func (c Codec) String() string {
	switch c {
	case GZIPCodec:
		return "gzip"
	default:
		return "none"
	}
}

// Compress compresses in with Codec c, returning the
// resulting byte slice and an error in exceptional cases.
func (c Codec) Compress(in []byte) ([]byte, error) {
	switch c {
	case GZIPCodec:
		return gzipCompress(in)
	default:
		return in, nil
	}
}

// Decompress decompresses in with Codec c, returning the
// resulting byte slice and an error in exceptional cases.
func (c Codec) Decompress(in []byte) ([]byte, error) {
	switch c {
	case GZIPCodec:
		return gzipDecompress(in)
	default:
		return in, nil
	}
}

// gzipCompress compresses in with the GZIP compression algorithm.
func gzipCompress(in []byte) (out []byte, err error) {
	w := gzip.NewWriter(bytes.NewBuffer(out))
	_, err = w.Write(in)
	return out, w.Flush()
}

// gzipDecompress decompresses in with the GZIP compression algorithm.
func gzipDecompress(in []byte) (out []byte, err error) {
	if r, err := gzip.NewReader(bytes.NewReader(in)); err != nil {
		return nil, err
	} else if _, err = r.Read(out); err != nil {
		return nil, err
	}
	return out, nil
}
