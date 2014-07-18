package message

// Level defines the iteration level to use.
type Level uint8

const (
	// Header defines an Iterator level which only reads a Message's header
	// on each iteration.
	Header Level = iota
	// Full defines an Iterator level which reads a full message on each
	// iteration.
	Full
)

// Iterator defines an interface for a MessageSet iterator.
type Iterator interface {
	// Next returns a pointer to the next msg in the MessageSet or nil if the end
	// is reached.
	Next(lv Level) (*MessageOffset, error)
}

// IteratorFunc is an adapter to allow the use of ordinary functions as
// Iterators.
type IteratorFunc func(Level) (*MessageOffset, error)

// Next implements the Iterator interface by calling wrapping fn with the right
// signature.
func (fn IteratorFunc) Next(lv Level) (*MessageOffset, error) {
	return fn(lv)
}
