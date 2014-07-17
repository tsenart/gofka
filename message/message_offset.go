package message

// MessageOffset is an utility type wrapping a Message and its logical and
// physical offsets within a MessageSet.
type MessageOffset struct {
	// Offset is the Message logical offset within a MessageSet.
	Offset uint64
	// Pos is the Message physical byte offset within a MessageSet.
	Pos uint64
	// Message is the actual Message payload.
	Message
}
