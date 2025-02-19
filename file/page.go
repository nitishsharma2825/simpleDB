package file

import "encoding/binary"

// the book uses java's ByteBuffer to represent the page
// Only Integer and String data present in page
// the book appends length of data in an integer 4 bytes before the actual data

const IntBytes = 4

type Page struct {
	buf     []byte
	maxSize int
}

func NewPageWithSize(size int) *Page {
	return &Page{
		buf:     make([]byte, size),
		maxSize: size,
	}
}

func NewPageWithSlice(buf []byte) *Page {
	return &Page{
		buf:     buf,
		maxSize: len(buf),
	}
}

func (p *Page) GetBytes(offset int) []byte {
	// size of the data
	size := p.GetInt(offset)

	result := make([]byte, size)
	copy(result, p.buf[offset+IntBytes:offset+IntBytes+size])

	return result
}

func (p *Page) SetBytes(offset int, buf []byte) {
	// check if bytes to be written are > page size
	assertPageWrite(offset+len(buf)+IntBytes, p.maxSize)

	p.SetInt(offset, len(buf))
	copy(p.buf[offset+IntBytes:], buf)
}

// string methods

func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetString(offset int, val string) {
	p.SetBytes(offset, []byte(val))
}

// Integer methods

func (p *Page) GetInt(offset int) int {
	return int(binary.BigEndian.Uint32(p.buf[offset : offset+IntBytes]))
}

func (p *Page) SetInt(offset int, val int) {
	binary.BigEndian.PutUint32(p.buf[offset:offset+IntBytes], uint32(val))
}

func (p *Page) Contents() []byte {
	return p.buf
}

// returns len of string + int32 prefix to indicate the size
func MaxLength(strlen int) int {
	return IntBytes + strlen
}

func assertPageWrite(offsetAfterInsert int, maxSize int) {
	if offsetAfterInsert > maxSize {
		panic("Page Out of bounds")
	}
}
