package file

import "encoding/binary"

// the book uses java's ByteBuffer to represent the page
// Only Integer and String data present in page
// the book appends length of data in an integer 4 bytes before the actual data

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
	copy(result, p.buf[offset+4:offset+4+size])

	return result
}

func (p *Page) SetBytes(offset int, buf []byte) {
	p.SetInt(offset, len(buf))
	copy(p.buf[offset+4:], buf)
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
	return int(binary.BigEndian.Uint32(p.buf[offset : offset+4]))
}

func (p *Page) SetInt(offset int, val int) {
	binary.BigEndian.PutUint32(p.buf[offset:offset+4], uint32(val))
}

// returns len of string + int32 prefix to indicate the size
func MaxLength(strlen int) int {
	return 4 + strlen
}

func (p *Page) Contents() []byte {
	return p.buf
}
