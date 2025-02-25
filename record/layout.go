package record

import "github.com/nitishsharma2825/simpleDB/file"

/*
Description of the structure of a record
Contains name, type, length and offset of each field of the table
*/
type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

/*
Create a layout from the given schema
Used when a table is created.
Determines the physical offset of each field within the record
*/
func NewLayout(schema *Schema) *Layout {
	layout := &Layout{
		schema:  schema,
		offsets: make(map[string]int),
	}

	pos := file.IntBytes // leave space for empty/inuse flag
	for _, fieldName := range schema.Fields() {
		layout.offsets[fieldName] = pos
		pos += lengthInBytes(schema, fieldName)
	}
	layout.slotSize = pos

	return layout
}

/*
Create a layout object from the specified metadata
this is used when metadata is retrieved from the catalog
*/
func NewLayoutWithMetadata(schema *Schema, offsets map[string]int, slotSize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fieldName string) int {
	return l.offsets[fieldName]
}

func (l *Layout) SlotSize() int {
	return l.slotSize
}

func lengthInBytes(schema *Schema, fieldName string) int {
	fieldType := schema.FieldType(fieldName)
	if fieldType == INTEGER {
		return file.IntBytes
	}
	return file.MaxLength(schema.Length(fieldName))
}
