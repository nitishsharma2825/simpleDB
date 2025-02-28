package record

import "testing"

func TestLayout(t *testing.T) {
	sch := NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)

	layout := NewLayout(sch)
	for _, fieldName := range layout.Schema().fields {
		offset := layout.Offset(fieldName)
		t.Logf("%q has offset %d", fieldName, offset)
	}
}
