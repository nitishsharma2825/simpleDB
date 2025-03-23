package record

import (
	"github.com/nitishsharma2825/simpleDB/tx"
)

// max characters a tablename/fieldname can have
const MAX_NAME = 16

/*
Create a table,
save the metadata in the catalog,
obtain metadata of a previously created table
*/
type TableManager struct {
	// store metadata about each table (tblname, slotSize)
	tcatLayout *Layout
	// store metadata about each field of each table (tblname, fldName, type, length, offset)
	fcatLayout *Layout
}

func NewTableManager(isNew bool, tx *tx.Transaction) *TableManager {
	tcatSchema := NewSchema()
	tcatSchema.AddStringField("tblname", MAX_NAME)
	tcatSchema.AddIntField("slotsize")
	tcatLayout := NewLayout(tcatSchema)

	fcatSchema := NewSchema()
	fcatSchema.AddStringField("tblname", MAX_NAME)
	fcatSchema.AddStringField("fldname", MAX_NAME)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	fcatLayout := NewLayout(fcatSchema)

	tm := &TableManager{
		tcatLayout: tcatLayout,
		fcatLayout: fcatLayout,
	}

	if isNew {
		tm.CreateTable("tblcat", tcatSchema, tx)
		tm.CreateTable("fldcat", fcatSchema, tx)
	}

	return tm
}

func (tm *TableManager) CreateTable(tblName string, schema *Schema, tx *tx.Transaction) {
	layout := NewLayout(schema)

	// insert 1 record into tblcat
	tcat := NewTableScan(tx, "tblcat", tm.tcatLayout)
	tcat.Insert()
	tcat.SetString("tblname", tblName)
	tcat.SetInt("slotsize", layout.SlotSize())
	tcat.Close()

	// insert 1 record into fldcat for each field
	fcat := NewTableScan(tx, "fldcat", tm.fcatLayout)
	for _, fieldName := range schema.Fields() {
		fcat.Insert()
		fcat.SetString("tblname", tblName)
		fcat.SetString("fldname", fieldName)
		fcat.SetInt("type", schema.FieldType(fieldName))
		fcat.SetInt("length", schema.Length(fieldName))
		fcat.SetInt("offset", layout.Offset(fieldName))
	}
	fcat.Close()
}

func (tm *TableManager) GetLayout(tblname string, tx *tx.Transaction) *Layout {
	size := -1

	// find the table in tcat
	tcat := NewTableScan(tx, "tblcat", tm.tcatLayout)
	for tcat.Next() {
		if tcat.GetString("tblname") == tblname {
			size = tcat.GetInt("slotsize")
			break
		}
	}
	tcat.Close()

	sch := NewSchema()
	offsets := make(map[string]int)
	fcat := NewTableScan(tx, "fldcat", tm.fcatLayout)
	for fcat.Next() {
		if fcat.GetString("tblname") == tblname {
			fldname := fcat.GetString("fldname")
			fldType := fcat.GetInt("type")
			length := fcat.GetInt("length")
			offset := fcat.GetInt("offset")
			offsets[fldname] = offset
			sch.AddField(fldname, fldType, length)
		}
	}
	fcat.Close()
	return NewLayoutWithMetadata(sch, offsets, size)
}
