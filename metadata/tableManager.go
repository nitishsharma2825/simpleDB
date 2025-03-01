package metadata

import (
	"github.com/nitishsharma2825/simpleDB/record"
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
	tcatLayout *record.Layout
	// store metadata about each field of each table (tblname, fldName, type, length, offset)
	fcatLayout *record.Layout
}

func NewTableManager(isNew bool, tx *tx.Transaction) *TableManager {
	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField("tblname", MAX_NAME)
	tcatSchema.AddIntField("slotsize")
	tcatLayout := record.NewLayout(tcatSchema)

	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField("tblname", MAX_NAME)
	fcatSchema.AddStringField("fldname", MAX_NAME)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	fcatLayout := record.NewLayout(fcatSchema)

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

func (tm *TableManager) CreateTable(tblName string, schema *record.Schema, tx *tx.Transaction) {
	layout := record.NewLayout(schema)

	// insert 1 record into tblcat
	tcat := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	tcat.Insert()
	tcat.SetString("tblname", tblName)
	tcat.SetInt("slotsize", layout.SlotSize())
	tcat.Close()

	// insert 1 record into fldcat for each field
	fcat := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
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

func (tm *TableManager) GetLayout(tblname string, tx *tx.Transaction) *record.Layout {
	size := -1

	// find the table in tcat
	tcat := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	for tcat.Next() {
		if tcat.GetString("tblname") == tblname {
			size = tcat.GetInt("slotsize")
			break
		}
	}
	tcat.Close()

	sch := record.NewSchema()
	offsets := make(map[string]int)
	fcat := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
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
	return record.NewLayoutWithMetadata(sch, offsets, size)
}
