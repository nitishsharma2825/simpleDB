package record

const (
	INTEGER = 4
	VARCHAR = 12
)

/*
The record schema of a table
Contains the name and type of each field of the table
as well as the len of each varchar field
*/
type Schema struct {
	fields []string
	info   map[string]FieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

func (s *Schema) AddField(fieldName string, fieldType int, length int) {
	s.fields = append(s.fields, fieldName)
	s.info[fieldName] = NewFieldInfo(fieldType, length)
}

func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, INTEGER, 0)
}

func (s *Schema) AddStringField(fieldName string, length int) {
	s.AddField(fieldName, VARCHAR, length)
}

/*
Add a field to the schema having the same
type and length as the field in the another schema
*/
func (s *Schema) Add(fieldName string, sch *Schema) {
	fieldType := sch.FieldType(fieldName)
	length := sch.Length(fieldName)
	s.AddField(fieldName, fieldType, length)
}

/*
Add all of the fields in the specified schema to this schema
*/
func (s *Schema) Addall(sch *Schema) {
	for _, fieldName := range sch.Fields() {
		s.Add(fieldName, sch)
	}
}

func (s *Schema) HasField(fieldName string) bool {
	_, ok := s.info[fieldName]
	return ok
}

func (s *Schema) FieldType(fieldName string) int {
	return s.info[fieldName].fieldType
}

func (s *Schema) Length(fieldName string) int {
	return s.info[fieldName].length
}

func (s *Schema) Fields() []string {
	return s.fields
}

type FieldInfo struct {
	fieldType int
	length    int
}

func NewFieldInfo(fieldType int, length int) FieldInfo {
	return FieldInfo{fieldType, length}
}
