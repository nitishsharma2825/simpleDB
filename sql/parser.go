package sql

// Entire grammar for the SQL subset supported by SimpleDB
// <Field> := TokenIdentifier
// <Constant> := TokenString | TokenNumber
// <Expression> := <Field> | <Constant>
// <Term> := <Expression> = <Expression>
// <Predicate> := <Term> [AND <Predicate>]
// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ] [ORDER BY <Field> [, <FieldList>]]
// <SelectList> := <Field> [, <SelectList> ]
// <TableList> := TokenIdentifier [, <TableList> ]
// <UpdateCmd> := <Insert> | <Delete> | <Modify> | <Create>
// <Create> := <CreateTable> | <CreateView> | <CreateIndex>
// <Insert> := INSERT INTO TokenIdentifier ( <FieldList> ) VALUES ( <ConstList> )
// <FieldList> := <Field> [, <FieldList> ]
// <ConstList> := <Constant> [, <ConstList> ]
// <Delete> := DELETE FROM TokenIdentifier [ WHERE <Predicate> ]
// <Modify> := UPDATE TokenIdentifier SET <Field> = <Expression> [ WHERE <Predicate> ]
// <CreateTable> := CREATE TABLE TokenIdentifier ( <FieldDefs> )
// <FieldDefs> := <FieldDef> [, <FieldDefs> ]
// <FieldDef> := TokenIdentifier <TypeDef>
// <TypeDef> := INT | TEXT | VARCHAR ( TokenNumber )
// <CreateView> := CREATE VIEW TokenIdentifier AS <Query>
// <CreateIndex> := CREATE INDEX TokenIdentifier ON TokenIdentifier ( <Field> )
