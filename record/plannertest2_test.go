package record

import (
	"fmt"
	"os"
	"path"
	"testing"
)

func TestPlanner2(t *testing.T) {
	db := NewSimpleDB("plannertest2")
	tx := db.NewTx()
	planner := db.Planner()
	cmd := "create table T1(A int, B varchar(9))"
	planner.ExecuteUpdate(cmd, tx)

	t.Cleanup(func() {
		p1 := path.Join("plannertest2", "simpledb.log")
		os.RemoveAll(path.Dir(p1))
	})

	n := 200
	t.Logf("Inserting %d records into T1\n", n)
	for i := range n {
		cmd = fmt.Sprintf("insert into T1(A, B) values (%d, 'bbb%d')", i, i)
		planner.ExecuteUpdate(cmd, tx)
	}

	cmd = "create table T2(C int, D varchar(9))"
	planner.ExecuteUpdate(cmd, tx)
	t.Logf("Inserting %d records into T2\n", n)
	for i := range n {
		cmd = fmt.Sprintf("insert into T2(C, D) values (%d, 'ddd%d')", n-i-1, n-i-1)
		planner.ExecuteUpdate(cmd, tx)
	}

	query := "select B,D from T1,T2 where A=C"
	plan := planner.CreateQueryPlan(query, tx)
	scan := plan.Open().(*ProjectScan)
	for scan.Next() {
		t.Logf("%q %q\n", scan.GetString("b"), scan.GetString("d"))
	}
	scan.Close()
	tx.Commit()
}
