package record

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
)

func TestPlanner1(t *testing.T) {
	db := NewSimpleDB("plannertest1")
	tx := db.NewTx()
	planner := db.Planner()
	cmd := "create table T1(A int, B varchar(9))"
	planner.ExecuteUpdate(cmd, tx)

	t.Cleanup(func() {
		p1 := path.Join("plannertest1", "simpledb.log")
		os.RemoveAll(path.Dir(p1))
	})

	n := 200
	t.Logf("Inserting %d random records\n", n)
	for range n {
		k := rand.Intn(50)
		cmd = fmt.Sprintf("insert into T1(A, B) values (%d, 'rec%d')", k, k)
		planner.ExecuteUpdate(cmd, tx)
	}

	query := "select B from T1 where A=10"
	plan := planner.CreateQueryPlan(query, tx)
	scan := plan.Open().(*ProjectScan)
	for scan.Next() {
		t.Logf("%q\n", scan.GetString("b"))
	}
	scan.Close()
	tx.Commit()
}
