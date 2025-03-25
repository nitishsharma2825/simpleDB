package record

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestPlanner1(t *testing.T) {
	db := NewSimpleDB("plannertest1")
	tx := db.NewTx()
	planner := db.Planner()
	cmd := "create table T1(A int, B varchar(9))"
	planner.ExecuteUpdate(cmd, tx)

	n := 200
	t.Logf("Inserting %d random records\n", n)
	for range n {
		k := rand.Intn(50)
		cmd = fmt.Sprintf("insert into T1(A, B) values (%d, 'rec%d')", k, k)
		t.Logf("current command: %q\n", cmd)
		planner.ExecuteUpdate(cmd, tx)
	}

	t.Logf("Done with record insertion\n")

	query := "select B from T1 where A=10"
	plan := planner.CreateQueryPlan(query, tx)
	scan := plan.Open().(*SelectScan)
	for scan.Next() {
		t.Logf("%q\n", scan.GetString("B"))
	}
	scan.Close()
	tx.Commit()
}
