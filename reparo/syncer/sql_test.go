package syncer

import (
	"github.com/kami-zh/go-capturer"
	"github.com/pingcap/check"
	"os"
)

type testSqlSuite struct{}

var _ = check.Suite(&testSqlSuite{})

func (s *testSqlSuite) TestSqlSyncer(c *check.C) {
	syncer, err := newSqlSyncer("")
	c.Assert(err, check.IsNil)

	out := capturer.CaptureOutput(func() {
		syncer.destSqlFile = os.Stdout // redeclare destSqlFile in CaptureOutput for output capture
		syncTest(c, Syncer(syncer))
	})

	c.Assert(out, check.Equals,
		"create database test;\n"+
			"Commit; # 0\n"+
			"Start Transaction; # 0\n"+
			"INSERT INTO test.t1(a, b, c) values (1, 'test', 'test');\n"+
			"DELETE FROM test.t1 WHERE a = 1 AND b = 'test' AND c = 'test';\n"+
			"UPDATE test.t1 SET a = 1, b = 'test', c = 'abc' WHERE a = 1 AND b = 'test' AND c = 'test';\n"+
			"Commit; # 0\n")

	//err = syncer.Close()  // destSqlFile have already closed in CaptureOutput, skip it
	c.Assert(err, check.IsNil)
}
