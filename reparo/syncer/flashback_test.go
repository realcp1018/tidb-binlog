package syncer

import (
	"github.com/kami-zh/go-capturer"
	"github.com/pingcap/check"
	"os"
)

type testFlashbackSuite struct{}

var _ = check.Suite(&testFlashbackSuite{})

func (s *testFlashbackSuite) TestFlashbackSyncer(c *check.C) {
	syncer, err := newFlashbackSyncer("")
	c.Assert(err, check.IsNil)

	out := capturer.CaptureOutput(func() {
		syncer.destSqlFile = os.Stdout // redeclare destSqlFile in CaptureOutput for output capture
		syncTest(c, Syncer(syncer))
	})

	c.Assert(out, check.Equals,
		"Start Transaction; # 0\n"+
			"DELETE FROM test.t1 WHERE a = 1 AND b = 'test' AND c = 'test';\n"+
			"INSERT INTO test.t1(a, b, c) VALUES (1, 'test', 'test');\n"+
			"UPDATE test.t1 SET a = 1, b = 'test', c = 'test' WHERE a = 1 AND b = 'test' AND c = 'abc';\n"+
			"Commit; # 0\n")

	//err = syncer.Close()  // destSqlFile have already closed in CaptureOutput, skip it
	c.Assert(err, check.IsNil)
}
