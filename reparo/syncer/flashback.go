package syncer

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
	"io"
	"os"
)

type flashbackSyncer struct {
	destSqlFile io.WriteCloser
}

func newFlashbackSyncer(sqlFile string) (*flashbackSyncer, error) {
	if sqlFile == "" {
		return &flashbackSyncer{destSqlFile: os.Stdout}, nil
	}
	file, err := os.OpenFile(sqlFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &flashbackSyncer{destSqlFile: file}, nil
}

func (s *flashbackSyncer) Sync(pbBinlog *pb.Binlog, cb func(binlog *pb.Binlog)) error {
	switch pbBinlog.Tp {
	case pb.BinlogType_DDL:
		log.Info("DDL skipped", zap.ByteString("statement", pbBinlog.GetDdlQuery()),
			zap.Int64("commitTs", pbBinlog.CommitTs))
		cb(pbBinlog)
	case pb.BinlogType_DML:
		s.writeTxnStop(pbBinlog.GetCommitTs())
		for _, event := range pbBinlog.GetDmlData().GetEvents() {
			if err := s.writeDMLEvent(&event); err != nil {
				return err
			}
		}
		s.writeTxnStart(pbBinlog.GetCommitTs())
		cb(pbBinlog)
	default:
		return errors.Errorf("unknown type: %v", pbBinlog.Tp)
	}

	return nil
}

func (s *flashbackSyncer) Close() error {
	if err := s.destSqlFile.Close(); err != nil {
		return err
	}
	return nil
}

func (s *flashbackSyncer) writeDDL(binlog *pb.Binlog) error {
	if _, err := s.destSqlFile.Write(binlog.GetDdlQuery()); err != nil {
		return err
	}
	s.writeLineSep()
	return nil
}

func (s *flashbackSyncer) writeDMLEvent(event *pb.Event) error {
	switch event.GetTp() {
	case pb.EventType_Insert:
		err := s.writeInsertEvent(event.GetSchemaName(), event.GetTableName(), event.Row)
		if err != nil {
			log.Error("print insert event for flashback failed", zap.Error(err))
		}
		s.writeLineSep()
	case pb.EventType_Update:
		err := s.writeUpdateEvent(event.GetSchemaName(), event.GetTableName(), event.Row)
		if err != nil {
			log.Error("print update event for flashback failed", zap.Error(err))
		}
		s.writeLineSep()
	case pb.EventType_Delete:
		err := s.writeDeleteEvent(event.GetSchemaName(), event.GetTableName(), event.Row)
		if err != nil {
			log.Error("print delete event for flashback failed", zap.Error(err))
		}
		s.writeLineSep()
	}
	return nil
}

// writeInsertEvent: write insert event for flashback
func (s *flashbackSyncer) writeInsertEvent(schema string, table string, row [][]byte) error {
	sql := fmt.Sprintf("DELETE FROM %s.%s where ", schema, table)
	for i, c := range row {
		col := new(pb.Column)
		err := col.Unmarshal(c)
		if err != nil {
			return errors.Annotate(err, "unmarshal failed")
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return errors.Annotate(err, "decode row failed")
		}

		s := formatWithNull(formatValueToSqlString(val, col.Tp[0]), true)
		if i == len(row)-1 {
			sql += fmt.Sprintf("%s%s;", col.GetName(), s)
		} else {
			sql += fmt.Sprintf("%s%s AND ", col.GetName(), s)
		}
	}
	if _, err := s.destSqlFile.Write([]byte(sql)); err != nil {
		return err
	}
	return nil
}

func (s *flashbackSyncer) writeDeleteEvent(schema string, table string, row [][]byte) error {
	sql := fmt.Sprintf("INSERT INTO %s.%s(", schema, table)
	var (
		sqlColumns string
		sqlValues  string
	)
	for i, c := range row {
		col := new(pb.Column)
		err := col.Unmarshal(c)
		if err != nil {
			return errors.Annotate(err, "unmarshal failed")
		}

		if i == len(row)-1 {
			sqlColumns += col.GetName()
		} else {
			sqlColumns += col.GetName() + ", "
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return errors.Annotate(err, "decode row failed")
		}

		s := formatValueToSqlString(val, col.Tp[0])
		if i == len(row)-1 {
			sqlValues += s
		} else {
			sqlValues += s + ", "
		}
	}

	sql += fmt.Sprintf("%s) VALUES (%s);", sqlColumns, sqlValues)
	if _, err := s.destSqlFile.Write([]byte(sql)); err != nil {
		return err
	}
	return nil
}

func (s *flashbackSyncer) writeUpdateEvent(schema string, table string, row [][]byte) error {
	sql := fmt.Sprintf("UPDATE %s.%s SET ", schema, table)
	var (
		setVals         string
		whereConditions string
	)
	for i, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return errors.Annotate(err, "unmarshal failed")
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return errors.Annotate(err, "decode row failed")
		}

		_, changedVal, err := codec.DecodeOne(col.ChangedValue)
		if err != nil {
			return errors.Annotate(err, "decode row failed")
		}

		sBefore := formatWithNull(formatValueToSqlString(changedVal, col.Tp[0]), true)
		sAfter := formatWithNull(formatValueToSqlString(val, col.Tp[0]), false)
		if i == len(row)-1 {
			setVals += fmt.Sprintf("%s%s WHERE ", col.GetName(), sAfter)
			whereConditions += fmt.Sprintf("%s%s;", col.GetName(), sBefore)
		} else {
			setVals += fmt.Sprintf("%s%s, ", col.GetName(), sAfter)
			whereConditions += fmt.Sprintf("%s%s AND ", col.GetName(), sBefore)
		}
	}
	sql += setVals + whereConditions
	if _, err := s.destSqlFile.Write([]byte(sql)); err != nil {
		return err
	}
	return nil
}

// utils: writeTxnStart, writeTxnStop, writeLineSep
func (s *flashbackSyncer) writeLineSep() {
	_, _ = s.destSqlFile.Write([]byte("\n"))
}

func (s *flashbackSyncer) writeTxnStart(commitTS int64) {
	_, _ = s.destSqlFile.Write([]byte(fmt.Sprintf("Start Transaction; # %d\n", commitTS)))
}

func (s *flashbackSyncer) writeTxnStop(commitTS int64) {
	_, _ = s.destSqlFile.Write([]byte(fmt.Sprintf("Commit; # %d\n", commitTS)))
}
