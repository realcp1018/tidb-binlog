// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"encoding/hex"
	"fmt"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"strconv"
	"strings"
)

// formatValueToString: for print syncer
func formatValueToString(data types.Datum, tp byte) string {
	val := data.GetValue()
	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeNewDecimal, mysql.TypeVarchar, mysql.TypeString, mysql.TypeJSON:
		if val != nil {
			return fmt.Sprintf("%s", val)
		}
		fallthrough
	default:
		return fmt.Sprintf("%v", val)
	}
}

// formatValueToSqlString: for sql&flashback syncer
/*
Some Types Conversion:
	TypeDuration 	=> time
	TypeString		=> binary|char
	// for binary:	if len(value) < binary fixed size, mysql will right pad null(ascii code 0 or \0 or caret notation ^@)
	// for char: 	if len(value) < char fixed size, mysql will right pad/trim space(ascii code 32) automatically
	// Remember to use --binary-mode for binary columns
	TypeVarString 	=> varbinary
	TypeBlob		=> blob|text
	TypeTinyBlob	=> tinyblob|tinytext
	TypeMediumBlob	=> mediumblob|mediumtext
	TypeLongBlob	=> longblob|longtext
	TypeNewDecimal  => decimal
	TypeEnum,TypeSet can be used as number
	Others see: https://github.com/pingcap/tidb/blob/master/parser/mysql/type.go
*/
func formatValueToSqlString(data types.Datum, tp byte) string {
	val := data.GetValue()
	if val == nil {
		return "null"
	}
	switch tp {
	// Numeric data types
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24, mysql.TypeLonglong, mysql.TypeFloat, mysql.TypeDouble:
		return fmt.Sprintf("%v", val)
	case mysql.TypeBit:
		return data.GetMysqlBit().ToBitLiteralString(true)
	case mysql.TypeNewDecimal:
		return data.GetMysqlDecimal().String()
	// String data types
	case mysql.TypeVarchar:
		return quote(escape(data.GetString()))
	case mysql.TypeString:
		return quote(escape(string(val.([]byte))))
	case mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeLongBlob, mysql.TypeMediumBlob:
		return "0x" + hex.EncodeToString(val.([]byte))
	case mysql.TypeEnum:
		return strconv.FormatUint(data.GetMysqlEnum().Value, 10)
	case mysql.TypeSet:
		return strconv.FormatUint(data.GetMysqlSet().Value, 10)
	// Date and Time
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeNewDate:
		return quote(string(val.([]byte)))
	case mysql.TypeYear:
		return quote(strconv.FormatUint(data.GetUint64(), 10))
	case mysql.TypeDuration:
		return quote(data.GetMysqlDuration().String())
	// Json type
	case mysql.TypeJSON:
		return quote(data.GetMysqlJSON().String())
	default:
		// todo：unsupported types like mysql.TypeGeometry...
		panic(fmt.Sprintf("unsupported col type %x", tp))
	}
}

func escape(s string) string {
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "'", "\\'", -1)
	return s
}

// quote add single-quote around str
func quote(str string) string {
	return fmt.Sprintf("'%s'", str)
}

func formatValue(value types.Datum, tp byte) types.Datum {
	if value.GetValue() == nil {
		return value
	}

	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeNewDecimal, mysql.TypeVarchar, mysql.TypeString, mysql.TypeJSON:
		value = types.NewDatum(fmt.Sprintf("%s", value.GetValue()))
	case mysql.TypeEnum:
		value = types.NewDatum(value.GetMysqlEnum().Value)
	case mysql.TypeSet:
		value = types.NewDatum(value.GetMysqlSet().Value)
	case mysql.TypeBit:
		// see drainer/translator/mysql.go formatData
		value = types.NewDatum(value.GetUint64())
	}

	return value
}
