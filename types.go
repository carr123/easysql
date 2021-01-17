package easysql

//v1.0.0

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"

	"strings"
	"time"

	"github.com/lib/pq"
)

/*
type STRING = easysql.STRING
type INT64 = easysql.INT64
type DATE = easysql.DATE
type DATETIME = easysql.DATETIME
type FLOAT64 = easysql.FLOAT64
type BOOL = easysql.BOOL
type JSONB = easysql.JSONB
type StringArray = easysql.StringArray
type Int64Array = easysql.Int64Array
*/

//定义的数据类型能够接收数据库的空字段。空字段被解析为默认类型。比如 "", 0, false
//------------------------------------------------------------------------------

//日期类型为字符串，格式为: 2006-01-02
//空字段解析为空字符串
type DATE struct {
	sql.NullString
}

func NewDate(szDate string) DATE {
	obj := DATE{}
	obj.NullString.Valid = false
	if len(szDate) > 0 {
		if _, err := time.Parse("2006-01-02", szDate); err == nil {
			obj.NullString.String = szDate
			obj.NullString.Valid = true
		}
	}
	return obj
}

// Scan implements the Scanner interface.
func (t *DATE) Scan(value interface{}) error {
	if err := t.NullString.Scan(value); err != nil {
		return err
	}

	if t.NullString.Valid {
		exp := regexp.MustCompile(`[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])`)
		ss := exp.FindString(t.NullString.String)
		if len(ss) < 10 {
			return errors.New("date format error:" + t.NullString.String)
		}

		t.NullString.String = ss
	}

	return nil
}

// Value implements the driver Valuer interface.
func (t DATE) Value() (driver.Value, error) {
	if !t.NullString.Valid {
		return nil, nil
	}
	return t.NullString.String, nil
}

func (t DATE) MarshalJSON() ([]byte, error) {
	if t.NullString.Valid {
		return json.Marshal(t.NullString.String)
	} else {
		return json.Marshal("")
	}
}

func (t DATE) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t DATE) String() string {
	if t.NullString.Valid {
		return t.NullString.String
	} else {
		return ""
	}
}

//格式为: "2006-01-02"
func (t *DATE) SetVal(szDate string) error {
	if len(szDate) > 0 {
		if _, err := time.Parse("2006-01-02", szDate); err == nil {
			t.NullString.String = szDate
			t.NullString.Valid = true
			return nil
		} else {
			return err
		}
	}

	return nil
}

func (t *DATE) SetNULL() {
	t.NullString.String = ""
	t.NullString.Valid = false
}

func (t *DATE) UnmarshalJSON(data []byte) error {
	var ss string
	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}

	tt, err := time.Parse("2006-01-02", ss)
	if err != nil {
		return err
	}

	t.NullString.Valid = true
	t.NullString.String = tt.Format("2006-01-02")

	return nil
}

//------------------------------------------------------------------------------
//时间日期为字符串，格式为: "2006-01-02 15:04:05"
//空字段解析为空字符串
type DATETIME struct {
	sql.NullString
}

func NewDateTime(szDate string) DATETIME {
	obj := DATETIME{}
	obj.NullString.Valid = false

	if len(szDate) > 0 {
		if _, err := time.Parse("2006-01-02 15:04:05", szDate); err == nil {
			obj.NullString.String = szDate
			obj.NullString.Valid = true
		}
	}

	return obj
}

// Scan implements the Scanner interface.
func (t *DATETIME) Scan(value interface{}) error {
	if err := t.NullString.Scan(value); err != nil {
		return err
	}

	if t.NullString.Valid {
		ss := strings.ReplaceAll(t.NullString.String, "T", " ")
		ss = strings.ReplaceAll(ss, "Z", "")

		exp := regexp.MustCompile(`[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\s+(20|21|22|23|[0-1]\d):[0-5]\d:[0-5]\d`)
		ss = exp.FindString(ss)
		if len(ss) < 19 {
			return errors.New("datetime format error:" + t.NullString.String)
		}

		t.NullString.String = ss
	}
	return nil
}

// Value implements the driver Valuer interface.
func (t DATETIME) Value() (driver.Value, error) {
	if !t.NullString.Valid {
		return nil, nil
	}
	return t.NullString.String, nil
}

func (t DATETIME) MarshalJSON() ([]byte, error) {
	if t.NullString.Valid {
		return json.Marshal(t.NullString.String)
	} else {
		return json.Marshal("")
	}
}

func (t DATETIME) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t DATETIME) String() string {
	if t.NullString.Valid {
		return t.NullString.String
	} else {
		return ""
	}
}

func (t *DATETIME) UnmarshalJSON(data []byte) error {
	var ss string
	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}

	tt, err := time.Parse("2006-01-02 15:04:05", ss)
	if err != nil {
		return err
	}

	t.NullString.Valid = true
	t.NullString.String = tt.Format("2006-01-02 15:04:05")

	return nil
}

//格式为: "2006-01-02 15:04:05"
func (t *DATETIME) SetVal(szDateTime string) error {
	if len(szDateTime) > 0 {
		if _, err := time.Parse("2006-01-02 15:04:05", szDateTime); err == nil {
			t.NullString.String = szDateTime
			t.NullString.Valid = true
			return nil
		} else {
			return err
		}
	}

	return nil
}

func (t *DATETIME) SetNULL() {
	t.NullString.String = ""
	t.NullString.Valid = false
}

//------------------------------------------------------------------------------
//空字段解析为空字符串
type STRING struct {
	sql.NullString
}

// Scan implements the Scanner interface.
func (t *STRING) Scan(value interface{}) error {
	return t.NullString.Scan(value)
}

// Value implements the driver Valuer interface.
func (t STRING) Value() (driver.Value, error) {
	return t.NullString.Value()
}

func (t STRING) MarshalJSON() ([]byte, error) {
	if t.NullString.Valid {
		return json.Marshal(t.NullString.String)
	} else {
		return json.Marshal("")
	}
}

func (t *STRING) UnmarshalJSON(data []byte) error {
	var ss string
	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}

	t.NullString.Valid = true
	t.NullString.String = ss

	return nil
}

func (t STRING) String() string {
	if t.NullString.Valid {
		return t.NullString.String
	} else {
		return ""
	}
}

func (t *STRING) SetVal(szVal string) error {
	t.NullString.String = szVal
	t.NullString.Valid = true
	return nil
}

func (t *STRING) SetNULL() {
	t.NullString.String = ""
	t.NullString.Valid = false
}

//------------------------------------------------------------------------------
//空字段会解析为0
type INT64 struct {
	sql.NullInt64
}

// Scan implements the Scanner interface.
func (t *INT64) Scan(value interface{}) error {
	return t.NullInt64.Scan(value)
}

// Value implements the driver Valuer interface.
func (t INT64) Value() (driver.Value, error) {
	return t.NullInt64.Value()
}

func (t INT64) MarshalJSON() ([]byte, error) {
	if t.NullInt64.Valid {
		return json.Marshal(t.NullInt64.Int64)
	} else {
		return json.Marshal(0)
	}
}

func (t *INT64) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var nNum int64
	if err := json.Unmarshal(data, &nNum); err != nil {
		return err
	}

	t.NullInt64.Valid = true
	t.NullInt64.Int64 = nNum

	return nil
}

func (t *INT64) SetVal(val int64) error {
	t.NullInt64.Int64 = val
	t.NullInt64.Valid = true
	return nil
}

func (t *INT64) SetNULL() {
	t.NullInt64.Int64 = 0
	t.NullInt64.Valid = false
}

//------------------------------------------------------------------------------
//空字段会解析为0
type FLOAT64 struct {
	sql.NullFloat64
}

// Scan implements the Scanner interface.
func (t *FLOAT64) Scan(value interface{}) error {
	return t.NullFloat64.Scan(value)
}

// Value implements the driver Valuer interface.
func (t FLOAT64) Value() (driver.Value, error) {
	return t.NullFloat64.Value()
}

func (t FLOAT64) MarshalJSON() ([]byte, error) {
	if t.NullFloat64.Valid {
		return json.Marshal(t.NullFloat64.Float64)
	} else {
		return json.Marshal(0)
	}
}

func (t *FLOAT64) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var nNum float64
	if err := json.Unmarshal(data, &nNum); err != nil {
		return err
	}

	t.NullFloat64.Valid = true
	t.NullFloat64.Float64 = nNum

	return nil
}

func (t *FLOAT64) SetVal(val float64) error {
	t.NullFloat64.Float64 = val
	t.NullFloat64.Valid = true
	return nil
}

func (t *FLOAT64) SetNULL() {
	t.NullFloat64.Float64 = 0
	t.NullFloat64.Valid = false
}

//------------------------------------------------------------------------------
//空字段会解析为 false
type BOOL struct {
	sql.NullBool
}

// Scan implements the Scanner interface.
func (t *BOOL) Scan(value interface{}) error {
	return t.NullBool.Scan(value)
}

// Value implements the driver Valuer interface.
func (t BOOL) Value() (driver.Value, error) {
	return t.NullBool.Value()
}

func (t BOOL) MarshalJSON() ([]byte, error) {
	if t.NullBool.Valid {
		return json.Marshal(t.NullBool.Bool)
	} else {
		return json.Marshal(false)
	}
}

func (t *BOOL) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var val bool
	if err := json.Unmarshal(data, &val); err != nil {
		return err
	}

	t.NullBool.Valid = true
	t.NullBool.Bool = val
	return nil
}

func (t *BOOL) SetVal(val bool) error {
	t.NullBool.Bool = val
	t.NullBool.Valid = true
	return nil
}

func (t *BOOL) SetNULL() {
	t.NullBool.Bool = false
	t.NullBool.Valid = false
}

//------------------------------------------------------------------------------
//JSONB格式，数据库字段格式为: "{}", 不能为json数组等其他格式
//空字段解析为空map
type JSONB struct {
	KV map[string]interface{}
}

// Scan implements the Scanner interface.
func (t *JSONB) Scan(value interface{}) error {
	var ss sql.NullString

	if err := ss.Scan(value); err != nil {
		return err
	}

	if ss.Valid {
		d := json.NewDecoder(strings.NewReader(ss.String))
		d.UseNumber()
		if err := d.Decode(&t.KV); err != nil {
			return err
		}
		return nil

	} else {
		t.KV = make(map[string]interface{})
	}

	return nil
}

// Value implements the driver Valuer interface.
func (t JSONB) Value() (driver.Value, error) {
	if t.KV == nil {
		return nil, nil
	}

	bin, err := json.Marshal(t.KV)
	if err != nil {
		return nil, err
	}

	return string(bin), nil
}

func (t JSONB) MarshalJSON() ([]byte, error) {
	if t.KV == nil {
		return json.Marshal(map[string]interface{}{})
	} else {
		return json.Marshal(t.KV)
	}
}

func (t JSONB) MarshalText() ([]byte, error) {
	return t.MarshalJSON()
}

func (t JSONB) String() string {
	if bin, err := t.MarshalJSON(); err == nil {
		return string(bin)
	} else {
		return "{}"
	}
}

func (t *JSONB) UnmarshalJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	return d.Decode(&t.KV)
}

func (t *JSONB) Merge(mp map[string]interface{}) *JSONB {
	for k, v := range mp {
		t.KV[k] = v
	}

	return t
}

func (t *JSONB) SetVal(mp map[string]interface{}) error {
	t.KV = mp
	return nil
}

func (t *JSONB) SetNULL() {
	t.KV = nil
}

type StringArray = pq.StringArray
type Int64Array = pq.Int64Array
type Float64Array = pq.Float64Array
type ByteArray = pq.ByteaArray
