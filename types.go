package easysql

//v1.1.0

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
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

//注意:日期不带时区信息
type DATE struct {
	tm     time.Time
	Valid  bool
	layout string //用于json和字符串， 序列号和反序列号显示格式
}

//szDate 为UTC时间。 layout格式为: "2006-01-02"
func NewDate(szDate string) DATE {
	obj := DATE{}
	obj.Valid = false
	obj.layout = "2006-01-02"

	if len(szDate) > 0 {
		if tm, err := time.ParseInLocation(obj.layout, szDate, time.UTC); err == nil {
			obj.Valid = true
			obj.tm = tm
		}
	}

	return obj
}

//时间字符串解析/显示 layout
func (t *DATE) SetLayout(layout string) *DATE {
	t.layout = layout
	return t
}

// Scan implements the Scanner interface.
// read data from database
func (t *DATE) Scan(value interface{}) error {
	var ss sql.NullString
	if err := ss.Scan(value); err != nil {
		return err
	}

	if !ss.Valid {
		t.Valid = false
		return nil
	}

	tm, err := parseTimeString(ss.String, time.UTC)
	if err != nil {
		return err
	}

	t.Valid = true
	t.tm = tm

	return nil
}

// Value implements the driver Valuer interface.
// save data to database
func (t DATE) Value() (driver.Value, error) {
	if !t.Valid {
		return nil, nil
	}

	return t.tm.UTC(), nil
}

func (t DATE) String() string {
	if !t.Valid {
		return ""
	}

	if len(t.layout) == 0 {
		t.layout = "2006-01-02"
	}

	return t.tm.Format(t.layout)
}

func (t DATE) MarshalJSON() ([]byte, error) {
	if t.Valid {
		return json.Marshal(t.String())
	} else {
		return json.Marshal("")
	}
}

func (t DATE) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *DATE) SetVal(szDate string) error {
	if len(t.layout) == 0 {
		t.layout = "2006-01-02"
	}

	if tm, err := time.ParseInLocation(t.layout, szDate, time.UTC); err == nil {
		t.Valid = true
		t.tm = tm
		return nil
	} else {
		return err
	}
}

func (t *DATE) SetNULL() {
	t.Valid = false
}

func (t *DATE) UnmarshalJSON(data []byte) error {
	var ss string
	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}

	if len(t.layout) == 0 {
		t.layout = "2006-01-02"
	}

	if tm, err := time.ParseInLocation(t.layout, ss, time.UTC); err == nil {
		t.Valid = true
		t.tm = tm
		return nil
	} else {
		return err
	}
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
