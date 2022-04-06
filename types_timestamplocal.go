package easysql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"
)

type DATETIME_LOCAL struct {
	tm     time.Time //带时间和时区信息. 不赋值时默认为UTC时区
	Valid  bool
	layout string //用于json和字符串， 序列号和反序列号显示格式
}

//时间字符串解析/显示 layout
func (t *DATETIME_LOCAL) SetLayout(layout string) {
	t.layout = layout
}

// Scan implements the sql.Scanner interface.
// read data from database
func (t *DATETIME_LOCAL) Scan(value interface{}) error {
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
	t.tm = tm.Local()

	return nil
}

//Value implements the driver.Valuer interface.
//save data to database
func (t DATETIME_LOCAL) Value() (driver.Value, error) {
	if !t.Valid {
		return nil, nil
	}

	return t.tm.UTC(), nil
}

func (t DATETIME_LOCAL) String() string {
	if !t.Valid {
		return ""
	}

	if len(t.layout) == 0 {
		t.layout = "2006-01-02 15:04:05"
	}

	return t.tm.Format(t.layout)
}

func (t DATETIME_LOCAL) MarshalJSON() ([]byte, error) {
	if t.Valid {
		return json.Marshal(t.String())
	} else {
		return json.Marshal("")
	}
}

func (t DATETIME_LOCAL) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t DATETIME_LOCAL) ToTime() time.Time {
	return t.tm
}

func (t *DATETIME_LOCAL) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		t.Valid = false
		return nil
	}

	var ss string
	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}

	if len(t.layout) == 0 {
		t.layout = "2006-01-02 15:04:05"
	}

	tt, err := time.ParseInLocation(t.layout, ss, time.Local)
	if err != nil {
		return err
	}

	t.Valid = true
	t.tm = tt

	return nil
}

func (t *DATETIME_LOCAL) SetVal(tm time.Time) *DATETIME_LOCAL {
	if !tm.IsZero() {
		t.Valid = true
		t.tm = tm.Local()
	} else {
		t.Valid = false
	}

	return t
}

func (t *DATETIME_LOCAL) SetNULL() *DATETIME_LOCAL {
	t.Valid = false
	return t
}
