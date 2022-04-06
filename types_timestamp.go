package easysql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type DATETIME struct {
	tm     time.Time //带时间和时区信息. 不赋值时默认为UTC时区
	Valid  bool
	layout string //用于json和字符串， 序列号和反序列号显示格式
}

//szDate 为UTC时间。 layout格式为: "2006-01-02 15:04:05"
func NewDateTimeUTC(szDate string) DATETIME {
	obj := DATETIME{}
	obj.Valid = false
	obj.layout = "2006-01-02 15:04:05"

	if len(szDate) > 0 {
		tm, err := time.ParseInLocation(obj.layout, szDate, time.UTC)
		if err == nil {
			obj.Valid = true
			obj.tm = tm
		}
	}

	return obj
}

//szDate为本地时区时间。 layout格式为: "2006-01-02 15:04:05"
func NewDateTimeLocal(szDate string) DATETIME {
	obj := DATETIME{}
	obj.Valid = false
	obj.layout = "2006-01-02 15:04:05"

	if len(szDate) > 0 {
		tm, err := time.ParseInLocation(obj.layout, szDate, time.Local)
		if err == nil {
			obj.Valid = true
			obj.tm = tm
		}
	}

	return obj
}

//layout默认为: "2006-01-02 15:04:05", 时区为tm所在时区
func NewDateTime(tm time.Time) DATETIME {
	obj := DATETIME{}
	obj.Valid = false
	obj.layout = "2006-01-02 15:04:05"

	if !tm.IsZero() {
		obj.Valid = true
		obj.tm = tm
	}

	return obj
}

//设置时区
//影响MarshalJSON(), MarshalText(), String(), UnmarshalJSON() 显示
func (t *DATETIME) SetTimezone(tz *time.Location) *DATETIME {
	t.tm = t.tm.In(tz)
	return t
}

//时间字符串解析/显示 layout
func (t *DATETIME) SetLayout(layout string) *DATETIME {
	t.layout = layout
	return t
}

// Scan implements the sql.Scanner interface.
// read data from database
func (t *DATETIME) Scan(value interface{}) error {
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
	t.tm = tm.In(t.tm.Location())

	return nil
}

//Value implements the driver.Valuer interface.
//save data to database
func (t DATETIME) Value() (driver.Value, error) {
	if !t.Valid {
		return nil, nil
	}

	return t.tm.UTC(), nil
}

func (t DATETIME) String() string {
	if !t.Valid {
		return ""
	}

	if len(t.layout) == 0 {
		t.layout = "2006-01-02 15:04:05"
	}

	return t.tm.Format(t.layout)
}

func (t DATETIME) MarshalJSON() ([]byte, error) {
	if t.Valid {
		return json.Marshal(t.String())
	} else {
		return json.Marshal("")
	}
}

func (t DATETIME) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t DATETIME) ToTime() time.Time {
	return t.tm
}

func (t *DATETIME) UnmarshalJSON(data []byte) error {
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

	tt, err := time.ParseInLocation(t.layout, ss, t.tm.Location())
	if err != nil {
		return err
	}

	t.Valid = true
	t.tm = tt

	return nil
}

func (t *DATETIME) SetVal(tm time.Time) *DATETIME {
	if !tm.IsZero() {
		t.Valid = true
		t.tm = tm
	} else {
		t.Valid = false
	}

	return t
}

func (t *DATETIME) SetNULL() *DATETIME {
	t.Valid = false
	return t
}

func parseTimeString(szDateTime string, loc *time.Location) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		time.RFC822,
		time.RFC822Z,
		time.RFC850,
		time.RFC1123,
		time.RFC1123Z,
		time.Kitchen,
	}

	for _, layout := range layouts {
		tm, err := time.ParseInLocation(layout, szDateTime, loc)
		if err == nil {
			return tm, nil
		}
	}

	return time.Time{}, fmt.Errorf("time parse fail:%s", szDateTime)
}
