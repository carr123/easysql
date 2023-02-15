package cockroach

import (
	"context"
	"database/sql"
	"encoding/base64"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DBServer struct {
	db *sqlx.DB
}

type Conn struct {
	db     *sqlx.DB
	tx     *sqlx.Tx
	excter execAndQuery
	ctx    context.Context
}

type QItem map[string]interface{}
type QArray []QItem

type execAndQuery interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type TxCompatible struct {
	*sqlx.Tx
}

func (t *TxCompatible) Commit(context.Context) error {
	return t.Tx.Commit()
}

func (t *TxCompatible) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := t.Tx.ExecContext(ctx, query, args...)
	return err
}

func (t *TxCompatible) Rollback(context.Context) error {
	return t.Tx.Rollback()
}

// "postgresql://root@127.0.0.1:26257/bank?sslmode=disable"
func New(dataSourceName string, MaxIdleConn int) (*DBServer, error) {
	inst := &DBServer{}
	db, err := sqlx.Connect("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(MaxIdleConn)
	db.SetConnMaxLifetime(time.Second * 300)
	inst.db = db
	return inst, nil
}

func (this *DBServer) Close() {
	if this.db != nil {
		this.db.Close()
		this.db = nil
	}
}

func (this *DBServer) NewConn() *Conn {
	return &Conn{db: this.db, tx: nil, excter: this.db, ctx: context.Background()}
}

func (this *DBServer) ExecInTx(fn func(*Conn) error) error {
	ctx := context.Background()
	return this.ExecInTxContext(ctx, fn)
}

func (this *DBServer) ExecInTxContext(ctx context.Context, fn func(*Conn) error) error {
	tx, err := this.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	conn := &Conn{db: this.db, tx: tx, excter: tx, ctx: ctx}
	err = crdb.ExecuteInTx(ctx, &TxCompatible{tx}, func() error {
		return fn(conn)
	})

	return err
}

func (this *Conn) Context() context.Context {
	if this.ctx != nil {
		return this.ctx
	}
	return context.Background()
}

func (this *Conn) WithContext(ctx context.Context) *Conn {
	if ctx == nil {
		panic("nil context")
	}
	conn2 := &Conn{db: this.db, tx: this.tx, excter: this.excter, ctx: ctx}
	return conn2
}

func (this *Conn) Ping() error {
	return this.db.Ping()
}

// insert update delete
// create table, alter index etc.
func (this *Conn) Exec(cmd string, args ...interface{}) error {
	query, argsx, err := sqlx.In(cmd, args...)
	if err != nil {
		return err
	}

	query = this.db.Rebind(query)
	_, err = this.excter.ExecContext(this.Context(), query, argsx...)
	return err
}

// insert many records at one shot. often insert many logs.
// values := make([]interface{}, 0, batchsize*nCol)
// conn.BulkInsert("insert into files(bucket,filename)", nCol, values...)
func (this *Conn) BulkInsert(cmd string, nCol int, args ...interface{}) error {
	var szSQL string
	szBracket := "(" + strings.TrimSuffix(strings.Repeat("?,", nCol), ",") + "),"
	szSQL = cmd + " values " + strings.TrimSuffix(strings.Repeat(szBracket, len(args)/nCol), ",")
	return this.Exec(szSQL, args...)
}

// conn.BulkInsertEx("insert into msgs(fid, username, area)", nColumn, values, "ON CONFLICT(fid) DO NOTHING")
// reference: INSERT INTO ON CONFLICT(id) DO NOTHING
// reference: INSERT INTO ON CONFLICT(sn,orgid) DO UPDATE SET status=excluded.status
// reference: INSERT INTO ON CONFLICT(id) DO UPDATE SET (status,name)=(excluded.status,excluded.name)
func (this *Conn) BulkInsertEx(cmd string, nCol int, args []interface{}, szSQLsurfix ...string) error {
	var szSQL string
	szBracket := "(" + strings.TrimSuffix(strings.Repeat("?,", nCol), ",") + "),"
	szSQL = cmd + " values " + strings.TrimSuffix(strings.Repeat(szBracket, len(args)/nCol), ",")
	if len(szSQLsurfix) > 0 {
		szSQL = szSQL + " " + strings.Join(szSQLsurfix, " ")
	}

	return this.Exec(szSQL, args...)
}

func MakeQArray() QArray {
	return make(QArray, 0)
}

// query database
func (this *Conn) Query(query string, args ...interface{}) (QArray, error) {
	queryx, argsx, err := sqlx.In(query, args...)
	if err != nil {
		return nil, err
	}
	queryx = this.db.Rebind(queryx)

	rows, err := this.excter.QueryxContext(this.Context(), queryx, argsx...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(QArray, 0, 100)
	for rows.Next() {
		out := make(map[string]interface{})
		err := rows.MapScan(out)
		if err != nil {
			return nil, err
		}
		result = append(result, out)
	}

	return result, nil
}

// query database
func (this *Conn) Select(dest interface{}, query string, args ...interface{}) error {
	queryx, argsx, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	queryx = this.db.Rebind(queryx)
	return this.excter.SelectContext(this.Context(), dest, queryx, argsx...)
}

// select count(*) from ...
func (this *Conn) QueryCount(query string, args ...interface{}) (int64, error) {
	queryx, argsx, err := sqlx.In(query, args...)
	if err != nil {
		return 0, err
	}
	queryx = this.db.Rebind(queryx)

	rows, err := this.excter.QueryxContext(this.Context(), queryx, argsx...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64 = 0
	for rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return 0, err
		}
	}

	return count, nil
}

func (this *Conn) Close() error {
	return nil
}

func (item QItem) AsMap() map[string]interface{} {
	return item
}

func (item QItem) GetColumnString(key string) string {
	return item[key].(string)
}

func (item QItem) GetColumnInt64(key string) int64 {
	return item[key].(int64)
}

func (item QItem) GetColumnFloat64(key string) float64 {
	return item[key].(float64)
}

func (item QItem) ToInt64(keys ...string) QItem {
	for _, key := range keys {
		if ss, ok := item[key].(string); ok {
			item[key], _ = strconv.ParseInt(ss, 10, 64)
		}
	}
	return item
}

func (item QItem) IntToBool(keys ...string) QItem {
	for _, key := range keys {
		if ss, ok := item[key].(string); ok {
			n, _ := strconv.ParseInt(ss, 10, 64)
			if n == 0 {
				item[key] = false
			} else {
				item[key] = true
			}
		}
	}
	return item
}

func (item QItem) ToFloat64(keys ...string) QItem {
	for _, key := range keys {
		if ss, ok := item[key].(string); ok {
			item[key], _ = strconv.ParseFloat(ss, 64)
		}
	}
	return item
}

func (item QItem) Base64Encode(keys ...string) QItem {
	for _, key := range keys {
		if ss, ok := item[key].(string); ok {
			item[key] = base64.StdEncoding.EncodeToString([]byte(ss))
		}
	}
	return item
}

func (array *QArray) ToInt64(key ...string) *QArray {
	for _, item := range *array {
		item.ToInt64(key...)
	}
	return array
}

func (array *QArray) Shuffle() *QArray {
	nLen := len(*array)

	if nLen < 2 {
		return array
	}

	rand.Seed(time.Now().UnixNano())
	dest := make([]QItem, nLen)
	perm := rand.Perm(nLen)
	for i, v := range perm {
		dest[v] = (*array)[i]
	}

	*array = dest

	return array
}

func (array *QArray) IntToBool(key ...string) *QArray {
	for _, item := range *array {
		item.IntToBool(key...)
	}
	return array
}

func (array *QArray) ToFloat64(key ...string) *QArray {
	for _, item := range *array {
		item.ToFloat64(key...)
	}
	return array
}

func (array *QArray) Base64Encode(key ...string) *QArray {
	for _, item := range *array {
		item.Base64Encode(key...)
	}
	return array
}

func (array *QArray) ToRawArray() []interface{} {
	arr := make([]interface{}, 0, len(*array))

	for _, item := range *array {
		arr = append(arr, map[string]interface{}(item))
	}

	return arr
}
