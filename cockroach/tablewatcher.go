package cockroach

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type TableWatcher struct {
	db             *sqlx.DB
	dataSourceName string
}

func NewTableWatcher(dataSourceName string) (*TableWatcher, error) {
	inst := &TableWatcher{}
	inst.dataSourceName = dataSourceName
	return inst, nil
}

func (this *TableWatcher) Close() error {
	if this.db != nil {
		err := this.db.Close()
		this.db = nil
		return err
	}
	return nil
}

func (this *TableWatcher) WatchTables(tables []string, tmBegin time.Time, key_only bool, callback func(tbl string, keys string, records string)) error {
	if len(tables) == 0 {
		return fmt.Errorf("table empty")
	}

	defer this.Close()

	db, err := sqlx.Connect("postgres", this.dataSourceName)
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(0)
	this.db = db

	ctx := context.Background()
	if _, err = this.db.ExecContext(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true`); err != nil {
		return err
	}

	tbNames := strings.Join(tables, ",")
	cursor := fmt.Sprintf("%.10f", float64(tmBegin.UnixNano()))
	envelope := "row"
	if key_only {
		envelope = "key_only"
	}

	query := fmt.Sprintf(`EXPERIMENTAL CHANGEFEED FOR %s WITH cursor='%s',envelope=%s`, tbNames, cursor, envelope)
	rows, err := this.db.QueryxContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ev struct {
			Table  string `db:"table"`
			Key    string `db:"key"`
			Record string `db:"value"`
		}

		err := rows.StructScan(&ev)
		if err != nil {
			return err
		}

		if callback != nil {
			callback(ev.Table, ev.Key, ev.Record)
		}
	}

	return nil
}

// func toJson(obj interface{}) string {
// 	bin, _ := json.Marshal(obj)
// 	return string(bin)
// }

/*
func WatchRowChange() {
	watcher, err := dbserver.NewTableWatcher("postgresql://root:12345@localhost:26257/bank")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer watcher.Close()

	notify := func(tb string, key string, val string) {
		fmt.Println("table:", tb, " key:", key, " val:", val)
	}

	for {
		log.Println("begin wait event")
		if err := watcher.WatchTables([]string{"idused"}, time.Now().Add(-time.Hour*0), false, notify); err != nil {
			fmt.Println("wait fail:", err)
			time.Sleep(time.Second)
			continue
		}
	}

	log.Println("exit !!!")
}
*/
