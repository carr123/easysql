# easysql
this's a golang library for mysql/postgresql/cockroachdb.
it's easy to switch backend dbserver between mysql, postgresql and cockroachdb with little change in your golang code.

features:
1. you should write SQL clause to talk to backend db server.
2. data type binding between database and golang type. string, int, float, date, datetime, string[], int[], jsonb, etc.
3. support transfer to default value if one db column is NULL


package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/carr123/easysql"

	dbserver "github.com/carr123/easysql/mysql"
 // dbserver "github.com/carr123/easysql/cockroach"
 // dbserver "github.com/carr123/easysql/postgre"
)

var (
	db *dbserver.DBServer
)

type STRING = easysql.STRING
type INT64 = easysql.INT64
type DATE = easysql.DATE
type DATETIME = easysql.DATETIME
type FLOAT64 = easysql.FLOAT64
type BOOL = easysql.BOOL

func main() {
	var err error
	db, err = dbserver.New("root:123456@tcp(127.0.0.1:3306)/eshop?charset=utf8", 10)
	if err != nil {
		fmt.Println("err:", err)
		return
	}  
}

func demo1() {
	conn := db.NewConn()
	defer conn.Close()

	if err := conn.Exec("insert into users(userid,username)values(?,?)", 1, "user1"); err != nil {
		fmt.Println("err:", err)
		return
	}

	if err := conn.Exec("delete from users where username=?", "wang"); err != nil {
		fmt.Println("err:", err)
		return
	}

	if err := conn.Exec("update users set age=? where userid=?", 20, 1); err != nil {
		fmt.Println("err:", err)
		return
	}
}

func demo2() {
	conn := db.NewConn()
	defer conn.Close()

	if true {
		//如果只查询一个字段，返回一个数组. 可以这样写
		var out []int64
		err := conn.Select(&out, "select userid from users")
		if err != nil {
			fmt.Println("err:", err)
			return
		}
		fmt.Println(out)
	}

	if true {
		//查询结果放到结构体数组中
		var out []struct {
			UserID   int64    `db:"userid" json:"id"`
			UserName STRING   `db:"username" json:"name"`
			Age      INT64    `db:"age" json:"age"`
			Birthday DATE     `db:"birthday" json:"birthday"`
			Createat DATETIME `db:"createat" json:"createat"`
			Income   FLOAT64  `db:"income" json:"income"`
			Male     BOOL     `db:"male" json:"male"`
		}
		if err := conn.Select(&out, "select userid,username,birthday,age,createat,income,male from users where userid in (?)", []int64{1, 2, 3}); err != nil {
			fmt.Println("err:", err)
			return
		}  
	}
  
 func demo3() {
  conn := db.NewConn()
	defer conn.Close()

	t1 := time.Now()
	values := make([]interface{}, 0, 20)
	for k := 0; k < 1000; k++ {
		values = append(values, 10+k)
		values = append(values, fmt.Sprintf("user%d", 10+k))
	}

	if err := conn.BulkInsert("insert into users(userid,username)", 2, values); err != nil {
		log.Println(err)
		return
	}
	t2 := time.Now()
	fmt.Println("time taken:", t2.Sub(t1))
}

func TransactionDemo() {
	//演示事务。2个协程去更新同一个字段.
	//mysql的默认事务隔离级别是repeatable read,读的快照.	要加上for update 行锁，否则读取的字段可能是被其他事务修改过了。
	//cockroach不存在这个问题.加不加for update都不影响cockroach的SERIALIZABLE事务属性.
	conn := db.NewConn()
	defer conn.Close()
	if err := conn.Exec("update users set age=20 where userid=?", 1); err != nil {
		log.Println(err)
		return
	}

	//下面演示2个协程操作同一条记录
	go func() {
		err := db.ExecInTx(func(conn *dbserver.Conn) error {
			log.Println("tx start")
			v, err := conn.Query("select userid,age from users where userid=? for update", 1)
			if err != nil {
				return err
			}
			bin, _ := json.Marshal(v)
			log.Println("in tx:", string(bin))

			time.Sleep(time.Second * 4)
			if err := conn.Exec("update users set age=age+1 where userid=?", 1); err != nil {
				return err
			}

			if v, err := conn.Query("select userid,age from users where userid=?", 1); err != nil {
				return err
			} else {
				bin, _ := json.Marshal(v)
				log.Println("in tx:", string(bin))
			}

			return nil
		})
		if err != nil {
			log.Println("tx failed:", err)
		}
	}()

	log.Println("update begin")
	if err := conn.Exec("update users set age=age+1 where userid=?", 1); err != nil {
		log.Println("in tx:", err)
		return
	}
	log.Println("update finish")

	time.Sleep(time.Second * 10)
}





