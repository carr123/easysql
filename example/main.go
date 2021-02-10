package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/carr123/easysql"
	dbserver "github.com/carr123/easysql/cockroach"
	//dbserver "github.com/carr123/easysql/mysql"
	//dbserver "github.com/carr123/easysql/postgre"
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
type JSONB = easysql.JSONB
type StringArray = easysql.StringArray
type Int64Array = easysql.Int64Array
type BoolArray = easysql.BoolArray

func main() {
	var err error
	db, err = dbserver.New("root:123456@tcp(127.0.0.1:3306)/bank?charset=utf8", 10)
	//db, err = dbserver.New("postgresql://root@127.0.0.1:26257/bank?sslmode=disable", 10)
	if err != nil {
		fmt.Println("err:", err)
		return
	}
}

func demo1() {
	//基本的增删改
	conn := db.NewConn()
	defer conn.Close()

	if err := conn.Exec("insert into accounts(userid,username)values(?,?)", 5, "user5"); err != nil {
		fmt.Println("err:", err)
		return
	}

	if err := conn.Exec("delete from accounts where username=?", "wang"); err != nil {
		fmt.Println("err:", err)
		return
	}

	if err := conn.Exec("update accounts set age=? where userid=?", 20, 1); err != nil {
		fmt.Println("err:", err)
		return
	}
}

func demo2() {
	conn := db.NewConn()
	defer conn.Close()

	if false {
		//in 后面的数组不能为空数组,否则程序报错
		//返回的是 []map[string]interface{}, 注意返回的字段类型可能和数据库字段类型不对应
		v, err := conn.Query("select * from accounts where userid in (?)", []int{1, 2, 3})
		if err != nil {
			fmt.Println("err:", err)
			return
		}
		bin, _ := json.Marshal(v)
		fmt.Println(string(bin))
	}

	if false {
		//如果只查询一个字段，返回一个数组. 可以这样写
		var out []int64
		err := conn.Select(&out, "select userid from accounts")
		if err != nil {
			fmt.Println("err:", err)
			return
		}
		fmt.Println(out)
	}

	if false {
		//查询结果放到结构体数组中
		//注意数据库的空字段，可以解析到结构体失败
		//easysql.STRING能把空字段解析为空字符串
		//easysql.DATE不但能解析空字段，还能把时分秒截去保留年月日
		var out []struct {
			UserID   int64    `db:"userid" json:"id"`
			UserName STRING   `db:"username" json:"name"`
			Birthday DATE     `db:"birthday" json:"birthday"`
			Createat DATETIME `db:"createat" json:"createat"`
			Age      INT64    `db:"age" json:"age"`
			Income   FLOAT64  `db:"income" json:"income"`
			Male     BOOL     `db:"male" json:"male"`
		}
		if err := conn.Select(&out, "select userid,username,birthday,age,income,createat,male from accounts where userid in (?)", []int64{1, 2, 3, 5}); err != nil {
			fmt.Println("err:", err)
			return
		}

		bin, _ := json.Marshal(out)
		fmt.Println("output:", string(bin))
	}

	if false {
		//查询数量用QueryCount更方便
		nNum, err := conn.QueryCount("select count(*) from accounts")
		if err != nil {
			fmt.Println("err:", err)
			return
		}
		fmt.Println(nNum)
	}

	if false {
		//查询数组字段
		//数组下标从1开始. 数组字段不支持嵌套数组.
		//以下查询数组的第一个元素
		if out, err := conn.Query("select userid,hobby[1] from accounts"); err != nil {
			fmt.Println("err:", err)
			return
		} else {
			bin, _ := json.Marshal(out)
			fmt.Println(string(bin))
		}

		//修改数组字段: UPDATE accounts SET hobby = array_append(hobby, 'eat') WHERE userid=1;
	}
}

func InsertMulti() {
	//批量插入数据. 通常用在一次插入大量日志记录的场景
	batchsize := 1000
	nColumn := 2

	conn := db.NewConn()
	defer conn.Close()

	t1 := time.Now()
	values := make([]interface{}, 0, batchsize*nColumn)
	for k := 0; k < batchsize; k++ {
		values = append(values, 10+k)
		values = append(values, fmt.Sprintf("user%d", 10+k))
	}

	if err := conn.BulkInsert("insert into accounts(userid,username)", nColumn, values...); err != nil {
		log.Println(err)
		return
	}
	t2 := time.Now()
	fmt.Println("time taken:", t2.Sub(t1))
}

func TransactionDemo() {
	//演示事务。2个协程去更新同一个字段.
	//默认的事务隔离级别是SERIALIZABLE，也就是看上去多个事务仿佛是串行执行。实际是并发执行带冲突重试.
	//事务冲突时候 ExecInTx函数会自动重试。
	//select for update 是20.1.6版本新特性，加行锁，避免了事务重试，提高性能。即使不加for update也不会导致业务逻辑出错，只是增加了事务冲突重试的代价。

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := db.ExecInTx(func(conn *dbserver.Conn) error {
			log.Println("tx start")
			v, err := conn.Query("select userid,address,birthday,createat,age from accounts where address=? for update", "suzhou")
			if err != nil {
				return err
			}
			bin, _ := json.Marshal(v)
			log.Println("in tx:", string(bin))

			time.Sleep(time.Second * 4)
			if err := conn.Exec("update accounts set age=age+1 where userid=1"); err != nil {
				return err
			}

			if v, err := conn.Query("select userid,address,age from accounts where userid=1"); err != nil {
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

	time.Sleep(time.Second)

	log.Println("update begin")
	conn := db.NewConn()
	defer conn.Close()

	if v, err := conn.Query("select userid,address,age from accounts where userid=1"); err != nil {
		fmt.Println("error:", err)
		return
	} else {
		bin, _ := json.Marshal(v)
		log.Println("before update 2:", string(bin))
	}

	if err := conn.Exec("update accounts set age=age+1 where userid=1"); err != nil {
		log.Println("in tx:", err)
		return
	}
	log.Println("update finish")

	wg.Wait()
}

func JsonDemo() {
	//json字段
	conn := db.NewConn()
	defer conn.Close()

	err := conn.Exec("insert into accounts(userid,posts) values (?,?)", 7, `{"schedule":{"date":"2015-10-16","times":10},"title":"food"}`)
	if err != nil {
		fmt.Println("err:", err)
		return
	}

	//查询json字段
	if out, err := conn.Query("select userid,posts->'schedule'->>'times' as times from accounts"); err != nil {
		fmt.Println("err:", err)
		return
	} else {
		out.ToInt64("times")
		bin, _ := json.Marshal(out)
		fmt.Println(string(bin))
	}

	var info []struct {
		UserID INT64 `db:"userid" json:"userid"`
		Posts  JSONB `db:"posts" json:"posts"`
	}

	if err := conn.Select(&info, "select userid,posts from accounts"); err != nil {
		fmt.Println(err)
	}

	//更新jsonb 字段，userinfo是json字段, address是key
	//也可以把json字段更新为json对象(传入json字符串即可)
	err := conn.Exec(`update aaa set userinfo=jsonb_set(userinfo, '{address}', ?) where userid=1`, "shanghai")
}

func arraydemo() {
	conn := db.NewConn()
	defer conn.Close()

	err := conn.Exec("create table if not exists arraydemo(id int, names string[], ages int[])")
	if err != nil {
		fmt.Println("err:", err)
		return
	}

	name := []string{"u1", "u2"}
	age := []int64{14, 22}
	if err := conn.Exec("insert into arraydemo(id,names,ages)values(?,?,?)", 1, StringArray(name), Int64Array(age)); err != nil {
		fmt.Println("err:", err)
		return
	}

	var info []struct {
		ID   INT64       `db:"id" json:"id"`
		Name StringArray `db:"names" json:"names"`
		Age  Int64Array  `db:"ages" json:"ages"`
	}

	//检索ages 字段中含有 [22,35]的记录
	//数组字段可以建立倒排索引: CREATE INVERTED INDEX on arraydemo(ages);
	if err := conn.Select(&info, "select id,names,ages from arraydemo where ages @> ?", Int64Array([]int64{22, 35})); err != nil {
		fmt.Println(err)
	}
}
