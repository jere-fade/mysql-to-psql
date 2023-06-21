package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	rep "mysql-to-psql/replication"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

func main() {
	cfg := rep.Config{
		Mysql_Host:   "127.0.0.1",
		Mysql_Port:   3306,
		Pq_Host:      "127.0.0.1",
		Pq_port:      5432,
		Pq_defaultDB: "postgres",
	}

	rand.Seed(time.Now().UnixNano())

	performance := make(map[int]int64)
	performance[100] = 0
	performance[1000] = 0
	performance[10000] = 0
	performance[100000] = 0

	mysql_cfg := mysql.Config{
		User:   "root",
		Passwd: "k4aTJCcB4j=+",
		Net:    "tcp",
		Addr:   "127.0.0.1:3306",
		DBName: "homework",
	}

	var err error
	db, err := sql.Open("mysql", mysql_cfg.FormatDSN())
	if err != nil {
		panic(err)
	}

	var score float64
	var id int
	for k := range performance {
		for i := 0; i < k; i++ {
			id = 100238 + rand.Intn(100000)
			score = rand.Float64() * 100
			score = math.Round(score*100) / 100
			sql := fmt.Sprintf("UPDATE student set score = %f where id=%d", score, id)
			_, err = db.Exec(sql)
			if err != nil {
				panic(err)
			}
		}
		t, _ := rep.Replicate(cfg)
		fmt.Printf("Record updated: %d, Time used: %d ms\n", k, t)
		performance[k] = t
	}

	content, err := json.MarshalIndent(performance, "", "\t")
	if err != nil {
		panic(err)
	}
	err = os.WriteFile("result.json", content, 0644)
	if err != nil {
		panic(err)
	}

}
