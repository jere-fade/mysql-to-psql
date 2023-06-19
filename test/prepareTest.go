package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/go-sql-driver/mysql"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	cfg := mysql.Config{
		User:   "root",
		Passwd: "k4aTJCcB4j=+",
		Net:    "tcp",
		Addr:   "127.0.0.1:3306",
		DBName: "homework",
	}

	var err error
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	pingErr := db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	fmt.Println("Connected!")

	var name string
	var score float64
	var year, month, day int
	var date string
	var sql string

	for i := 0; i < 100000; i++ {
		name = randSeq(16)
		score = rand.Float64() * 100
		score = math.Round(score*100) / 100
		year = 2002
		month = rand.Intn(12) + 1
		day = rand.Intn(27) + 1
		date = fmt.Sprintf("%d-%02d-%02d", year, month, day)
		sql = fmt.Sprintf("INSERT INTO student (name, score, birth) values ('%s', %f, '%s')", name, score, date)
		_, err := db.Exec(sql)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%dth record inserted\n", i)
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
