package main

import (
	"flag"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jehiah/retrydb"
)

func main() {
	primaryConn := flag.String("primary", "", "the DSN for primary db connection")
	secondaryConn := flag.String("secondary", "", "the DSN for primary db connection")
	flag.Parse()

	db, err := retrydb.Open("mysql", *primaryConn, "mysql", *secondaryConn)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Printf("ping failed %s", err)
	}

	var k string
	rows, err := db.Query("select k from monitor_t where k = ?", "k")
	if err != nil {
		log.Fatalf("query failed %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&k)
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("Query got k:%q error:%v", k, err)

	k = ""
	err = db.QueryRow("select k from monitor_t where k = ?", "k").Scan(&k)
	log.Printf("QueryRow got k:%q error:%v", k, err)

	defer db.Close()
}
