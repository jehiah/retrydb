package main

import (
	"flag"
	"log"

	"github.com/jehiah/retrydb"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	primaryConn := flag.String("primary", "", "the DSN for primary db connection")
	secondaryConn := flag.String("secondary", "", "the DSN for primary db connection")

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
	log.Printf("got %s error %v", k, err)

	defer db.Close()
}
