package retrydb_test

import (
	"flag"
	"log"

	"github.com/jehiah/retrydb"
)

func Example() {
	primaryConn := flag.String("primary", "", "the DSN for primary db connection")
	secondaryConn := flag.String("secondary", "", "the DSN for primary db connection")
	flag.Parse()

	db, err := retrydb.Open("mysql", *primaryConn, "mysql", *secondaryConn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Printf("ping failed %s", err)
	}

	var k string
	rows, err := db.Query("select k from table_t where k = ?", "k")
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
	err = db.QueryRow("select k from table_t where k = ?", "k").Scan(&k)
	log.Printf("QueryRow got k:%q error:%v", k, err)
}
