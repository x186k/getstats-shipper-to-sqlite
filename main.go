package main

import (
	"io/ioutil"
	"net/http"

	"encoding/json"
	"fmt"

	"log"

	"path/filepath"
	"runtime"

	"github.com/spf13/pflag"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

func checkFatal(err error) {
	if err != nil {
		_, fileName, fileLine, _ := runtime.Caller(1)
		log.Fatalf("FATAL %s:%d %v", filepath.Base(fileName), fileLine, err)
		// log.Fatalf calls os.Exit(1)
	}
}

var hostport = pflag.String("hostport", ":8080", "host:port for http")

const createTable = `
CREATE TABLE IF NOT EXISTS getstats (
	json TEXT NOT NULL,
	updated DATETIME DEFAULT CURRENT_TIMESTAMP
)
`

const createIndex = `
CREATE INDEX IF NOT EXISTS getstats_updated_idx ON getstats (updated)
`

const insert = `insert into getstats (json) values ($1)`




func main() {
	var err error

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	pflag.Parse()

	db, err := sqlx.Open("sqlite3", "getstats.db")
	checkFatal(err)

	db.MustExec(createTable)
	db.MustExec(createIndex)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Println(err.Error())
			return
		}

		var objmap map[string]json.RawMessage
		err = json.Unmarshal(body, &objmap)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Println(err.Error())
			return
		}

		for _, v := range objmap {

			n, err := db.MustExec(insert, string(v)).RowsAffected()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				log.Println(err.Error())
				return
			}
			if n != 1 {
				m := fmt.Errorf("insert did not return one: %d", n)
				http.Error(w, m.Error(), http.StatusInternalServerError)
				log.Println(m.Error())
				return
			}

		}

	})

	err = http.ListenAndServe(*hostport, mux)
	checkFatal(err)

}
