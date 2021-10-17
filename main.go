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

// pcid is a unique unint64 per peer connection
// the WebRTC developers did not put a unique identifier in RTCPeerConnection
// so the javascript side of this, just makes a u64 bit random
// https://github.com/w3c/webrtc-pc/issues/1775  Discussion on adding a unique id

const createTable = `
CREATE TABLE IF NOT EXISTS getstats (
	pcid INT8 NOT NULL,
	json TEXT NOT NULL,
	updated DATETIME DEFAULT CURRENT_TIMESTAMP
)
`

const createIndex = `
CREATE INDEX IF NOT EXISTS getstats_pcid_idx ON getstats (pcid);
CREATE INDEX IF NOT EXISTS getstats_updated_idx ON getstats (updated);
`

const insert = `insert into getstats (pcid,json) values ($1,$2)`

type Body struct {
	PCID    string
	Reports map[string]json.RawMessage
}

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

		var decodedBody Body
		err = json.Unmarshal(body, &decodedBody)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Println(err.Error())
			return
		}

		for _, v := range decodedBody.Reports {

			n, err := db.MustExec(insert, decodedBody.PCID, string(v)).RowsAffected()
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
