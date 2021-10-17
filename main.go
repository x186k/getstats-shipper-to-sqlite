package main

import (
	"database/sql"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

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

// pcid is a unique unint64 per peer connection
// the WebRTC developers did not put a unique identifier in RTCPeerConnection
// so the javascript side of this, just makes a u64 bit random
// https://github.com/w3c/webrtc-pc/issues/1775  Discussion on adding a unique id

const createTable = `
CREATE TABLE IF NOT EXISTS rawjson (
	json TEXT NOT NULL,
	updated DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS getstats (
	pcid INT8 NOT NULL,
	updated DATETIME DEFAULT CURRENT_TIMESTAMP
);
`

const createIndex = `
CREATE INDEX IF NOT EXISTS getstats_pcid_idx ON getstats (pcid);
CREATE INDEX IF NOT EXISTS getstats_updated_idx ON getstats (updated);
`

var cols map[string]struct{} = make(map[string]struct{})
var colsMutex sync.Mutex

const insert = `insert into getstats (pcid,json) values ($1,$2)`

type BodyHalfDecode struct {
	PCID    string
	Reports map[string]json.RawMessage
}

type BodyFullDecode struct {
	PCID    string
	Reports map[string]map[string]interface{}
}

var hostport = pflag.String("hostport", ":8080", "host:port for http")
var saveJson = pflag.BoolP("save-json", "j", false, "Enables the saving of raw json")
var saveNormalized = pflag.BoolP("save-normalized", "n", true, "Enables the saving of normalized data: no json_extract(...) for reports")

//it appears from quick research, we can launch concurrent requests against a single db
// but not unlimited requests
//https://github.com/jmoiron/sqlx/issues/120
//db.SetMaxOpenConns(42)
//this is a shared var by design
var db *sqlx.DB

var bodyChan = make(chan []byte)

func main() {
	var err error

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	pflag.Parse()

	db, err = sqlx.Open("sqlite3", "getstats.db")
	checkFatal(err)

	db.SetMaxOpenConns(5)

	db.MustExec(createTable)
	db.MustExec(createIndex)

	rows, err := db.Query("PRAGMA table_info(getstats)")
	checkFatal(err)

	// iterate over each row
	for rows.Next() {
		var index int
		var name string
		var ig sql.NullString
		err = rows.Scan(&index, &name, &ig, &ig, &ig, &ig)
		checkFatal(err)
		println("found column", name)
		cols[name] = struct{}{}
	}
	// check the error from rows
	err = rows.Err()
	checkFatal(err)

	mux := http.NewServeMux()
	mux.HandleFunc("/", postHandler)

	go func() {}()
	err = http.ListenAndServe(*hostport, mux)
	checkFatal(err)

	for body := range bodyChan {

		if *saveJson {
			n, err := db.MustExec(insert, decodedBody.PCID, string(v)).RowsAffected()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				log.Println(err.Error())
				return
			}
		}

		if *saveNormalized {
			var x BodyFullDecode
			err = json.Unmarshal(body, &x)
			if err != nil {
				log.Println(err.Error())
				return
			}

			// {"id":"RTCCodec_0_Inbound_107","timestamp":1634435347027.614,"type":"codec","transportId":"RTCTransport_0_1","payloadType":107,"mimeType":"video/rtx","clockRate":90000,"sdpFmtpLine":"apt=125"}
			// {"id":"RTCCodec_0_Inbound_124","timestamp":1634435347027.614,"type":"codec","transportId":"RTCTransport_0_1","payloadType":124,"mimeType":"video/H264","clockRate":90000,"sdpFmtpLine":"level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=4d0032"}
			// {"id":"RTCCodec_0_Outbound_109","timestamp":1634435347027.614,"type":"codec","transportId":"RTCTransport_0_1","payloadType":109,"mimeType":"video/rtx","clockRate":90000,"sdpFmtpLine":"apt=108"}

			for _, singleReport := range x.Reports {

				keys := make([]string, len(singleReport))
				qmarks := make([]string, len(singleReport))
				vals := make([]interface{}, len(singleReport))

				i := 0
				for key, val := range singleReport {

					valarray, isarray := val.([]interface{})
					if isarray {
						val = StrConvert(valarray)
					}
					vals[i] = fmt.Sprintf("%v", val) //convert to string
					key = strings.Map(safechar, key)
					keys[i] = key
					qmarks[i] = "?"

					i++

					colsMutex.Lock()
					_, ok := cols[key]
					colsMutex.Unlock()

					if !ok {
						valtype := ""
						println(777, key, val)
						err = db.Get(&valtype, "SELECT typeof(?)", val)
						checkFatal(err)

						println(88, valtype)

						const addcol = `ALTER TABLE getstats ADD COLUMN %s %s`

						colsMutex.Lock()
						cols[key] = struct{}{}
						colsMutex.Unlock()
						sqlx.MustExec(db, fmt.Sprintf(addcol, key, valtype))
					}
				}

				insertstmt := fmt.Sprintf("insert into getstats (pcid,%s) values (%s,%s)", strings.Join(keys, ","), x.PCID, strings.Join(qmarks, ","))
				println(33, insertstmt)
				println(44, StrConvert(vals), ",")

				db.MustExec(insertstmt, vals...)

				// n, err := db.MustExec(insert, x.PCID, string(singleReport)).RowsAffected()
				// if err != nil {
				// 	http.Error(w, err.Error(), http.StatusInternalServerError)
				// 	log.Println(err.Error())
				// 	return
				// }
				// if n != 1 {
				// 	m := fmt.Errorf("insert did not return one: %d", n)
				// 	http.Error(w, m.Error(), http.StatusInternalServerError)
				// 	log.Println(m.Error())
				// 	return
				// }

			}

		}
	}

}
func safechar(r rune) rune {
	switch {
	case r == '-':
		fallthrough
	case r >= 'A' && r <= 'Z':
		fallthrough
	case r >= '0' && r <= '9':
		fallthrough
	case r >= 'a' && r <= 'z':
		return r
	}
	return -1
}

func StrConvert(a ...interface{}) string {
	str := ""

	for index := 0; index < len(a); index++ {
		str1 := fmt.Sprintf("%v", a[index])
		str += "," + str1
	}
	return str
}



func postHandler(w http.ResponseWriter, r *http.Request) {

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Println(err.Error())
		return
	}

	bodyChan <- string(body)

}

var _ = saveJsonFunc

func saveJsonFunc(body []byte, w http.ResponseWriter) {
	var decodedBody BodyHalfDecode
	var err error
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

}
