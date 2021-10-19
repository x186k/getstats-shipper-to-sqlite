package main

import (
	"database/sql"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

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

const createAll = `
CREATE TABLE IF NOT EXISTS getstats (
	pcid INT8 NOT NULL,
	updated DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS getstats_pcid_idx ON getstats (pcid);
CREATE INDEX IF NOT EXISTS getstats_updated_idx ON getstats (updated);
`

var cols map[string]struct{} = make(map[string]struct{})

type BodyHalfDecode struct {
	PCID    string
	Reports map[string]json.RawMessage
}

// type BodyFullDecode struct {
// 	PCID    string
// 	Reports map[string]map[string]interface{}
// }

var hostport = pflag.String("hostport", ":8080", "host:port for http")
var debug = pflag.BoolP("debug", "d", false, "enable debug log")
var saveJson = pflag.BoolP("save-json", "j", false, "Enables the saving of raw json")
var saveNormalized = pflag.BoolP("save-normalized", "n", true, "Enables the saving of normalized data: no json_extract(...) for reports")

//it appears from quick research, we can launch concurrent requests against a single db
// but not unlimited requests
//https://github.com/jmoiron/sqlx/issues/120
//db.SetMaxOpenConns(42)
//this is a shared var by design
var db *sqlx.DB

var bodyChan = make(chan []byte)

var dbg = log.New(os.Stdout, "D ", log.LstdFlags|log.Lshortfile)

func main() {
	var err error

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	pflag.Parse()

	if !*debug {
		dbg.SetOutput(io.Discard)
	}

	// https://github.com/mattn/go-sqlite3
	db, err = sqlx.Open("sqlite3", "getstats.db?cache=shared&mode=rwc&_journal_mode=WAL")
	checkFatal(err)

	db.SetMaxOpenConns(5)

	db.MustExec(createAll)

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

	go func() {
		err = http.ListenAndServe(*hostport, mux)
		checkFatal(err)
	}()

	lastBody, err = os.Create("jsonlog.txt")
	checkFatal(err)

	n := 0

	for body := range bodyChan {

		if n >= 1000 {
			off, err := lastBody.Seek(0, io.SeekStart)
			checkFatal(err)
			if off != 0 {
				checkFatal(fmt.Errorf("bad seek off"))
			}
			err = lastBody.Truncate(0)
			checkFatal(err)

			_, err = lastBody.Write(body)
			checkFatal(err)
			_, err = lastBody.WriteString("\n")
			checkFatal(err)

			n = 0
		}

		if *saveNormalized {
			// {"id":"RTCCodec_0_Inbound_107","timestamp":1634435347027.614,"type":"codec","transportId":"RTCTransport_0_1","payloadType":107,"mimeType":"video/rtx","clockRate":90000,"sdpFmtpLine":"apt=125"}
			// {"id":"RTCCodec_0_Inbound_124","timestamp":1634435347027.614,"type":"codec","transportId":"RTCTransport_0_1","payloadType":124,"mimeType":"video/H264","clockRate":90000,"sdpFmtpLine":"level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=4d0032"}
			// {"id":"RTCCodec_0_Outbound_109","timestamp":1634435347027.614,"type":"codec","transportId":"RTCTransport_0_1","payloadType":109,"mimeType":"video/rtx","clockRate":90000,"sdpFmtpLine":"apt=108"}
			//convert to string
			dbg.Println("calling insertIntoDB")
			err := insertIntoDB(body)
			if err != nil {
				log.Println(fmt.Errorf("err:%w  body:%s", err, string(body)))
			}
		}
	}

}

var lastBody *os.File

// var lastRow *os.File

func insertIntoDB(body []byte) error {
	var err error

	var x BodyHalfDecode
	err = json.Unmarshal(body, &x)
	if err != nil {
		return err
	}

	for _, row := range x.Reports {

		var singleReport map[string]interface{}

		err = json.Unmarshal(row, &singleReport)
		if err != nil {
			return err
		}

		keys := make([]string, len(singleReport))
		qmarks := make([]string, len(singleReport))
		vals := make([]interface{}, len(singleReport))

		i := 0
		for key, val := range singleReport {

			valarray, isarray := val.([]interface{})
			if isarray {
				val = StrConvert(valarray)
			}
			vals[i] = fmt.Sprintf("%v", val)
			key = strings.Map(safechar, key)
			keys[i] = key
			qmarks[i] = "?"

			i++

			_, ok := cols[key]

			if !ok {
				valtype := ""

				err = db.Get(&valtype, "SELECT typeof(?)", val)
				if err != nil {
					return fmt.Errorf("select typeof fail: %w, json: %s", err, string(row))
				}

				cols[key] = struct{}{}

				addcol := fmt.Sprintf(`ALTER TABLE getstats ADD COLUMN %s %s`, key, valtype)
				_, err := db.Exec(addcol)
				if err != nil {
					return fmt.Errorf("alter table fail: %w, json: %s, sql: %s", err, string(row), addcol)
				}
			}
		}

		insertstmt := fmt.Sprintf("insert into getstats (pcid,%s) values (%s,%s)", strings.Join(keys, ","), x.PCID, strings.Join(qmarks, ","))
		dbg.Println(insertstmt)
		dbg.Println("values = ", StrConvert(vals))

		_, err := db.Exec(insertstmt, vals...)
		if err != nil {
			return fmt.Errorf("insert into table fail: %w, json: %s, sql: %s", err, string(row), insertstmt)
		}

	}
	return nil
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

	println(88)
	bodyChan <- body
	println(77)

}
