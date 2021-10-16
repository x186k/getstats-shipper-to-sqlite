package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"
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

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func Gzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Content-Encoding", "gzip")

		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		defer func() {
			gz.Close()
			w.Header().Set("Content-Length", fmt.Sprint(len(b.Bytes())))
			_, _ = w.Write(b.Bytes())
		}()

		r.Header.Del("Accept-Encoding") // prevent double-gzipping
		next.ServeHTTP(&gzipResponseWriter{ResponseWriter: w, Writer: gz}, r)
	})
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
