package http

import (
	"io"
	"net/http"
	"rache/kv"
)

var GetHandler = func(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		io.WriteString(w, "request is wrong!")
	}
	key, ok := r.Form["key"]
	if !ok {
		io.WriteString(w, "request params is invalid")
	}
	value := kv.RacheClient.Get(key[0])
	io.WriteString(w, value)
}

var PutHandler = func(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		io.WriteString(w, "request is wrong!")
	}
	key, ok1 := r.Form["key"]
	value, ok2 := r.Form["value"]
	if !(ok2 && ok1) {
		io.WriteString(w, "request params is invalid")
	}
	kv.RacheClient.Put(key[0], value[0])
}

var AppendHandler = func(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		io.WriteString(w, "request is wrong!")
	}
	key, ok1 := r.Form["key"]
	value, ok2 := r.Form["value"]
	if !(ok2 && ok1) {
		io.WriteString(w, "request params is invalid")
	}
	kv.RacheClient.Append(key[0], value[0])
}

func init() {
	http.HandleFunc("/api/get", GetHandler)
	http.HandleFunc("/api/put", PutHandler)
	http.HandleFunc("/api/append", AppendHandler)
	go http.ListenAndServe(":8080", nil)
}

func Init() {

}