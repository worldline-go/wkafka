package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type Error struct {
	Msg  string
	Err  error
	Code int
}

func (e Error) Error() string {
	if e.Err == nil {
		return e.Msg
	}

	return fmt.Sprintf("%s; %s", e.Msg, e.Err.Error())
}

func NewError(message string, err error, code int) Error {
	return Error{
		Msg:  message,
		Err:  err,
		Code: code,
	}
}

func httpResponseError(w http.ResponseWriter, err error) {
	if e := new(Error); !errors.As(err, &e) {
		httpResponse(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		httpResponse(w, e.Msg, e.Code)
	}
}

func httpResponse(w http.ResponseWriter, msg string, code int) {
	v, _ := json.Marshal(Response{
		Message: msg,
	})

	httpResponseByte(w, v, code)
}

func httpResponseJSON(w http.ResponseWriter, msg any, code int) {
	v, _ := json.Marshal(msg)

	httpResponseByte(w, v, code)
}

func httpResponseByte(w http.ResponseWriter, msg []byte, code int) {
	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(code)
	w.Write(msg)
}
