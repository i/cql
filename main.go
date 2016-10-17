package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	log.Println("connecting...")
	conn, err := net.Dial("tcp", "127.0.0.1:9042")
	fatalIfError("error connecting", err)

	go frameReader(conn)

	frame, err := startupFrame()
	fatalIfError("error creating startup frame", err)
	fb, err := frame.bytes()
	fatalIfError("error serializing startup frame", err)
	conn.Write(fb)
	fatalIfError("error writing startup frame", err)

	select {}
}

func frameReader(c net.Conn) {
	for {
		f, err := readFrame(c)
		if err != nil {
			log.Printf("error reading frame: %v", err)
			continue
		}

		switch f.header.Opcode {
		case _error:
		case _startup:
		case _ready:
		case _authenticate:
		case _options:
		case _supported:
		case _query:
		case _result:
		case _prepare:
		case _execute:
		case _register:
		case _event:
		case _batch:
		case _authChallenge:
		case _authResponse:
		case _authSuccess:
		default:
			log.Printf("got frame with unknown opcode: %x", f.header.Opcode)
		}
	}
}

func fatalIfError(fmtStr string, err error) {
	if err != nil {
		log.Fatal(fmt.Sprintf("%s: %v", fmtStr, err))
	}
}
