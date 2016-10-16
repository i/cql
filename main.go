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

	frame, err := startupFrame()
	fatalIfError("error creating startup frame", err)
	fb, err := frame.bytes()
	fatalIfError("error serializing startup frame", err)
	conn.Write(fb)
	fatalIfError("error writing startup frame", err)

	f, err := readFrame(conn)
	fatalIfError("error reading", err)

	fmt.Println(f.header)

}

func fatalIfError(fmtStr string, err error) {
	if err != nil {
		log.Fatal(fmt.Sprintf("%s: %v", fmtStr, err))
	}
}
