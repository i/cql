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

	frame := startupFrame()
	bb, err := frame.bytes()
	fatalIfError("error serializing startup frame", err)

	_, err = conn.Write(bb)
	fatalIfError("error writing startup frame", err)

	//f, err := readResponse(buf[:n])
	_, err = readFrame(conn)
	fatalIfError("error reading", err)

}

func fatalIfError(fmtStr string, err error) {
	if err != nil {
		log.Fatal(fmt.Sprintf("%s: %v", fmtStr, err))
	}
}
