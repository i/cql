package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:7199")
	fatalIfError("error connecting", err)

	fmt.Println(conn)

	bb, err := startupFrame().bytes()
	fatalIfError("error serializing startup frame", err)

	_, err = conn.Write(bb)
	fatalIfError("error writing startup frame", err)

	time.Sleep(time.Second)

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	fmt.Println(n)
	fatalIfError("error reading", err)

}

func fatalIfError(fmtStr string, err error) {
	if err != nil {
		log.Fatal(fmt.Sprintf("%s: %v", fmtStr, err))
	}
}
