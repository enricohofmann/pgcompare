package main

import (
	"fmt"
	"github.com/namsral/flag"
	"io"
	"log"
	"net"
	"strconv"
)

func main() {
	var lPort int

	flag.IntVar(&lPort, "port", 8080, "local port on which the proxy will listen")
	remoteAddr := flag.String("remote", "", "remote address, as a pair of addr:port, where the requests are sent")

	log.SetFlags(0)
	log.SetPrefix("proxy: ")

	flag.Parse()

	if *remoteAddr == "" {
		log.Fatal("need to define a remote address")
	}

	log.Printf("Bind Proxy to port: %v", lPort)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(lPort))
	if err != nil {
		panic("connection error:" + err.Error())
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept Error:", err)
			continue
		}
		copyConn(conn, *remoteAddr)
	}
}

func copyConn(src net.Conn, address string) {
	log.Printf("new client connection")
	dst, err := net.Dial("tcp", address)
	if err != nil {
		panic("Dial Error:" + err.Error())
	}

	done := make(chan struct{})

	go func() {
		defer src.Close()
		defer dst.Close()
		io.Copy(dst, src)
		done <- struct{}{}
	}()

	go func() {
		defer src.Close()
		defer dst.Close()
		io.Copy(src, dst)
		done <- struct{}{}
	}()

	<-done
	<-done
}
