package server

import (
	"net"

	"github.com/stdrickforce/go-thrift/thrift"
)

type Server struct {
	addr  string
	codec ServerCodec
	pb    thrift.ProtocolBuilder
	// TODO transport builder.
}

func NewServer(
	addr string,
	server interface{},
	pb thrift.ProtocolBuilder,
) *Server {
	return &Server{
		addr:  addr,
		codec: NewServerCodec(server),
	}
}

func (s *Server) process(trans thrift.Transport) (err error) {
	defer trans.Close()

	if err = s.recv(trans); err != nil {
		return
	}

	if err = s.send(trans); err != nil {
		return
	}

	return nil
}

func (s *Server) recv(trans thrift.Transport) (err error) {
	return nil
}

func (s *Server) send(trans thrift.Transport) (err error) {
	return nil
}

func (s *Server) Serve() (err error) {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		trans := thrift.NewTransport(conn, thrift.BinaryProtocol)
		go s.process(trans)
	}
}
