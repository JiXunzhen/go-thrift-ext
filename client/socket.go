package client

// TODO oneway support.

import (
	"errors"
	"fmt"
	"net"

	"github.com/stdrickforce/go-thrift/thrift"
)

const (
	CALL  = thrift.MessageTypeCall
	REPLY = thrift.MessageTypeReply
	EXC   = thrift.MessageTypeException
)

type Client struct {
	addr string
	pb   thrift.ProtocolBuilder
	seq  int32
}

func (c *Client) send(
	conn thrift.Transport,
	req interface{},
	method string,
	seq int32,
) (err error) {
	// write message begin
	if err = conn.WriteMessageBegin(method, CALL, seq); err != nil {
		return
	}

	// write message body
	if err = thrift.EncodeStruct(conn, req); err != nil {
		return
	}

	// write message end
	if err = conn.WriteMessageEnd(); err != nil {
		return
	}

	// flush transport
	if err = conn.Flush(); err != nil {
		return
	}
	return
}

func (c *Client) recv(
	conn thrift.Transport,
	res interface{},
	method string,
	seq int32,
) (err error) {
	defer conn.ReadMessageEnd()

	_, mtype, rseq, err := conn.ReadMessageBegin()
	if err != nil {
		return
	}

	// NOTE is checking response method name neccessary?
	// if method != name {
	// 	return errors.New("method name mismatch!")
	// }

	if rseq != seq {
		return errors.New("sequence id out of order!")
	}

	switch mtype {
	case thrift.MessageTypeReply:
		return c.recvResponse(conn, res)
	case thrift.MessageTypeException:
		return c.recvException(conn)
	default:
		return errors.New(fmt.Sprintf("unknown message type %d", mtype))
	}
}

func (c *Client) recvResponse(
	conn thrift.Transport,
	res interface{},
) (err error) {
	if err = thrift.DecodeStruct(conn, res); err != nil {
		return
	}
	return
}

func (c *Client) recvException(
	conn thrift.Transport,
) (err error) {
	exception := &thrift.ApplicationException{}
	if err = thrift.DecodeStruct(conn, exception); err != nil {
		return err
	}
	return errors.New(exception.String())
}

func (c *Client) create() (trans thrift.Transport, err error) {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return nil, err
	}
	trans = thrift.NewTransport(
		conn,
		c.pb,
	)
	return
}

func (c *Client) get_seq_id() int32 {
	defer func() { c.seq++ }()
	return c.seq
}

func (c *Client) Call(
	method string,
	req interface{},
	res interface{},
) (err error) {
	trans, err := c.create()
	if err != nil {
		return
	}
	defer trans.Close()

	seq := c.get_seq_id()
	if err = c.send(trans, req, method, seq); err != nil {
		return
	}
	if err = c.recv(trans, res, method, seq); err != nil {
		return
	}
	return
}

func NewClient(addr string, p thrift.ProtocolBuilder) *Client {
	return &Client{
		addr: addr,
		pb:   p,
	}
}
