package client

import (
	"net"

	"github.com/stdrickforce/go-thrift/thrift"
)

type ClientPool struct {
	Client
	conns chan thrift.Transport
	addr  string
	pb    thrift.ProtocolBuilder
	seq   int32
}

func (c *ClientPool) Call(
	method string,
	req interface{},
	res interface{},
) (err error) {
	trans, err := c.get_or_create()
	if err != nil {
		return err
	}

	seq := c.get_seq_id()
	if err = c.send(trans, req, method, seq); err != nil {
		trans.Close()
		return
	}
	if err = c.recv(trans, res, method, seq); err != nil {
		trans.Close()
		return
	}

	// NOTE only pushback transport while request has been fully served.
	c.pushback(trans)
	return
}

func (c *ClientPool) create() (trans thrift.Transport, err error) {
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

func (c *ClientPool) get_or_create() (trans thrift.Transport, err error) {
	select {
	case conn := <-c.conns:
		// TODO check is-alive before return.
		return conn, nil
	default:
		return c.create()
	}
}

func (c *ClientPool) pushback(conn thrift.Transport) {
	select {
	case c.conns <- conn:
		return
	default:
		conn.Close()
	}
}

func NewClientPool(addr string, p thrift.ProtocolBuilder) *ClientPool {
	return &ClientPool{
		addr:  addr,
		pb:    p,
		conns: make(chan thrift.Transport, 10),
		seq:   0,
	}
}
