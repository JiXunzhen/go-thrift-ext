package client

import (
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/stdrickforce/go-thrift-ext/transport"
	"github.com/stdrickforce/go-thrift/thrift"
)

type HttpClient struct {
	Client
	uri string
	pb  thrift.ProtocolBuilder
	seq int32
}

func (c *HttpClient) request(trans *transport.TMemoryBuffer) (err error) {
	buf := trans.GetBuffer()

	// send http request and get response
	resp, err := http.Post(c.uri, "application/x-thrift", buf)
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}

	// put http response to memory buffer.
	bytes, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return
	}
	trans.PutBack(bytes)
	return
}

func (c *HttpClient) Call(
	method string,
	req interface{},
	res interface{},
) (err error) {
	seq := c.get_seq_id()

	trans := transport.NewMemoryBuffer(c.pb)
	defer trans.Close()

	if err = c.send(trans, req, method, seq); err != nil {
		return
	}

	if err = c.request(trans); err != nil {
		return
	}

	if err = c.recv(trans, res, method, seq); err != nil {
		return
	}
	return
}

func NewHttpClient(uri string, p thrift.ProtocolBuilder) *HttpClient {
	return &HttpClient{
		uri: uri,
		pb:  p,
		seq: 0,
	}
}
