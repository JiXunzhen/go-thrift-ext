package transport

import (
	"bytes"

	"github.com/stdrickforce/go-thrift/thrift"
)

type TMemoryBuffer struct {
	thrift.ProtocolReader
	thrift.ProtocolWriter
	buf *bytes.Buffer
}

func NewMemoryBuffer(p thrift.ProtocolBuilder) *TMemoryBuffer {
	mb := &TMemoryBuffer{
		buf: bytes.NewBuffer([]byte{}),
	}
	mb.ProtocolReader = p.NewProtocolReader(mb.buf)
	mb.ProtocolWriter = p.NewProtocolWriter(mb.buf)
	return mb
}

func (self *TMemoryBuffer) Flush() error {
	return nil
}

func (self *TMemoryBuffer) Close() error {
	return nil
}

func (self *TMemoryBuffer) GetBuffer() *bytes.Buffer {
	return self.buf
}

func (self *TMemoryBuffer) PutBack(bytes []byte) (err error) {
	_, err = self.buf.Write(bytes)
	return
}
