package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
)

// use this mostly as a reference
type (
	_int        int32
	_long       int64
	_short      int16
	_string     string
	_longString struct {
		n _int
		b []byte
	}
	_uuid       [16]byte
	_stringList struct {
		n _short
		s []_string
	}
	_bytes struct {
		n _int
		b []byte
	}
	_shortBytes struct {
		n _short
		b []byte
	}
	_option struct {
		id    _short
		value interface{}
	}
	_optionList struct {
		n       _short
		options []_option
	}
	_inet struct {
		n byte
		b []byte
	}
	_stringMap      map[string]string
	_stringMultiMap struct {
		n     _short
		pairs []struct {
			k _string
			v _stringList
		}
	}
)

type body interface {
	bytes() ([]byte, error)
}

type frame struct {
	header *header
	body   []byte
}

func (f *frame) bytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, f.header.Version); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, f.header.Flags); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, f.header.Stream); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, f.header.Opcode); err != nil {
		return nil, err
	}

	// write length to frame
	if err := binary.Write(buf, binary.BigEndian, int32(len(f.body))); err != nil {
		return nil, err
	}

	// write body to frame
	if _, err := buf.Write(f.body); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type opcode int8

const (
	_error opcode = iota
	_startup
	_ready
	_authenticate
	_ // there is no 0x04 opcode
	_options
	_supported
	_query
	_result
	_prepare
	_execute
	_register
	_event
	_batch
	_authChallenge
	_authResponse
	_authSuccess
)

// A Consistency represents a consistency level (see link(todo))
type Consistency _short

// Consistency levels
const (
	Any Consistency = iota
	One
	Two
	Three
	Quorum
	All
	LocalQuorum
	EachQuorum
	Serial
	LocalSerial
	LocalOne
)

const (
	_cqlVersionKey     = "CQL_VERSION"
	_cqlVersionValue   = "3.0.0"
	_compressionString = "COMPRESSION"
)

func startupFrame() (*frame, error) {
	h := newHeader()
	h.Version = _versionRequest
	h.Flags = 0
	h.Stream = 1
	h.Opcode = _startup
	f := newFrame()
	f.header = h
	var err error
	f.body, err = _stringMap(map[string]string{
		_cqlVersionKey: _cqlVersionValue,
	}).bytes()
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s _string) bytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, _short(len(s))); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(s)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// [string map]
// A [short] n, followed by n pair <k><v> where <k> and <v>
// are [string].
func (m _stringMap) bytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, _short(len(m))); err != nil {
		return nil, err
	}
	for k, v := range m {
		kb, err := _string(k).bytes()
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(kb); err != nil {
			return nil, err
		}
		vb, err := _string(v).bytes()
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(vb); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

type version byte

const (
	_versionRequest  = 0x03
	_versionResponse = 0x83
)

var (
	framePool = &sync.Pool{
		New: func() interface{} {
			return new(frame)
		},
	}
	headerPool = sync.Pool{
		New: func() interface{} {
			return new(header)
		},
	}
)

func newFrame() *frame {
	f := framePool.Get().(*frame)
	f.header = nil
	f.body = nil
	return f
}

func newHeader() *header {
	h := headerPool.Get().(*header)
	h.Version = 0
	h.Flags = 0
	h.Stream = 0
	h.Opcode = 0
	h.Length = 0
	return h
}

type header struct {
	Version version
	Flags   int8
	Stream  int16
	Opcode  opcode
	Length  int32
}

func readHeader(r io.Reader) (*header, error) {
	h := new(header)
	if err := binary.Read(r, binary.BigEndian, h); err != nil {
		return nil, err
	}
	return h, nil
}

func readFrame(r io.Reader) (*frame, error) {
	h, err := readHeader(r)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, h.Length)
	if _, err := r.Read(buf); err != nil {
		return nil, err
	}

	f := newFrame()
	f.header = h
	f.body = buf

	return f, nil
}
