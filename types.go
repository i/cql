package main

import (
	"bytes"
	"encoding/binary"
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
	version version
	flags   int8
	stream  int16
	opcode  opcode
	length  int32
	body    body
}

func (f *frame) bytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, f.version); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, f.flags); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, f.stream); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, f.opcode); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, f.length); err != nil {
		return nil, err
	}
	bb, err := f.body.bytes()
	if err != nil {
		return nil, err
	}
	if _, err := buf.Write(bb); err != nil {
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

func startupFrame() *frame {
	f := newFrame()
	f.version = _versionRequest
	f.flags = 0
	f.stream = 1
	f.opcode = _startup
	f.body = _stringMap(map[string]string{
		_cqlVersionKey: _cqlVersionValue,
	})
	return f
}

func (s _string) bytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, _short(len(s)))
	buf.Write([]byte(s))
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

type version int8

const (
	_versionRequest  = 0x03
	_versionResponse = 0x83
)

var framePool = &sync.Pool{
	New: func() interface{} {
		return new(frame)
	},
}

func newFrame() *frame {
	f := framePool.Get().(*frame)
	f.version = 0
	f.flags = 0
	f.stream = 0
	f.opcode = 0
	f.length = 0
	f.body = nil
	return f
}
