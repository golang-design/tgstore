package tgstore

import (
	"context"
	"crypto/cipher"
	"errors"
	"io"
	"os"
	"sync"
)

// Object is the unit of the `TGStore`.
type Object struct {
	ID       string
	MIMEType string
	Size     int64
	Checksum []byte

	aead         cipher.AEAD
	chunkIDs     []string
	hashMidstate []byte
}

// NewReader returns a new instance of the `ObjectReader`.
func (o *Object) NewReader(ctx context.Context) (*ObjectReader, error) {
	return &ObjectReader{
		ctx:      ctx,
		aead:     o.aead,
		chunkIDs: o.chunkIDs,
		size:     o.Size,
	}, nil
}

// ObjectReader is the reader of the `Object`.
type ObjectReader struct {
	ctx        context.Context
	mutex      sync.Mutex
	aead       cipher.AEAD
	chunkIDs   []string
	size       int64
	offset     int64
	closed     bool
	readCloser io.ReadCloser
}

// Read implements the `io.Reader`.
func (or *ObjectReader) Read(b []byte) (int, error) {
	or.mutex.Lock()
	defer or.mutex.Unlock()

	if or.closed {
		return 0, os.ErrClosed
	} else if or.offset >= or.size {
		return 0, io.EOF
	}

	if or.readCloser == nil {
		pipeReader, pipeWriter := io.Pipe()
		go func() (err error) {
			defer func() {
				pipeWriter.CloseWithError(err)
			}()

			return errors.New("not implemented")
		}()

		or.readCloser = pipeReader
	}

	n, err := or.readCloser.Read(b)
	or.offset += int64(n)

	return n, err
}

// Seek implements the `io.Seeker`.
func (or *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	or.mutex.Lock()
	defer or.mutex.Unlock()

	if or.closed {
		return 0, os.ErrClosed
	}

	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += or.offset
	case io.SeekEnd:
		offset += or.size
	default:
		return 0, errors.New("invalid whence")
	}

	if offset < 0 {
		return 0, errors.New("negative position")
	}

	if offset != or.offset {
		or.offset = offset
		if or.readCloser != nil {
			if err := or.readCloser.Close(); err != nil {
				return 0, err
			}

			or.readCloser = nil
		}
	}

	return or.offset, nil
}

// Close implements the `io.Closer`.
func (or *ObjectReader) Close() error {
	or.mutex.Lock()
	defer or.mutex.Unlock()

	if or.closed {
		return os.ErrClosed
	}

	if or.readCloser != nil {
		if err := or.readCloser.Close(); err != nil {
			return err
		}

		or.readCloser = nil
	}

	or.closed = true

	return nil
}
