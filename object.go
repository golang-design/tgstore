package tgstore

import (
	"bytes"
	"context"
	"crypto/cipher"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/poly1305"
)

const (
	// objectChunkSize is the chunk size of object.
	objectChunkSize = 64 << 10

	// objectEncryptedChunkSize is the encrypted chunk size of object.
	objectEncryptedChunkSize = chacha20poly1305.NonceSize + objectChunkSize + poly1305.TagSize
)

// Object is the unit of the `TGStore`.
type Object struct {
	ID       string
	Size     int64
	Checksum []byte

	tgs          *TGStore
	aead         cipher.AEAD
	contents     []*objectContent
	hashMidstate []byte
}

// NewReader returns a new instance of the `ObjectReader`.
func (o *Object) NewReader(ctx context.Context) (*ObjectReader, error) {
	return &ObjectReader{
		ctx:    ctx,
		object: o,
	}, nil
}

// ObjectReader is the reader of the `Object`.
type ObjectReader struct {
	ctx        context.Context
	mutex      sync.Mutex
	object     *Object
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
	} else if or.offset >= or.object.Size {
		return 0, io.EOF
	}

	if or.readCloser == nil {
		pipeReader, pipeWriter := io.Pipe()
		go func() (err error) {
			defer func() {
				pipeWriter.CloseWithError(err)
			}()

			offset := or.offset
			for _, content := range or.object.contents {
				if content.Size > offset {
					cr, err := content.newReader(
						or.ctx,
						or.object.tgs,
						or.object.aead,
						offset,
					)
					if err != nil {
						return err
					}

					_, err = io.Copy(pipeWriter, cr)
					cr.Close()
					if err != nil {
						return err
					}

					offset = 0
				} else {
					offset -= content.Size
				}
			}

			return nil
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
		offset += or.object.Size
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

// objectMetadata is the metadata of the `Object`.
type objectMetadata struct {
	Contents     []*objectContent `json:"contents"`
	Size         int64            `json:"size"`
	HashMidstate string           `json:"hash_midstate"`
}

// objectContent is the unit of the `Object`.
type objectContent struct {
	ID   string `json:"id"`
	Size int64  `json:"size"`
}

// newReader returns a new instance of the `io.ReadCloser`.
func (oc *objectContent) newReader(
	ctx context.Context,
	tgs *TGStore,
	aead cipher.AEAD,
	offset int64,
) (io.ReadCloser, error) {
	if offset >= oc.Size {
		return ioutil.NopCloser(bytes.NewReader(nil)), nil
	}

	fullChunkOffset := offset / objectChunkSize
	offset -= fullChunkOffset * objectChunkSize

	rc, err := tgs.downloadTelegramFile(
		ctx,
		oc.ID,
		fullChunkOffset*objectEncryptedChunkSize,
	)
	if err != nil {
		return nil, err
	}

	pipeReader, pipeWriter := io.Pipe()
	go func() (err error) {
		defer func() {
			pipeWriter.CloseWithError(err)
			rc.Close()
		}()

		buf := make([]byte, objectEncryptedChunkSize)
		for {
			n, err := io.ReadFull(rc, buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else if !errors.Is(err, io.ErrUnexpectedEOF) {
					return err
				}
			}

			nonce := buf[:chacha20poly1305.NonceSize]
			chunkContent := buf[chacha20poly1305.NonceSize:n]
			if chunkContent, err = aead.Open(
				chunkContent[:0],
				nonce,
				chunkContent,
				nil,
			); err != nil {
				return err
			}

			if offset > 0 {
				chunkContent = chunkContent[offset:]
				offset = 0
			}

			if _, err := pipeWriter.Write(
				chunkContent,
			); err != nil {
				return err
			}
		}

		return nil
	}()

	return pipeReader, nil
}
