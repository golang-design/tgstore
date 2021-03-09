package tgstore

import (
	"context"
	"crypto/cipher"
	"errors"
	"io"
	"io/fs"
	"sync"
)

// objectMetadata is the metadata of the object.
type objectMetadata struct {
	PartIDs  []string `json:"part_ids"`
	PartSize int64    `json:"part_size"`
	Size     int64    `json:"size"`
}

// objectReader is the reader of the object.
type objectReader struct {
	ctx        context.Context
	mutex      sync.Mutex
	tgs        *TGStore
	aead       cipher.AEAD
	metadata   objectMetadata
	offset     int64
	closed     bool
	readCloser io.ReadCloser
}

// Read implements the `io.Reader`.
func (or *objectReader) Read(b []byte) (int, error) {
	or.mutex.Lock()
	defer or.mutex.Unlock()

	if or.closed {
		return 0, fs.ErrClosed
	} else if or.offset >= or.metadata.Size {
		return 0, io.EOF
	}

	if or.readCloser == nil {
		offsetPartCount := or.offset / or.metadata.PartSize
		offset := or.offset - offsetPartCount*or.metadata.PartSize

		pr, pw := io.Pipe()
		go func() (err error) {
			defer func() {
				pw.CloseWithError(err)
			}()

			nonceCounter := uint64(offsetPartCount*
				or.metadata.PartSize/
				tgFileChunkSize +
				1)

			partIDs := or.metadata.PartIDs[offsetPartCount:]
			for _, partID := range partIDs {
				prc, err := or.tgs.downloadTGFile(
					or.ctx,
					or.aead,
					0,
					&nonceCounter,
					partID,
					offset,
				)
				if err != nil {
					return err
				}

				offset = 0

				_, err = io.Copy(pw, prc)
				prc.Close()
				if err != nil {
					return err
				}
			}

			return nil
		}()

		or.readCloser = pr
	}

	n, err := or.readCloser.Read(b)
	or.offset += int64(n)

	return n, err
}

// Seek implements the `io.Seeker`.
func (or *objectReader) Seek(offset int64, whence int) (int64, error) {
	or.mutex.Lock()
	defer or.mutex.Unlock()

	if or.closed {
		return 0, fs.ErrClosed
	}

	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += or.offset
	case io.SeekEnd:
		offset += or.metadata.Size
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
func (or *objectReader) Close() error {
	or.mutex.Lock()
	defer or.mutex.Unlock()

	if or.closed {
		return fs.ErrClosed
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
