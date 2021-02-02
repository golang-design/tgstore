package tgstore

import (
	"context"
	"crypto/cipher"
	"errors"
	"io"
	"os"
	"sync"
)

// objectMetadata is the metadata of the object.
type objectMetadata struct {
	PartIDs   []string `json:"part_ids"`
	Size      int64    `json:"size"`
	SplitSize int64    `json:"split_size"`
}

// ObjectReader is the reader of the object.
//
// TODO: Unexport the `ObjectReader` and use the `io.ReadSeekCloser` instead
// once Go 1.16 is released.
type ObjectReader struct {
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
func (or *ObjectReader) Read(b []byte) (int, error) {
	or.mutex.Lock()
	defer or.mutex.Unlock()

	if or.closed {
		return 0, os.ErrClosed
	} else if or.offset >= or.metadata.Size {
		return 0, io.EOF
	}

	if or.readCloser == nil {
		var offsetPartCount int64
		if or.metadata.SplitSize > 0 {
			offsetPartCount = or.offset / or.metadata.SplitSize
		}

		offset := or.offset - offsetPartCount*or.metadata.SplitSize

		partIDs := or.metadata.PartIDs[offsetPartCount:]
		if len(partIDs) == 1 {
			var err error
			if or.readCloser, err = or.tgs.downloadTelegramFile(
				or.ctx,
				or.aead,
				or.metadata.PartIDs[0],
				offset,
			); err != nil {
				return 0, err
			}
		} else {
			pr, pw := io.Pipe()
			go func() (err error) {
				defer func() {
					pw.CloseWithError(err)
				}()

				for _, partID := range partIDs {
					prc, err := or.tgs.downloadTelegramFile(
						or.ctx,
						or.aead,
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
