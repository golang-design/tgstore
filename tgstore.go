/*
Package tgstore implements an encrypted object storage system with unlimited
space backed by Telegram.
*/
package tgstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/poly1305"
	"gopkg.in/tucnak/telebot.v2"
)

const (
	// telegramFileChunkSize is the chunk size of Telegram file.
	telegramFileChunkSize = 64 << 10

	// telegramFileEncryptedChunkSize is the encrypted chunk size of
	// Telegram file.
	telegramFileEncryptedChunkSize = chacha20poly1305.NonceSize +
		telegramFileChunkSize +
		poly1305.TagSize
)

// TGStore is the top-level struct of this project.
//
// It is highly recommended not to modify the value of any field of the
// `TGStore` after calling any methods of it, which will cause unpredictable
// problems.
//
// The new instances of the `TGStore` should only be created by calling the
// `New`.
type TGStore struct {
	// BotAPIEndpoint is the endpoint of the Telegram Bot API.
	//
	// You might prefer to use your local Telegram Bot API server. See
	// https://core.telegram.org/bots/api#using-a-local-bot-api-server for
	// benefits.
	//
	// Default value: "https://api.telegram.org"
	BotAPIEndpoint string `mapstructure:"bot_api_endpoint"`

	// BotToken is the Telegram bot token.
	//
	// Default value: ""
	BotToken string `mapstructure:"bot_token"`

	// ChatID is the ID of the Telegram chat used to store the objects to be
	// uploaded.
	//
	// It is ok to change the `ChatID` if you want. The objects that have
	// already been uploaded are not affected.
	//
	// Default value: 0
	ChatID int64 `mapstructure:"chat_id"`

	// MaxFileBytes is the maximum number of bytes allowed for a Telegram
	// file to have.
	//
	// The `MaxFileBytes` must be at least 20971520 (20 MiB).
	//
	// It is ok to change the `MaxFileBytes` if you want. The objects that
	// have already been uploaded are not affected.
	//
	// Default value: 20971520
	MaxFileBytes int `mapstructure:"max_file_bytes"`

	// MaxObjectMetadataCacheBytes is the maximum number of bytes allowed
	// for object metadata cache to use.
	//
	// The `MaxObjectMetadataCacheBytes` must be at least 1048576 (1 MiB).
	//
	// Default value: 1048576
	MaxObjectMetadataCacheBytes int `mapstructure:"max_object_metadata_cache_bytes"`

	// HTTPClient is the `http.Client` used to communicate with the Telegram
	// Bot API.
	//
	// Default value: `http.DefaultClient`
	HTTPClient *http.Client `mapstructure:"-"`

	loadOnce            sync.Once
	loadError           error
	bot                 *telebot.Bot
	chat                *telebot.Chat
	fileHeadPool        sync.Pool
	objectSplitSize     int64
	objectMetadataCache *bigcache.BigCache
}

// New returns a new instance of the `TGStore` with default field values.
//
// The `New` is the only function that creates new instances of the `TGStore`
// and keeps everything working.
func New() *TGStore {
	return &TGStore{
		BotAPIEndpoint:              "https://api.telegram.org",
		MaxFileBytes:                20 << 20,
		MaxObjectMetadataCacheBytes: 1 << 20,
		HTTPClient:                  http.DefaultClient,
		fileHeadPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 1<<20)
			},
		},
	}
}

// load loads the stuff of the tgs up.
func (tgs *TGStore) load() {
	if tgs.bot, tgs.loadError = telebot.NewBot(telebot.Settings{
		URL:      tgs.BotAPIEndpoint,
		Token:    tgs.BotToken,
		Reporter: func(error) {},
		Client:   tgs.HTTPClient,
	}); tgs.loadError != nil {
		tgs.loadError = fmt.Errorf(
			"failed to create telegram bot: %v",
			tgs.loadError,
		)
		return
	}

	if tgs.chat, tgs.loadError = tgs.bot.ChatByID(
		strconv.FormatInt(tgs.ChatID, 10),
	); tgs.loadError != nil {
		tgs.loadError = fmt.Errorf(
			"failed to get telegram chat: %v",
			tgs.loadError,
		)
		return
	}

	if tgs.MaxFileBytes < 20<<20 {
		tgs.loadError = errors.New("invalid max file bytes")
		return
	}

	tgs.objectSplitSize = int64(tgs.MaxFileBytes)
	tgs.objectSplitSize /= telegramFileEncryptedChunkSize
	tgs.objectSplitSize *= telegramFileChunkSize

	if tgs.MaxObjectMetadataCacheBytes < 1<<20 {
		tgs.loadError = errors.New(
			"invalid max object metadata cache bytes",
		)
		return
	}

	maxObjectMetadataCacheMB := tgs.MaxObjectMetadataCacheBytes / (1 << 20)
	if tgs.objectMetadataCache, tgs.loadError = bigcache.NewBigCache(
		bigcache.Config{
			Shards:             1024,
			LifeWindow:         24 * time.Hour,
			CleanWindow:        10 * time.Second,
			MaxEntriesInWindow: 1000 * 10 * 60,
			MaxEntrySize:       500,
			HardMaxCacheSize:   maxObjectMetadataCacheMB,
		},
	); tgs.loadError != nil {
		tgs.loadError = fmt.Errorf(
			"failed to create object metadata cache: %v",
			tgs.loadError,
		)
		return
	}
}

// Upload uploads the content to the cloud.
//
// Note that the returned id is not guaranteed for a fixed length.
//
// The lenth of the secretKey must be 16.
func (tgs *TGStore) Upload(
	ctx context.Context,
	secretKey []byte,
	content io.Reader,
) (string, error) {
	tgs.loadOnce.Do(tgs.load)
	if tgs.loadError != nil {
		return "", tgs.loadError
	}

	aead, err := chacha20poly1305.New(secretKey)
	if err != nil {
		return "", err
	}

	if content == nil {
		return "0", nil
	}

	metadata := objectMetadata{}
	for buf := bytes.NewBuffer(nil); ; {
		if _, err := io.CopyN(buf, content, 1); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return "", err
		}

		part := &countReader{
			r: io.LimitReader(
				io.MultiReader(buf, content),
				tgs.objectSplitSize,
			),
		}

		tgfID, err := tgs.uploadTelegramFile(ctx, aead, part)
		if err != nil {
			return "", err
		}

		metadata.PartIDs = append(metadata.PartIDs, tgfID)
		metadata.Size += part.c
	}

	switch len(metadata.PartIDs) {
	case 0:
		return "0", nil
	case 1:
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return "", err
		}

		id := fmt.Sprint("0", metadata.PartIDs[0])
		tgs.objectMetadataCache.Set(id, metadataJSON)

		return id, nil
	}

	metadata.SplitSize = tgs.objectSplitSize

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}

	gzippedMetadataJSON := bytes.Buffer{}
	if gw, err := gzip.NewWriterLevel(
		&gzippedMetadataJSON,
		gzip.BestCompression,
	); err != nil {
		return "", err
	} else if _, err := io.Copy(
		gw,
		bytes.NewReader(metadataJSON),
	); err != nil {
		return "", err
	} else if err := gw.Close(); err != nil {
		return "", err
	}

	tgfID, err := tgs.uploadTelegramFile(ctx, aead, &gzippedMetadataJSON)
	if err != nil {
		return "", err
	}

	id := fmt.Sprint("1", tgfID)
	tgs.objectMetadataCache.Set(id, metadataJSON)

	return id, nil
}

// Download downloads the object targeted by the id from the cloud. It returns
// `fs.ErrNotExist` if not found.
//
// The lenth of the secretKey must be 16.
func (tgs *TGStore) Download(
	ctx context.Context,
	secretKey []byte,
	id string,
) (io.ReadSeekCloser, error) {
	tgs.loadOnce.Do(tgs.load)
	if tgs.loadError != nil {
		return nil, tgs.loadError
	}

	aead, err := chacha20poly1305.New(secretKey)
	if err != nil {
		return nil, err
	}

	switch id {
	case "":
		return nil, fs.ErrNotExist
	case "0":
		return &objectReader{}, nil
	}

	reader := &objectReader{
		ctx:  ctx,
		aead: aead,
		tgs:  tgs,
	}

	if metadataJSON, err := tgs.objectMetadataCache.Get(id); err == nil {
		if err := json.Unmarshal(
			metadataJSON,
			&reader.metadata,
		); err != nil {
			return nil, err
		}

		return reader, nil
	}

	switch id[0] {
	case '0':
		reader.metadata.PartIDs = []string{id[1:]}
		if reader.metadata.Size, err = tgs.sizeTelegramFile(
			ctx,
			reader.metadata.PartIDs[0],
		); err != nil {
			return nil, err
		}

		metadataJSON, err := json.Marshal(reader.metadata)
		if err != nil {
			return nil, err
		}

		tgs.objectMetadataCache.Set(id, metadataJSON)
	case '1':
		tgfrc, err := tgs.downloadTelegramFile(ctx, aead, id[1:], 0)
		if err != nil {
			return nil, err
		}
		defer tgfrc.Close()

		gr, err := gzip.NewReader(tgfrc)
		if err != nil {
			return nil, err
		}
		defer gr.Close()

		metadataJSON, err := io.ReadAll(gr)
		if err != nil {
			return nil, err
		}

		tgs.objectMetadataCache.Set(id, metadataJSON)

		if err := json.Unmarshal(
			metadataJSON,
			&reader.metadata,
		); err != nil {
			return nil, err
		}
	default:
		return nil, fs.ErrNotExist
	}

	return reader, nil
}

// uploadTelegramFile uploads the content to the Telegram.
func (tgs *TGStore) uploadTelegramFile(
	ctx context.Context,
	aead cipher.AEAD,
	content io.Reader,
) (string, error) {
	pr, pw := io.Pipe()
	defer pr.Close()

	go func() (err error) {
		defer func() {
			pw.CloseWithError(err)
		}()

		var (
			buf   = make([]byte, telegramFileEncryptedChunkSize)
			nonce = make([]byte, chacha20poly1305.NonceSize)
		)

		for {
			n, err := io.ReadFull(
				content,
				buf[:telegramFileChunkSize],
			)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else if !errors.Is(err, io.ErrUnexpectedEOF) {
					return err
				}
			}

			if err := readNonce(nonce); err != nil {
				return err
			}

			if _, err := pw.Write(nonce); err != nil {
				return err
			}

			if _, err := pw.Write(aead.Seal(
				buf[:0],
				nonce,
				buf[:n],
				nil,
			)); err != nil {
				return err
			}
		}

		return nil
	}()

	head := tgs.fileHeadPool.Get().([]byte)[:1<<20]
	defer tgs.fileHeadPool.Put(head)

	if n, err := io.ReadFull(pr, head); err != nil {
		switch {
		case errors.Is(err, io.EOF):
			head = nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			head = head[:n]
		default:
			return "", err
		}
	}

	cr := &countReader{
		r: pr,
	}

Upload:
	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	m, err := tgs.bot.Send(tgs.chat, &telebot.Document{
		File: telebot.FromReader(io.MultiReader(
			bytes.NewReader(head),
			cr,
		)),
	})
	if err != nil {
		if cr.c == 0 && isRetryableTelegramBotAPIError(err) {
			time.Sleep(time.Second)
			goto Upload
		}

		return "", err
	}

	return m.Document.FileID, nil
}

// requestTelegramFile requests the file targeted by the id from the Telegram.
// It returns `fs.ErrNotExist` if not found.
func (tgs *TGStore) requestTelegramFile(
	ctx context.Context,
	id string,
	header http.Header,
) (*http.Response, error) {
Ready:
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	fileURL, err := tgs.bot.FileURLByID(id)
	if err != nil {
		if strings.Contains(err.Error(), "Not Found") {
			return nil, fs.ErrNotExist
		}

		if isRetryableTelegramBotAPIError(err) {
			time.Sleep(time.Second)
			goto Ready
		}

		return nil, err
	}

Request:
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		fileURL,
		nil,
	)
	if err != nil {
		return nil, err
	}

	if header != nil {
		req.Header = header
	}

	res, err := tgs.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode >= http.StatusBadRequest {
		switch res.StatusCode {
		case http.StatusBadRequest,
			http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			res.Body.Close()
			time.Sleep(time.Second)
			goto Request
		}

		b, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("GET %s: %s: %s", req.URL, res.Status, b)
	}

	return res, nil
}

// downloadTelegramFile downloads the file targeted by the id from the Telegram.
// It returns `fs.ErrNotExist` if not found.
func (tgs *TGStore) downloadTelegramFile(
	ctx context.Context,
	aead cipher.AEAD,
	id string,
	offset int64,
) (io.ReadCloser, error) {
	offsetChunkCount := offset / telegramFileChunkSize
	offset -= offsetChunkCount * telegramFileChunkSize

	var reqHeader http.Header
	if offset > 0 {
		reqHeader = http.Header{}
		reqHeader.Set("Range", fmt.Sprintf(
			"bytes=%d-",
			offsetChunkCount*telegramFileEncryptedChunkSize,
		))
	}

	res, err := tgs.requestTelegramFile(ctx, id, reqHeader)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() (err error) {
		defer func() {
			pw.CloseWithError(err)
			res.Body.Close()
		}()

		buf := make([]byte, telegramFileEncryptedChunkSize)
		for {
			n, err := io.ReadFull(res.Body, buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else if !errors.Is(err, io.ErrUnexpectedEOF) {
					return err
				}
			}

			nonce := buf[:chacha20poly1305.NonceSize]
			chunk := buf[chacha20poly1305.NonceSize:n]
			chunk, err = aead.Open(chunk[:0], nonce, chunk, nil)
			if err != nil {
				return err
			}

			if offset > 0 {
				chunk = chunk[offset:]
				offset = 0
			}

			if _, err := pw.Write(chunk); err != nil {
				return err
			}
		}

		return nil
	}()

	return pr, nil
}

// sizeTelegramFile sizes the file targeted by the id from the Telegram. It
// returns `fs.ErrNotExist` if not found.
func (tgs *TGStore) sizeTelegramFile(
	ctx context.Context,
	id string,
) (int64, error) {
	res, err := tgs.requestTelegramFile(ctx, id, nil)
	if err != nil {
		return 0, err
	}

	res.Body.Close()

	fullChunkCount := res.ContentLength / telegramFileEncryptedChunkSize
	size := fullChunkCount * telegramFileChunkSize
	if res.ContentLength%telegramFileEncryptedChunkSize != 0 {
		size += res.ContentLength -
			fullChunkCount*telegramFileEncryptedChunkSize -
			telegramFileEncryptedChunkSize +
			telegramFileChunkSize
	}

	return size, nil
}

// isRetryableTelegramBotAPIError reports whether the err is a retryable
// Telegram Bot API error.
func isRetryableTelegramBotAPIError(err error) bool {
	em := err.Error()
	return strings.Contains(em, "Bad Request") ||
		strings.Contains(em, "Too Many Requests") ||
		strings.Contains(em, "Internal Server Error") ||
		strings.Contains(em, "Bad Gateway") ||
		strings.Contains(em, "Service Unavailable") ||
		strings.Contains(em, "Gateway Timeout")
}

// readNonceMutex is the `sync.Mutex` for the `readNonce`.
var readNonceMutex sync.Mutex

// readNonce reads nonce to the b.
func readNonce(b []byte) error {
	readNonceMutex.Lock()
	defer readNonceMutex.Unlock()
	binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
	_, err := rand.Read(b[8:])
	return err
}

// countReader is used to count the number of bytes read from the underlying
// `io.Reader`.
type countReader struct {
	r io.Reader
	c int64
}

// Read implements the `io.Reader`.
func (cr *countReader) Read(b []byte) (int, error) {
	n, err := cr.r.Read(b)
	cr.c += int64(n)
	return n, err
}
