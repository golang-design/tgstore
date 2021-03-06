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
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/xelaj/mtproto/telegram"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/poly1305"
)

const (
	// tgFileChunkSize is the chunk size of Telegram file.
	tgFileChunkSize = 64 << 10

	// tgFileEncryptedChunkSize is the encrypted chunk size of Telegram
	// file.
	tgFileEncryptedChunkSize = tgFileChunkSize + poly1305.TagSize

	// noContentObjectID is the ID of an object with no content.
	noContentObjectID = "0AAAAAAAAAAA"
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
	// MTProtoServerHost is the MTProto server host.
	//
	// Available values:
	//   * 149.154.175.58:443 (Miami Florida, United States)
	//   * 149.154.167.50:443 (Amsterdam North Holland, Netherlands)
	//   * 149.154.175.100:443 (Miami Florida, United States)
	//   * 149.154.167.91:443 (Amsterdam North Holland, Netherlands)
	//   * 91.108.56.151:443 (Singapore, Singapore)
	//
	// Default value: "149.154.175.58:443"
	MTProtoServerHost string `mapstructure:"mtproto_server_host"`

	// MTProtoPublicKeys is the MTProto public keys.
	//
	// The `MTProtoPublicKeys` is from https://my.telegram.org/apps.
	//
	// Default value: ""
	MTProtoPublicKeys string `mapstructure:"mtproto_public_keys"`

	// AppAPIID is the Telegram app API ID.
	//
	// The `AppAPIID` is from https://my.telegram.org/apps.
	//
	// Default value: 0
	AppAPIID int32 `mapstructure:"app_api_id"`

	// AppAPIHash is the Telegram app API hash.
	//
	// The `AppAPIHash` is from https://my.telegram.org/apps.
	//
	// Default value: ""
	AppAPIHash string `mapstructure:"app_api_hash"`

	// BotToken is the Telegram bot token.
	//
	// The Telegram bot targeted by the `BotToken` must have at least
	// "Post Messages" permission in the channel targeted by the `ChannelID`
	// to upload objects.
	//
	// Default value: ""
	BotToken string `mapstructure:"bot_token"`

	// ChannelID is the ID of the Telegram channel used to store the objects
	// to be uploaded.
	//
	// It is ok to change the `ChannelID` if you want. The objects that have
	// already been uploaded are not affected.
	//
	// Default value: 0
	ChannelID int32 `mapstructure:"channel_id"`

	// MaxObjectMetadataCacheBytes is the maximum number of bytes allowed
	// for object metadata cache to use.
	//
	// The `MaxObjectMetadataCacheBytes` must be at least 1048576 (1 MiB).
	//
	// Default value: 67108864
	MaxObjectMetadataCacheBytes int `mapstructure:"max_object_metadata_cache_bytes"`

	loadOnce            sync.Once
	loadError           error
	client              *telegram.Client
	channelAccessHashes sync.Map
	objectMetadataCache *bigcache.BigCache
}

// New returns a new instance of the `TGStore` with default field values.
//
// The `New` is the only function that creates new instances of the `TGStore`
// and keeps everything working.
func New() *TGStore {
	return &TGStore{
		MTProtoServerHost:           "149.154.175.58:443",
		MaxObjectMetadataCacheBytes: 64 << 20,
	}
}

// load loads the stuff of the tgs up.
func (tgs *TGStore) load() {
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		tgs.loadError = fmt.Errorf(
			"failed to get user cache dir: %v",
			err,
		)
		return
	}

	appDir := filepath.Join(
		userCacheDir,
		"tgstore",
		strconv.FormatInt(int64(tgs.AppAPIID), 10),
	)
	if _, err := os.Stat(appDir); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			tgs.loadError = fmt.Errorf(
				"failed to stat app dir: %v",
				err,
			)
			return
		}

		if err := os.MkdirAll(appDir, 0700); err != nil {
			tgs.loadError = fmt.Errorf(
				"failed to make app dir: %v",
				err,
			)
			return
		}
	}

	publicKeysFile := filepath.Join(appDir, "public_keys.pem")
	if _, err := os.Stat(publicKeysFile); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			tgs.loadError = fmt.Errorf(
				"failed to stat mtproto public keys file: %v",
				err,
			)
			return
		}

		if err := os.WriteFile(
			publicKeysFile,
			[]byte(tgs.MTProtoPublicKeys),
			0666,
		); err != nil {
			tgs.loadError = fmt.Errorf(
				"failed to write mtproto public keys file: %v",
				err,
			)
			return
		}
	}

	sessionFile := filepath.Join(
		appDir,
		fmt.Sprintf(
			"%x-session.json",
			sha256.Sum256([]byte(tgs.BotToken)),
		),
	)

	client, err := telegram.NewClient(telegram.ClientConfig{
		SessionFile:    sessionFile,
		ServerHost:     tgs.MTProtoServerHost,
		PublicKeysFile: publicKeysFile,
		AppID:          int(tgs.AppAPIID),
		AppHash:        tgs.AppAPIHash,
	})
	if err != nil {
		tgs.loadError = fmt.Errorf(
			"failed to create telegram client: %v",
			err,
		)
		return
	}

	tgs.client = client

	if _, err := os.Stat(sessionFile); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			tgs.loadError = fmt.Errorf(
				"failed to stat mtproto session file: %v",
				err,
			)
			return
		}

		if _, err := client.AuthImportBotAuthorization(
			1,
			tgs.AppAPIID,
			tgs.AppAPIHash,
			tgs.BotToken,
		); err != nil {
			tgs.loadError = fmt.Errorf(
				"failed to auth telegram bot: %v",
				err,
			)
			return
		}
	}

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
// The lenth of the secretKey must be 16.
//
// Note that the returned id is URL safe (`^[A-Za-z0-9-_]{12}$`).
func (tgs *TGStore) Upload(
	ctx context.Context,
	secretKey []byte,
	content io.Reader,
) (string, error) {
	const splitSize = 4000 * 512 * 1024 /
		tgFileEncryptedChunkSize *
		tgFileChunkSize

	tgs.loadOnce.Do(tgs.load)
	if tgs.loadError != nil {
		return "", tgs.loadError
	}

	aead, err := chacha20poly1305.New(secretKey)
	if err != nil {
		return "", err
	}

	if content == nil {
		return noContentObjectID, nil
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
				splitSize,
			),
		}

		tgfID, err := tgs.uploadTGFile(ctx, aead, part)
		if err != nil {
			return "", err
		}

		metadata.PartIDs = append(metadata.PartIDs, tgfID)
		metadata.Size += part.c
	}

	switch len(metadata.PartIDs) {
	case 0:
		return noContentObjectID, nil
	case 1:
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return "", err
		}

		id := fmt.Sprint("0", metadata.PartIDs[0])
		tgs.objectMetadataCache.Set(id, metadataJSON)

		return id, nil
	}

	metadata.SplitSize = splitSize

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

	tgfID, err := tgs.uploadTGFile(ctx, aead, &gzippedMetadataJSON)
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
	case noContentObjectID:
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
		if reader.metadata.Size, err = tgs.sizeTGFile(
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
		tgfrc, err := tgs.downloadTGFile(ctx, aead, id[1:], 0)
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

// Delete deletes the object targeted by the id from the cloud.
//
// The lenth of the secretKey must be 16.
func (tgs *TGStore) Delete(
	ctx context.Context,
	secretKey []byte,
	id string,
) error {
	reader, err := tgs.Download(ctx, secretKey, id)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}

		return err
	} else if err := reader.Close(); err != nil {
		return err
	}

	tgs.objectMetadataCache.Delete(id)

	metadata := reader.(*objectReader).metadata
	switch len(metadata.PartIDs) {
	case 0:
		return nil
	case 1:
		return tgs.deleteTGFiles(ctx, metadata.PartIDs[0])
	}

	return tgs.deleteTGFiles(ctx, append(metadata.PartIDs, id[1:])...)
}

// tgChannelAccessHash returns the access hash of the Telegram channel targeted
// by the id. It returns `fs.ErrNotExist` if not found.
func (tgs *TGStore) tgChannelAccessHash(id int32) (int64, error) {
	if ahi, ok := tgs.channelAccessHashes.Load(id); ok {
		return ahi.(int64), nil
	}

	cs, err := tgs.client.ChannelsGetChannels([]telegram.InputChannel{
		&telegram.InputChannelObj{
			ChannelID: tgs.ChannelID,
		},
	})
	if err != nil {
		return 0, err
	}

	c, ok := cs.(*telegram.MessagesChatsObj).Chats[0].(*telegram.Channel)
	if !ok {
		return 0, fs.ErrNotExist
	}

	tgs.channelAccessHashes.Store(id, c.AccessHash)

	return c.AccessHash, nil
}

// uploadTGFile uploads the content to the Telegram.
func (tgs *TGStore) uploadTGFile(
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
			buf   = make([]byte, tgFileEncryptedChunkSize)
			nonce = make([]byte, chacha20poly1305.NonceSize)
		)

		for counter := uint64(1); ; counter++ {
			n, err := io.ReadFull(
				content,
				buf[:tgFileChunkSize],
			)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else if !errors.Is(err, io.ErrUnexpectedEOF) {
					return err
				}
			}

			binary.LittleEndian.PutUint64(nonce[4:], counter)

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

	randomFileIDBytes := make([]byte, 8)
	if _, err := rand.Read(randomFileIDBytes); err != nil {
		return "", err
	}

	randomFileID := int64(binary.BigEndian.Uint64(randomFileIDBytes))

	var (
		partCount int32
		buf       = make([]byte, 512<<10)
	)

	for totalParts := int32(4000); ; partCount++ {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		n, err := io.ReadFull(pr, buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else if !errors.Is(err, io.ErrUnexpectedEOF) {
				return "", err
			}
		}

		if partCount == 0 && n < 512<<10 {
			totalParts = 1
		}

		if _, err := tgs.client.UploadSaveBigFilePart(
			randomFileID,
			partCount,
			totalParts,
			buf[:n],
		); err != nil {
			return "", err
		}
	}

	channelAccessHash, err := tgs.tgChannelAccessHash(tgs.ChannelID)
	if err != nil {
		return "", err
	}

	update, err := tgs.client.MessagesSendMedia(
		&telegram.MessagesSendMediaParams{
			Silent: true,
			Peer: &telegram.InputPeerChannel{
				ChannelID:  tgs.ChannelID,
				AccessHash: channelAccessHash,
			},
			Media: &telegram.InputMediaUploadedDocument{
				File: &telegram.InputFileBig{
					ID:    randomFileID,
					Parts: partCount,
				},
				MimeType: "application/octet-stream",
			},
			Message:  "",
			RandomID: randomFileID,
		},
	)
	if err != nil {
		return "", err
	}

	fileIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint32(fileIDBytes, uint32(tgs.ChannelID))
	binary.BigEndian.PutUint32(
		fileIDBytes[4:],
		uint32(update.(*telegram.UpdatesObj).
			Updates[0].(*telegram.UpdateMessageID).
			ID),
	)

	return base64.RawURLEncoding.EncodeToString(fileIDBytes), nil
}

// downloadTGFile downloads the file targeted by the id from the Telegram. It
// returns `fs.ErrNotExist` if not found.
func (tgs *TGStore) downloadTGFile(
	ctx context.Context,
	aead cipher.AEAD,
	id string,
	offset int64,
) (io.ReadCloser, error) {
	offsetChunkCount := offset / tgFileChunkSize
	offset -= offsetChunkCount * tgFileChunkSize

	fileIDBytes, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return nil, err
	}

	channelID := int32(binary.BigEndian.Uint32(fileIDBytes[:4]))
	messageID := int32(binary.BigEndian.Uint32(fileIDBytes[4:]))

	channelAccessHash, err := tgs.tgChannelAccessHash(channelID)
	if err != nil {
		return nil, err
	}

	message, err := tgs.client.ChannelsGetMessages(
		&telegram.InputChannelObj{
			ChannelID:  channelID,
			AccessHash: channelAccessHash,
		},
		[]telegram.InputMessage{
			&telegram.InputMessageID{
				ID: messageID,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() (err error) {
		defer func() {
			pw.CloseWithError(err)
		}()

		document := message.(*telegram.MessagesChannelMessages).
			Messages[0].(*telegram.MessageObj).
			Media.(*telegram.MessageMediaDocument).
			Document.(*telegram.DocumentObj)

		fileLocation := &telegram.InputDocumentFileLocation{
			ID:            document.ID,
			AccessHash:    document.AccessHash,
			FileReference: document.FileReference,
		}

		offset := int32(offsetChunkCount * tgFileEncryptedChunkSize)
		for offset < document.Size {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			file, err := tgs.client.UploadGetFile(
				&telegram.UploadGetFileParams{
					Precise:  true,
					Location: fileLocation,
					Offset:   offset,
					Limit:    1 << 20,
				},
			)
			if err != nil {
				return err
			}

			fileContent := file.(*telegram.UploadFileObj).Bytes
			if _, err := pw.Write(fileContent); err != nil {
				return err
			}

			offset += int32(len(fileContent))
		}

		return nil
	}()

	pr2, pw2 := io.Pipe()
	go func() (err error) {
		defer func() {
			pw2.CloseWithError(err)
			pr.CloseWithError(err)
		}()

		var (
			buf   = make([]byte, tgFileEncryptedChunkSize)
			nonce = make([]byte, chacha20poly1305.NonceSize)
		)

		for counter := uint64(offsetChunkCount + 1); ; counter++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			n, err := io.ReadFull(pr, buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else if !errors.Is(err, io.ErrUnexpectedEOF) {
					return err
				}
			}

			binary.LittleEndian.PutUint64(nonce[4:], counter)

			chunk, err := aead.Open(buf[:0], nonce, buf[:n], nil)
			if err != nil {
				return err
			}

			if offset > 0 {
				chunk = chunk[offset:]
				offset = 0
			}

			if _, err := pw2.Write(chunk); err != nil {
				return err
			}
		}

		return nil
	}()

	return pr2, nil
}

// sizeTGFile sizes the file targeted by the id from the Telegram. It returns
// `fs.ErrNotExist` if not found.
func (tgs *TGStore) sizeTGFile(ctx context.Context, id string) (int64, error) {
	fileIDBytes, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return 0, err
	}

	channelID := int32(binary.BigEndian.Uint32(fileIDBytes[:4]))
	messageID := int32(binary.BigEndian.Uint32(fileIDBytes[4:]))

	channelAccessHash, err := tgs.tgChannelAccessHash(channelID)
	if err != nil {
		return 0, err
	}

	message, err := tgs.client.ChannelsGetMessages(
		&telegram.InputChannelObj{
			ChannelID:  channelID,
			AccessHash: channelAccessHash,
		},
		[]telegram.InputMessage{
			&telegram.InputMessageID{
				ID: messageID,
			},
		},
	)
	if err != nil {
		return 0, err
	}

	return int64(message.(*telegram.MessagesChannelMessages).
		Messages[0].(*telegram.MessageObj).
		Media.(*telegram.MessageMediaDocument).
		Document.(*telegram.DocumentObj).
		Size), nil
}

// deleteTGFiles deletes the files targeted by the ids from the Telegram.
func (tgs *TGStore) deleteTGFiles(ctx context.Context, ids ...string) error {
	messageIDs := map[int32][]int32{}
	for _, id := range ids {
		fileIDBytes, err := base64.RawURLEncoding.DecodeString(id)
		if err != nil {
			return err
		}

		channelID := int32(binary.BigEndian.Uint32(fileIDBytes[:4]))
		messageID := int32(binary.BigEndian.Uint32(fileIDBytes[4:]))

		messageIDs[channelID] = append(messageIDs[channelID], messageID)
	}

	for channelID, messageIDs := range messageIDs {
		channelAccessHash, err := tgs.tgChannelAccessHash(channelID)
		if err != nil {
			return err
		}

		if _, err := tgs.client.ChannelsDeleteMessages(
			&telegram.InputChannelObj{
				ChannelID:  channelID,
				AccessHash: channelAccessHash,
			},
			messageIDs,
		); err != nil {
			return err
		}
	}

	return nil
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
