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
	"strings"
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
	noContentObjectID = "0AAAAAAAAAAAAAAAAAAAAAA"
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
	// Default value: ""
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
	AppAPIID int64 `mapstructure:"app_api_id"`

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
	ChannelID int64 `mapstructure:"channel_id"`

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
		strconv.FormatInt(tgs.AppAPIID, 10),
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
			int32(tgs.AppAPIID),
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
// If the size < 0, the `Upload` will read from the content until it reaches an
// `io.EOF`. But it should be noted that this will take up the local disk space,
// usually up to 2000 MiB for each upload operation. Therefore, a positive size
// should be provided if possible.
//
// Note that the returned id is URL safe (`^[A-Za-z0-9-_]{23}$`).
func (tgs *TGStore) Upload(
	ctx context.Context,
	secretKey []byte,
	content io.Reader,
	size int64,
) (string, error) {
	tgs.loadOnce.Do(tgs.load)
	if tgs.loadError != nil {
		return "", tgs.loadError
	}

	aead, err := chacha20poly1305.New(secretKey)
	if err != nil {
		return "", err
	}

	if content == nil || size == 0 {
		return noContentObjectID, nil
	}

	var tgTempFileIDs []string
	defer func() {
		if len(tgTempFileIDs) > 0 {
			tgs.deleteTGFiles(
				context.Background(),
				tgTempFileIDs...,
			)
		}
	}()

	metadata := objectMetadata{
		PartSize: 4000 * 512 * 1024 /
			tgFileEncryptedChunkSize *
			tgFileChunkSize,
	}

	if nonceCounter := uint64(1); size > 0 {
		partSize := metadata.PartSize
		for remainingSize := size; remainingSize > 0; {
			if remainingSize < partSize {
				partSize = remainingSize
			}

			tgfID, err := tgs.uploadTGFile(
				ctx,
				aead,
				0,
				&nonceCounter,
				io.LimitReader(content, partSize),
				partSize,
			)
			if err != nil {
				return "", err
			}

			tgTempFileIDs = append(tgTempFileIDs, tgfID)

			metadata.PartIDs = append(metadata.PartIDs, tgfID)
			metadata.Size += partSize

			remainingSize -= partSize
		}
	} else {
		partFile, err := os.CreateTemp("", "tgstore-object-part")
		if err != nil {
			return "", err
		}
		defer os.Remove(partFile.Name())

		for buf := bytes.NewBuffer(nil); ; {
			if _, err := io.CopyN(buf, content, 1); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return "", err
			}

			if _, err := partFile.Seek(
				0,
				io.SeekStart,
			); err != nil {
				return "", err
			}

			n, err := io.Copy(
				partFile,
				io.LimitReader(
					io.MultiReader(buf, content),
					metadata.PartSize,
				),
			)
			if err != nil {
				return "", err
			}

			if _, err := partFile.Seek(
				0,
				io.SeekStart,
			); err != nil {
				return "", err
			}

			tgfID, err := tgs.uploadTGFile(
				ctx,
				aead,
				0,
				&nonceCounter,
				partFile,
				n,
			)
			if err != nil {
				return "", err
			}

			tgTempFileIDs = append(tgTempFileIDs, tgfID)

			metadata.PartIDs = append(metadata.PartIDs, tgfID)
			metadata.Size += n
		}
	}

	var id string
	switch len(metadata.PartIDs) {
	case 0:
		return noContentObjectID, nil
	case 1:
		metadata.PartSize = metadata.Size

		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return "", err
		}

		id = fmt.Sprint("0", metadata.PartIDs[0])
		tgs.objectMetadataCache.Set(id, metadataJSON)
	default:
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

		tgfID, err := tgs.uploadTGFile(
			ctx,
			aead,
			1,
			nil,
			&gzippedMetadataJSON,
			int64(gzippedMetadataJSON.Len()),
		)
		if err != nil {
			return "", err
		}

		tgTempFileIDs = append(tgTempFileIDs, tgfID)

		id = fmt.Sprint("1", tgfID)
		tgs.objectMetadataCache.Set(id, metadataJSON)
	}

	tgTempFileIDs = nil

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

		reader.metadata.PartSize = reader.metadata.Size

		metadataJSON, err := json.Marshal(reader.metadata)
		if err != nil {
			return nil, err
		}

		tgs.objectMetadataCache.Set(id, metadataJSON)
	case '1':
		tgfrc, err := tgs.downloadTGFile(ctx, aead, 1, nil, id[1:], 0)
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
func (tgs *TGStore) tgChannelAccessHash(
	ctx context.Context,
	id int32,
) (int64, error) {
	if ahi, ok := tgs.channelAccessHashes.Load(id); ok {
		return ahi.(int64), nil
	}

	cs, err := tgs.client.ChannelsGetChannels([]telegram.InputChannel{
		&telegram.InputChannelObj{
			ChannelID: int32(tgs.ChannelID),
		},
	})
	if err != nil {
		switch {
		case strings.Contains(
			err.Error(),
			"The provided channel is invalid",
		):
			return 0, fs.ErrNotExist
		case strings.Contains(
			err.Error(),
			"You haven't joined this channel/supergroup",
		):
			return 0, fs.ErrNotExist
		case strings.Contains(
			err.Error(),
			"Invalid message ID provided",
		):
			return 0, fs.ErrNotExist
		}

		return 0, err
	}

	c, ok := cs.(*telegram.MessagesChatsObj).Chats[0].(*telegram.Channel)
	if !ok {
		return 0, fs.ErrNotExist
	}

	tgs.channelAccessHashes.Store(id, c.AccessHash)

	return c.AccessHash, nil
}

// tgDocument returns the `telegram.DocumentObj` targeted by the fileID. It
// returns `fs.ErrNotExist` if not found.
func (tgs *TGStore) tgDocument(
	ctx context.Context,
	fileID string,
) (*telegram.DocumentObj, error) {
	fileIDBytes, err := base64.RawURLEncoding.DecodeString(fileID)
	if err != nil {
		return nil, err
	}

	channelID := int32(binary.BigEndian.Uint64(fileIDBytes[:8]))
	messageID := int32(binary.BigEndian.Uint64(fileIDBytes[8:]))

	channelAccessHash, err := tgs.tgChannelAccessHash(ctx, channelID)
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
		switch {
		case strings.Contains(
			err.Error(),
			"The provided channel is invalid",
		):
			return nil, fs.ErrNotExist
		case strings.Contains(
			err.Error(),
			"You haven't joined this channel/supergroup",
		):
			return nil, fs.ErrNotExist
		case strings.Contains(
			err.Error(),
			"No message ids were provided",
		):
		case strings.Contains(
			err.Error(),
			"Invalid message ID provided",
		):
			return nil, fs.ErrNotExist
		}

		return nil, err
	}

	firstMessage, ok := message.(*telegram.MessagesChannelMessages).
		Messages[0].(*telegram.MessageObj)
	if !ok {
		return nil, fs.ErrNotExist
	}

	return firstMessage.
		Media.(*telegram.MessageMediaDocument).
		Document.(*telegram.DocumentObj), nil
}

// uploadTGFile uploads the content to the Telegram.
func (tgs *TGStore) uploadTGFile(
	ctx context.Context,
	aead cipher.AEAD,
	noncePrefix uint32,
	nonceCounter *uint64,
	content io.Reader,
	size int64,
) (string, error) {
	const partSize = 512 << 10

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

		binary.LittleEndian.PutUint32(nonce[:4], noncePrefix)
		if nonceCounter == nil {
			nonceCounter = new(uint64)
			*nonceCounter = 1
		}

		for ; ; *nonceCounter++ {
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

			binary.LittleEndian.PutUint64(nonce[4:], *nonceCounter)

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

	fullChunkCount := size / tgFileChunkSize
	encryptedSize := fullChunkCount * tgFileEncryptedChunkSize
	if size%tgFileChunkSize != 0 {
		encryptedSize += size -
			fullChunkCount*tgFileChunkSize -
			tgFileChunkSize +
			tgFileEncryptedChunkSize
	}

	randomFileIDBytes := make([]byte, 8)
	if _, err := rand.Read(randomFileIDBytes); err != nil {
		return "", err
	}

	randomFileID := int64(binary.BigEndian.Uint64(randomFileIDBytes))

	partCount := int32(encryptedSize / partSize)
	if encryptedSize%partSize != 0 {
		partCount++
	}

	for i, buf := int32(0), make([]byte, partSize); i < partCount; i++ {
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

		if _, err := tgs.client.UploadSaveBigFilePart(
			randomFileID,
			i,
			partCount,
			buf[:n],
		); err != nil {
			return "", err
		}
	}

	channelAccessHash, err := tgs.tgChannelAccessHash(
		ctx,
		int32(tgs.ChannelID),
	)
	if err != nil {
		return "", err
	}

	update, err := tgs.client.MessagesSendMedia(
		&telegram.MessagesSendMediaParams{
			Silent: true,
			Peer: &telegram.InputPeerChannel{
				ChannelID:  int32(tgs.ChannelID),
				AccessHash: channelAccessHash,
			},
			Media: &telegram.InputMediaUploadedDocument{
				File: &telegram.InputFileBig{
					ID:    randomFileID,
					Parts: partCount,
				},
				MimeType: "application/octet-stream",
			},
			RandomID: randomFileID,
		},
	)
	if err != nil {
		return "", err
	}

	idBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(idBytes[:8], uint64(tgs.ChannelID))
	binary.BigEndian.PutUint64(
		idBytes[8:],
		uint64(update.(*telegram.UpdatesObj).
			Updates[0].(*telegram.UpdateMessageID).
			ID),
	)

	return base64.RawURLEncoding.EncodeToString(idBytes), nil
}

// downloadTGFile downloads the file targeted by the id from the Telegram. It
// returns `fs.ErrNotExist` if not found.
func (tgs *TGStore) downloadTGFile(
	ctx context.Context,
	aead cipher.AEAD,
	noncePrefix uint32,
	nonceCounter *uint64,
	id string,
	offset int64,
) (io.ReadCloser, error) {
	offsetChunkCount := offset / tgFileChunkSize
	offset -= offsetChunkCount * tgFileChunkSize

	document, err := tgs.tgDocument(ctx, id)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() (err error) {
		defer func() {
			pw.CloseWithError(err)
		}()

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

		binary.LittleEndian.PutUint32(nonce[:4], noncePrefix)
		if nonceCounter == nil {
			nonceCounter = new(uint64)
			*nonceCounter = 1
		}

		*nonceCounter += uint64(offsetChunkCount)
		for ; ; *nonceCounter++ {
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

			binary.LittleEndian.PutUint64(nonce[4:], *nonceCounter)

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
	document, err := tgs.tgDocument(ctx, id)
	if err != nil {
		return 0, err
	}

	fullChunkCount := document.Size / tgFileEncryptedChunkSize
	size := fullChunkCount * tgFileChunkSize
	if document.Size%tgFileEncryptedChunkSize != 0 {
		size += document.Size -
			fullChunkCount*tgFileEncryptedChunkSize -
			tgFileEncryptedChunkSize +
			tgFileChunkSize
	}

	return int64(size), nil
}

// deleteTGFiles deletes the files targeted by the ids from the Telegram.
func (tgs *TGStore) deleteTGFiles(ctx context.Context, ids ...string) error {
	messageIDs := map[int32][]int32{}
	for _, id := range ids {
		idBytes, err := base64.RawURLEncoding.DecodeString(id)
		if err != nil {
			return err
		}

		channelID := int32(binary.BigEndian.Uint64(idBytes[:8]))
		messageID := int32(binary.BigEndian.Uint64(idBytes[8:]))

		messageIDs[channelID] = append(messageIDs[channelID], messageID)
	}

	for channelID, messageIDs := range messageIDs {
		channelAccessHash, err := tgs.tgChannelAccessHash(
			ctx,
			channelID,
		)
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
