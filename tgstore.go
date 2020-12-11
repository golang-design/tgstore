/*
Package tgstore implements an encrypted object storage system with unlimited
space backed by Telegram.
*/
package tgstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"

	"gopkg.in/tucnak/telebot.v2"
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

	// MaxMessageFileBytes is the maximum number of bytes allowed for a
	// Telegram message file to have.
	//
	// The `MaxMessageFileBytes` must be at least 20971520.
	//
	// It is ok to change the `MaxMessageFileBytes` if you want. The objects
	// that have already been uploaded are not affected.
	//
	// Default value: 20971492
	MaxMessageFileBytes int `mapstructure:"max_message_file_bytes"`

	// MaxUploadWorkers is the maximum number of goroutines allowed for the
	// object chunk uploading to use at the same time.
	//
	// The `MaxUploadWorkers` must be at least 1.
	//
	// The runtime memory usage will increase significantly when the
	// `MaxUploadWorkers` is greater than 1.
	//
	// Default value: 1
	MaxUploadWorkers int `mapstructure:"max_upload_workers"`

	// HTTPClient is the `http.Client` used to communicate with the Telegram
	// Bot API.
	//
	// Default value: `http.DefaultClient`
	HTTPClient *http.Client `mapstructure:"-"`

	loadOnce  sync.Once
	loadError error
	bot       *telebot.Bot
	chat      *telebot.Chat
}

// New returns a new instance of the `TGStore` with default field values.
//
// The `New` is the only function that creates new instances of the `TGStore`
// and keeps everything working.
func New() *TGStore {
	return &TGStore{
		BotAPIEndpoint:      "https://api.telegram.org",
		MaxMessageFileBytes: 20 << 20,
		MaxUploadWorkers:    1,
		HTTPClient:          http.DefaultClient,
	}
}

// load loads the stuff of the tgs up.
func (tgs *TGStore) load() {
	if tgs.bot, tgs.loadError = telebot.NewBot(telebot.Settings{
		Token:    tgs.BotToken,
		Reporter: func(error) {},
		Client:   tgs.HTTPClient,
	}); tgs.loadError != nil {
		return
	}

	if tgs.chat, tgs.loadError = tgs.bot.ChatByID(
		strconv.FormatInt(tgs.ChatID, 10),
	); tgs.loadError != nil {
		return
	}

	if tgs.MaxMessageFileBytes < 20<<20 {
		tgs.loadError = errors.New("invalid max message file bytes")
		return
	}

	if tgs.MaxUploadWorkers < 1 {
		tgs.loadError = errors.New("invalid max upload workers")
		return
	}
}

// UploadObject uploades the content to the cloud.
//
// The lenth of the key must be 16.
func (tgs *TGStore) UploadObject(
	ctx context.Context,
	key []byte,
	content io.Reader,
) (*Object, error) {
	return tgs.AppendObject(ctx, "", key, content)
}

// AppendObject appends the content to the object targeted by the id.
//
// The lenth of the key must be 16.
func (tgs *TGStore) AppendObject(
	ctx context.Context,
	id string,
	key []byte,
	content io.Reader,
) (*Object, error) {
	tgs.loadOnce.Do(tgs.load)
	if tgs.loadError != nil {
		return nil, tgs.loadError
	}

	if content == nil {
		content = bytes.NewReader(nil)
	}

	buf := bytes.Buffer{}
	for {
		if _, err := io.CopyN(&buf, content, 1); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		object, err := tgs.appendObject(
			ctx,
			id,
			key,
			io.LimitReader(
				io.MultiReader(&buf, content),
				int64(tgs.MaxMessageFileBytes/
					objectEncryptedChunkSize*
					objectChunkSize),
			),
		)
		if err != nil {
			return nil, err
		}

		id = object.ID
	}

	if id != "" {
		return tgs.DownloadObject(ctx, id, key)
	}

	return tgs.appendObject(ctx, id, key, content)
}

// appendObject appends the content to the object targeted by the id.
//
// The lenth of the key must be 16.
func (tgs *TGStore) appendObject(
	ctx context.Context,
	id string,
	key []byte,
	content io.Reader,
) (*Object, error) {
	return nil, errors.New("not implemented")
}

// DownloadObject downloads the object targeted by the id from the cloud. It
// returns `os.ErrNotExist` if not found.
//
// The lenth of the key must be 16.
func (tgs *TGStore) DownloadObject(
	ctx context.Context,
	id string,
	key []byte,
) (*Object, error) {
	tgs.loadOnce.Do(tgs.load)
	if tgs.loadError != nil {
		return nil, tgs.loadError
	}

	return nil, errors.New("not implemented")
}
