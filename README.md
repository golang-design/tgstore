# TGStore

[![PkgGoDev](https://pkg.go.dev/badge/golang.design/x/tgstore)](https://pkg.go.dev/golang.design/x/tgstore)

An encrypted object storage system with unlimited space backed by Telegram.

Please only upload what you really need to upload, don't abuse any system.

## Features

* Extremely easy to use
	* One function: [`tgstore.New`](https://pkg.go.dev/golang.design/x/tgstore#New)
	* One struct: [`tgstore.TGStore`](https://pkg.go.dev/golang.design/x/tgstore#TGStore)
		* [`tgstore.TGStore.Upload`](https://pkg.go.dev/golang.design/x/tgstore#TGStore.Upload)
		* [`tgstore.TGStore.Download`](https://pkg.go.dev/golang.design/x/tgstore#TGStore.Download)
* Unlimited storage space
* Up to 50 TiB or more (depending on the [`tgstore.TGStore.MaxFileBytes`](https://pkg.go.dev/golang.design/x/tgstore#TGStore.MaxFileBytes)) per object
* Crazy upload and download speed (try concurrency to make it happen)

## Installation

Open your terminal and execute

```bash
$ go get golang.design/x/tgstore
```

done.

> The only requirement is the [Go](https://golang.org), at least v1.16.

## Hello, 世界

Create a file named `hello.go`

```go
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"golang.design/x/tgstore"
	"golang.org/x/crypto/chacha20poly1305"
)

func main() {
	tgs := tgstore.New()
	tgs.BotToken = "<your-telegram-bot-token"
	tgs.ChatID = 1234567890

	objectSecretKey := make([]byte, chacha20poly1305.KeySize)
	if _, err := rand.Read(objectSecretKey); err != nil {
		log.Fatal(err)
	}

	startTime := time.Now()

	objectID, err := tgs.Upload(
		context.TODO(),
		objectSecretKey,
		strings.NewReader("Hello, 世界"),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Upload time:", time.Since(startTime))

	startTime = time.Now()

	objectReader, err := tgs.Download(
		context.TODO(),
		objectSecretKey,
		object.ID,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Download time:", time.Since(startTime))

	startTime = time.Now()

	b, err := io.ReadAll(objectReader)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Read time:", time.Since(startTime))

	fmt.Println("Content:", string(b))
}
```

and run it

```bash
$ go run hello.go
```

then check what your terminal outputs.

## Community

If you want to discuss TGStore, or ask questions about it, simply post questions
or ideas [here](https://github.com/golang-design/tgstore/issues).

## Contributing

If you want to help build TGStore, simply follow
[this](https://github.com/golang-design/tgstore/wiki/Contributing) to send pull
requests [here](https://github.com/golang-design/tgstore/pulls).

## License

This project is licensed under the MIT License.

License can be found [here](LICENSE).
