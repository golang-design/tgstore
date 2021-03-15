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
		* [`tgstore.TGStore.Delete`](https://pkg.go.dev/golang.design/x/tgstore#TGStore.Delete)
* Unlimited storage space
* Up to N PiB per object (please never try to find out what N is, you don't need that)
* Crazy upload and download speed

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
	tgs.AppAPIID = 1234567890
	tgs.AppAPIHash = "<your-telegram-app-api-hash>"
	tgs.BotTokens = []string{"<your-telegram-bot-token>"}
	tgs.ChannelID = 1234567890

	objectSecretKey := make([]byte, chacha20poly1305.KeySize)
	if _, err := rand.Read(objectSecretKey); err != nil {
		log.Fatal(err)
	}

	objectContent := strings.NewReader("Hello, 世界")

	startTime := time.Now()

	objectID, err := tgs.Upload(
		context.TODO(),
		objectSecretKey,
		objectContent,
		objectContent.Size(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer tgs.Delete(context.TODO(), objectSecretKey, objectID)

	fmt.Println("Upload time:", time.Since(startTime))

	startTime = time.Now()

	objectReader, err := tgs.Download(
		context.TODO(),
		objectSecretKey,
		objectID,
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

	fmt.Print("Delete in")
	for i := 5; i > 0; i-- {
		fmt.Printf(" %ds...", i)
		time.Sleep(time.Second)
	}

	startTime = time.Now()

	if err := tgs.Delete(
		context.TODO(),
		objectSecretKey,
		objectID,
	); err != nil {
		fmt.Println()
		log.Fatal(err)
	}

	fmt.Println(" deleted!")

	fmt.Println("Delete time:", time.Since(startTime))
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
