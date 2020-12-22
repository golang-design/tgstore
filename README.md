# TGStore

[![PkgGoDev](https://pkg.go.dev/badge/golang.design/x/tgstore)](https://pkg.go.dev/golang.design/x/tgstore)

An encrypted object storage system with unlimited space backed by Telegram.

## Installation

Open your terminal and execute

```bash
$ go get golang.design/x/tgstore
```

done.

> The only requirement is the [Go](https://golang.org), at least v1.13.

## Hello, 世界

Create a file named `hello.go`

```go
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
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

	objectKey := make([]byte, chacha20poly1305.KeySize)
	if _, err := rand.Read(objectKey); err != nil {
		log.Fatal(err)
	}

	startTime := time.Now()

	object, err := tgs.Upload(
		context.TODO(),
		objectKey,
		strings.NewReader("Hello, 世界"),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Upload time:", time.Since(startTime))

	startTime = time.Now()

	downloadedObject, err := tgs.Download(
		context.TODO(),
		object.ID,
		objectKey,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Download time:", time.Since(startTime))

	startTime = time.Now()

	rc, err := downloadedObject.NewReader(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	b, err := ioutil.ReadAll(rc)
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
