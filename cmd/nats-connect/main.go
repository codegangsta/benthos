package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Import all plugins defined within the repo.
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure/extended"
)

func main() {
	builder := service.NewStreamBuilder()

	data, err := os.ReadFile("example.json")
	if err != nil {
		log.Fatalln(err)
	}

	config := make(map[string]any)

	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalln(err)
	}

	err = builder.SetConfig(config)
	if err != nil {
		log.Fatalln(err)
	}

	stream, err := builder.Build()
	if err != nil {
		log.Fatalln(err)
	}

	err = stream.Run(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
}
