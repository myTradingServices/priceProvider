package main

import (
	"context"
	"strconv"

	"github.com/mmfshirokan/PriceProvider/internal/config"
	"github.com/mmfshirokan/PriceProvider/internal/provider"
)

func main() {
	conf := config.New()
	symbols := NewSymbols()
	ctx := context.Background()
	forever := make(chan struct{})

	provide := provider.New(symbols, conf.KafkaURL, conf.KafkaTopic)

	go provide.Write(ctx)

	<-forever
}

func NewSymbols() []string {
	symbols := make([]string, 100)
	for i := range symbols {
		symbols[i] = "symbol" + strconv.FormatInt(int64(i), 10)
	}

	return symbols
}
