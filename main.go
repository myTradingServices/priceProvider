package main

import (
	"context"
	"strconv"

	"github.com/mmfshirokan/GoProject2/internal/config"
	"github.com/mmfshirokan/GoProject2/internal/provider"
)

func main() {
	conf := config.NewConfig()
	symbols := NewSymbols()
	context := context.Background()
	forever := make(chan struct{})

	provider := provider.New(symbols, conf.KafkaURL, conf.KafkaTopic)

	go provider.Write(context)

	<-forever
}

func NewSymbols() []string {
	symbols := make([]string, 100)
	for i := range symbols {
		symbols[i] = "symbol" + strconv.FormatInt(int64(i), 10)
	}

	return symbols
}
