package main

import (
	"context"
	"strconv"

	"github.com/mmfshirokan/PriceProvider/internal/config"
	"github.com/mmfshirokan/PriceProvider/internal/provider"
	log "github.com/sirupsen/logrus"
)

const (
	numberOfSymbols = 20
)

func main() {
	log.Info("Starting price provider")

	conf := config.New()
	symbols := NewSymbols(numberOfSymbols)
	ctx := context.Background()
	forever := make(chan struct{})

	provide := provider.New(conf.KafkaURL, conf.KafkaTopic)

	for _, val := range symbols {
		go provide.Write(ctx, val)
	}

	log.Info("Price provider working...")
	<-forever
}

func NewSymbols(numberOfSymbols int) []string {
	symbols := make([]string, numberOfSymbols)
	for i := range symbols {
		symbols[i] = "symbol" + strconv.FormatInt(int64(i), 10)
	}

	return symbols
}
