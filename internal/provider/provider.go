package provider

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type provider struct {
	symbols   []string
	brokerURL string
	topic     string
}

type Writer interface {
	Write(ctx context.Context)
}

func New(symbols []string, brokerURL string, topic string) Writer {
	return &provider{
		symbols:   symbols,
		brokerURL: brokerURL,
		topic:     topic,
	}
}

func (provide *provider) Write(ctx context.Context) {
	wrt := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{provide.brokerURL},
		Topic:    provide.topic,
		Async:    true,
		Balancer: &kafka.RoundRobin{},
	})

	ch := make(chan []int)
	go providePrices(ch)

	for {
		for _, symbol := range provide.symbols {
			candle := <-ch
			val := strconv.FormatInt(int64(candle[0]), 10) + ";" + strconv.FormatInt(int64(candle[1]), 10) + ";" + strconv.FormatInt(int64(candle[2]), 10) + ";" + strconv.FormatInt(int64(candle[3]), 10)

			err := wrt.WriteMessages(ctx, kafka.Message{
				Key:   []byte(symbol),
				Value: []byte(val),
			})
			if err != nil {
				log.Errorf("Error writing message: %v.\n", err)
				return
			}
		}
		time.Sleep(time.Second)
	}
}

func providePrices(ch chan []int) {
	const spread int = 11
	var shift int = 0

	for {
		shift += provideShift()
		ch <- provideCandle(spread, shift)
	}
}

func provideCandle(spread int, shift int) []int {
	candle := make([]int, 4)
	for i := range candle {
		candle[i] = rand.Intn(spread) + shift
	}

	sort.Slice(candle, func(i, j int) bool {
		return candle[i] < candle[j]
	})

	return candle
}

func provideShift() (shift int) {
	shift = rand.Intn(2)
	if shift == 0 {
		shift = -1
		return
	}

	return
}
