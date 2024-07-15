package provider

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/mmfshirokan/PriceProvider/internal/model"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
)

type provider struct {
	brokerURL string
	topic     string
}

type Writer interface {
	Write(ctx context.Context, symbol string)
}

func New(brokerURL, topic string) Writer {
	return &provider{
		brokerURL: brokerURL,
		topic:     topic,
	}
}

func (provide *provider) Write(ctx context.Context, symbol string) {
	log.Info("Provider Write started")

	wrt := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{provide.brokerURL},
		Topic:    provide.topic,
		Async:    true,
		Balancer: &kafka.RoundRobin{},
	})

	tmpPrice := ProvidePrice(decimal.NewFromInt(rand.Int63n(100)))
	for {
		modelToSent := model.Price{
			Date:   time.Now(),
			Bid:    tmpPrice,
			Ask:    tmpPrice.Add(decimal.New(rand.Int63n(101)-49, -2)),
			Symbol: symbol,
		}

		jsonMarsheld, err := json.Marshal(modelToSent)
		if err != nil {
			log.Error(err)
			return
		}

		err = wrt.WriteMessages(ctx, kafka.Message{
			Key:   []byte(symbol),
			Value: jsonMarsheld,
		})
		if err != nil {
			log.Error("Error at writing message: ", err)
			//return
		} else {
			log.WithFields(log.Fields{
				"symbol": modelToSent.Symbol,
				"bid":    modelToSent.Bid,
				"ask":    modelToSent.Ask,
				"date":   modelToSent.Date,
			}).Info("Message sent")
		}

		tmpPrice = ProvidePrice(tmpPrice)
		if tmpPrice.LessThanOrEqual(decimal.New(0, 0)) {
			tmpPrice.Add(ProvidePrice(decimal.New(5, 0)))
		}

		time.Sleep(time.Second)
	}
}

func ProvidePrice(prevPrice decimal.Decimal) decimal.Decimal {
	return decimal.Sum(prevPrice, decimal.New(int64(rand.Intn(501))-250, -2))
}
