package provider

//first make priceServic/priceProvider

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

var (
	wrt              Writer
	kafkaHostAndPort string
)

const (
	topic = "prices"
)

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Errorf("Could not construct pool: %s", err)
		return
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Errorf("Could not connect to Docker: %s", err)
		return
	}

	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: "zookeeper_kafka_network"})
	if err != nil {
		log.Errorf("could not create a network to zookeeper and kafka: %s", err)
		return
	}

	zookeeperResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         "zookeeper-test",
		Repository:   "wurstmeister/zookeeper",
		Tag:          "latest",
		NetworkID:    network.ID,
		Hostname:     "zookeeper",
		ExposedPorts: []string{"2181"},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Errorf("Could not start zookeeper: %s", err)
		return
	}

	conn, _, err := zk.Connect([]string{fmt.Sprintf("127.0.0.1:%s", zookeeperResource.GetPort("2181/tcp"))}, 60*time.Second) // 10 Seconds
	if err != nil {
		log.Errorf("could not connect zookeeper: %s", err)
		return
	}
	defer conn.Close()

	retryFn := func() error {
		switch conn.State() {
		case zk.StateHasSession, zk.StateConnected:
			return nil
		default:
			return errors.New("not yet connected")
		}
	}

	if err = pool.Retry(retryFn); err != nil {
		log.Errorf("could not connect to zookeeper: %s", err)
		return
	}

	kafkaResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "kafka-test",
		Repository: "wurstmeister/kafka",
		Tag:        "latest",
		NetworkID:  network.ID,
		Hostname:   "kafka",
		Env: []string{
			"KAFKA_CREATE_TOPICS=" + topic + ":1:1:compact",
			"KAFKA_ADVERTISED_LISTENERS=INSIDE://kafka:9092,OUTSIDE://localhost:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT",
			"KAFKA_LISTENERS=INSIDE://0.0.0.0:9092,OUTSIDE://0.0.0.0:9093",
			"KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
			"KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9093/tcp": {{HostIP: "localhost", HostPort: "9093/tcp"}},
		},
		ExposedPorts: []string{"9093/tcp"},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("could not start kafka: %s", err)
	}

	kafkaHostAndPort = kafkaResource.GetHostPort("9093/tcp")
	if kafkaHostAndPort == "" {
		log.Fatalf("could not get kafka hostAndPort")
	}

	wrt = New(kafkaHostAndPort, topic)

	code := m.Run()

	if err = pool.Purge(zookeeperResource); err != nil {
		log.Fatalf("could not purge zookeeperResource: %s", err)
	}

	if err = pool.Purge(kafkaResource); err != nil {
		log.Fatalf("could not purge kafkaResource: %s", err)
	}

	if err = pool.Client.RemoveNetwork(network.ID); err != nil {
		log.Fatalf("could not remove %s network: %s", network.Name, err)
	}

	os.Exit(code)
}

func TestWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	testTable := []struct {
		name    string
		symbol  string
		context context.Context
	}{
		{
			name:    "standart input with symbol=apple",
			symbol:  "apple",
			context: ctx,
		},
	}

	for _, test := range testTable {
		go wrt.Write(test.context, test.symbol)

		rdr := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{kafkaHostAndPort},
			Topic:     topic,
			Partition: 0,
		})

		for i := 0; i < 4; i++ {
			_, err := rdr.ReadMessage(test.context)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

}
