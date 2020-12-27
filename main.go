package main

import (
	"context"
	"github.com/gojekfarm/zhandlers/rabbitmq"
	"github.com/gojekfarm/ziggurat"
	"github.com/gojekfarm/ziggurat/mw"
)

func main() {
	z := &ziggurat.Ziggurat{}
	r := ziggurat.NewRouter()
	statusLogger := mw.NewProcessingStatusLogger()

	rmq := rabbitmq.NewRabbitRetrier(
		[]string{"amqp://user:bitnami@localhost:5672"},
		rabbitmq.QueueConfig{"plain-text-log": {DelayQueueExpirationInMS: "200", RetryCount: 2}},
		ziggurat.NewLogger("info"))

	r.HandleFunc("plain-text-log", func(event ziggurat.Event) ziggurat.ProcessStatus {
		return ziggurat.RetryMessage
	})
	handler := r.Compose(statusLogger.LogStatus, rmq.Retrier)

	z.StartFunc(func(ctx context.Context) {
		rmq.RunPublisher(ctx)
		rmq.RunConsumers(ctx, handler)
	})
	z.Run(context.Background(), handler, ziggurat.StreamRoutes{
		"plain-text-log": {
			BootstrapServers: "localhost:9092",
			OriginTopics:     "plain-text-log",
			ConsumerGroupID:  "plain_text_consumer",
			ConsumerCount:    1,
		},
	})
}
