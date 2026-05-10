package async

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	watermillnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
)

var (
	newNATSPublisher  = createNATSPublisher
	newNATSSubscriber = createNATSSubscriber
)

func createNATSPublisher(config watermillnats.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
	publisher, err := watermillnats.NewPublisher(config, logger)
	return natsPublisherResult(publisher, err)
}

func createNATSSubscriber(config watermillnats.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	subscriber, err := watermillnats.NewSubscriber(config, logger)
	return natsSubscriberResult(subscriber, err)
}

func natsPublisherResult(publisher message.Publisher, err error) (message.Publisher, error) {
	if err != nil {
		return nil, fmt.Errorf("create nats publisher: %w", err)
	}
	return publisher, nil
}

func natsSubscriberResult(subscriber message.Subscriber, err error) (message.Subscriber, error) {
	if err != nil {
		return nil, fmt.Errorf("create nats subscriber: %w", err)
	}
	return subscriber, nil
}

func NewNATS(cfg NATSConfig, logger watermill.LoggerAdapter, opts ...Option) (*Bus, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("async: nats url is required")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	cfgOptions := options{
		codec: JSONCodec{},
	}
	for _, opt := range opts {
		opt(&cfgOptions)
	}

	natsOptions := make([]nats.Option, 0, 1)
	if cfg.Username != "" || cfg.Password != "" {
		natsOptions = append(natsOptions, nats.UserInfo(cfg.Username, cfg.Password))
	}

	jetStream := watermillnats.JetStreamConfig{
		Disabled:      !cfg.JetStream.Enabled,
		AutoProvision: cfg.JetStream.AutoProvision,
		TrackMsgId:    cfg.JetStream.TrackMsgID,
		AckAsync:      cfg.JetStream.AckAsync,
		DurablePrefix: cfg.JetStream.DurablePrefix,
	}

	publisher, err := newNATSPublisher(watermillnats.PublisherConfig{
		URL:         cfg.URL,
		NatsOptions: natsOptions,
		JetStream:   jetStream,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("async: create nats publisher: %w", err)
	}

	subscriber, err := newNATSSubscriber(watermillnats.SubscriberConfig{
		URL:              cfg.URL,
		QueueGroupPrefix: cfg.QueueGroupPrefix,
		SubscribersCount: cfg.SubscribersCount,
		CloseTimeout:     cfg.CloseTimeout,
		AckWaitTimeout:   cfg.AckWaitTimeout,
		SubscribeTimeout: cfg.SubscribeTimeout,
		NatsOptions:      natsOptions,
		JetStream:        jetStream,
	}, logger)
	if err != nil {
		_ = publisher.Close()
		return nil, fmt.Errorf("async: create nats subscriber: %w", err)
	}

	return NewBus(publisher, subscriber, WithCodec(cfgOptions.codec)), nil
}
