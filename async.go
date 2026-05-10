package async

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Bus struct {
	publisher  message.Publisher
	subscriber message.Subscriber
	codec      Codec
}

type Envelope[T any] struct {
	UUID     string
	Metadata message.Metadata
	Payload  T
}

type Handler[T any] func(context.Context, Envelope[T]) error

type ConsumeOption func(*consumeOptions)

type consumeOptions struct {
	stopOnError  bool
	errorHandler func(context.Context, Envelope[[]byte], error)
}

func NewBus(publisher message.Publisher, subscriber message.Subscriber, opts ...Option) *Bus {
	cfgOptions := options{
		codec: JSONCodec{},
	}
	for _, opt := range opts {
		opt(&cfgOptions)
	}

	return &Bus{
		publisher:  publisher,
		subscriber: subscriber,
		codec:      cfgOptions.codec,
	}
}

func (b *Bus) Publish(ctx context.Context, topic string, payload any, metadata ...map[string]string) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("async: publish context: %w", ctx.Err())
	default:
	}

	data, err := b.codec.Marshal(payload)
	if err != nil {
		return fmt.Errorf("async: marshal publish payload: %w", err)
	}

	msg := message.NewMessage(watermill.NewUUID(), data)
	for _, values := range metadata {
		for key, value := range values {
			msg.Metadata.Set(key, value)
		}
	}

	if err := b.publisher.Publish(topic, msg); err != nil {
		return fmt.Errorf("async: publish %q: %w", topic, err)
	}

	return nil
}

func (b *Bus) Publisher() message.Publisher {
	return b.publisher
}

func (b *Bus) Subscriber() message.Subscriber {
	return b.subscriber
}

func (b *Bus) Consume(ctx context.Context, topic string, handler Handler[[]byte], opts ...ConsumeOption) error {
	return Consume(ctx, b.subscriber, b.codec, topic, handler, opts...)
}

func (b *Bus) Close() error {
	if b.publisher != nil {
		if err := b.publisher.Close(); err != nil {
			return fmt.Errorf("async: close publisher: %w", err)
		}
	}
	if b.subscriber == nil {
		return nil
	}
	if err := b.subscriber.Close(); err != nil {
		return fmt.Errorf("async: close subscriber: %w", err)
	}
	return nil
}

func Publish[T any](
	ctx context.Context,
	publisher message.Publisher,
	codec Codec,
	topic string,
	payload T,
	metadata ...map[string]string,
) error {
	return NewBus(publisher, nil, WithCodec(codec)).Publish(ctx, topic, payload, metadata...)
}

func Consume[T any](
	ctx context.Context,
	subscriber message.Subscriber,
	codec Codec,
	topic string,
	handler Handler[T],
	opts ...ConsumeOption,
) error {
	cfgOptions := consumeOptions{}
	for _, opt := range opts {
		opt(&cfgOptions)
	}

	messages, err := subscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("async: subscribe %q: %w", topic, err)
	}

	if codec == nil {
		codec = JSONCodec{}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-messages:
			if !ok {
				return nil
			}
			if err := handleMessage(ctx, codec, msg, handler); err != nil {
				if cfgOptions.errorHandler != nil {
					cfgOptions.errorHandler(ctx, rawEnvelope(msg), err)
				}
				msg.Nack()
				if cfgOptions.stopOnError {
					return err
				}
				continue
			}
			msg.Ack()
		}
	}
}

func WithStopOnError(enabled bool) ConsumeOption {
	return func(opts *consumeOptions) {
		opts.stopOnError = enabled
	}
}

func WithErrorHandler(handler func(context.Context, Envelope[[]byte], error)) ConsumeOption {
	return func(opts *consumeOptions) {
		opts.errorHandler = handler
	}
}

func handleMessage[T any](
	ctx context.Context,
	codec Codec,
	msg *message.Message,
	handler Handler[T],
) error {
	var payload T
	if err := codec.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("async: unmarshal message %q: %w", msg.UUID, err)
	}

	if err := handler(ctx, Envelope[T]{
		UUID:     msg.UUID,
		Metadata: msg.Metadata,
		Payload:  payload,
	}); err != nil {
		return fmt.Errorf("async: handle message %q: %w", msg.UUID, err)
	}

	return nil
}

func rawEnvelope(msg *message.Message) Envelope[[]byte] {
	return Envelope[[]byte]{
		UUID:     msg.UUID,
		Metadata: msg.Metadata,
		Payload:  msg.Payload,
	}
}
