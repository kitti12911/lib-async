package async

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	watermillnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

type job struct {
	ID string `json:"id"`
}

func TestPublishEncodesPayloadAndMetadata(t *testing.T) {
	t.Parallel()

	publisher := &testPublisher{}
	err := Publish(context.Background(), publisher, JSONCodec{}, "jobs.pdf", job{ID: "j1"}, map[string]string{
		"source": "test",
	})
	require.NoError(t, err)
	require.Len(t, publisher.messages, 1)
	require.JSONEq(t, `{"id":"j1"}`, string(publisher.messages[0].Payload))
	require.Equal(t, "test", publisher.messages[0].Metadata.Get("source"))
}

func TestPublishReturnsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Publish(ctx, &testPublisher{}, JSONCodec{}, "jobs.pdf", job{ID: "j1"})
	require.ErrorIs(t, err, context.Canceled)
}

func TestPublishReturnsMarshalError(t *testing.T) {
	t.Parallel()

	err := Publish(context.Background(), &testPublisher{}, JSONCodec{}, "jobs.pdf", make(chan int))
	require.ErrorContains(t, err, "marshal publish payload")
}

func TestPublishReturnsPublisherError(t *testing.T) {
	t.Parallel()

	expected := errors.New("publish failed")
	err := Publish(context.Background(), &testPublisher{publishErr: expected}, JSONCodec{}, "jobs.pdf", job{ID: "j1"})
	require.ErrorIs(t, err, expected)
}

func TestBusAccessorsAndRawConsume(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := message.NewMessage("m1", []byte(`"cmF3"`))
	publisher := &testPublisher{}
	subscriber := newTestSubscriber(msg)
	bus := NewBus(publisher, subscriber)

	require.Same(t, publisher, bus.Publisher())
	require.Same(t, subscriber, bus.Subscriber())

	err := bus.Consume(ctx, "jobs.raw", func(_ context.Context, envelope Envelope[[]byte]) error {
		require.Equal(t, []byte("raw"), envelope.Payload)
		cancel()
		return nil
	})
	require.NoError(t, err)
	requireAcked(t, msg)
}

func TestConsumeAcksHandledMessages(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := message.NewMessage("m1", []byte(`{"id":"j1"}`))
	subscriber := newTestSubscriber(msg)

	err := Consume(ctx, subscriber, JSONCodec{}, "jobs.pdf", func(_ context.Context, envelope Envelope[job]) error {
		require.Equal(t, "m1", envelope.UUID)
		require.Equal(t, "j1", envelope.Payload.ID)
		cancel()
		return nil
	})
	require.NoError(t, err)
	requireAcked(t, msg)
}

func TestConsumeDefaultsCodecAndReturnsWhenContextIsCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Consume[job](ctx, newTestSubscriber(), nil, "jobs.pdf", func(context.Context, Envelope[job]) error {
		t.Fatal("handler should not run")
		return nil
	})
	require.NoError(t, err)
}

func TestConsumeReturnsSubscribeError(t *testing.T) {
	t.Parallel()

	expected := errors.New("subscribe failed")
	err := Consume[job](
		context.Background(),
		&testSubscriber{subscribeErr: expected},
		JSONCodec{},
		"jobs.pdf",
		func(context.Context, Envelope[job]) error {
			t.Fatal("handler should not run")
			return nil
		},
	)
	require.ErrorIs(t, err, expected)
}

func TestConsumeNacksDecodeErrorsAndContinues(t *testing.T) {
	t.Parallel()

	msg := message.NewMessage("m1", []byte(`{`))
	var handled Envelope[[]byte]
	var handledErr error

	err := Consume[job](
		context.Background(),
		newTestSubscriber(msg),
		JSONCodec{},
		"jobs.pdf",
		func(context.Context, Envelope[job]) error {
			t.Fatal("handler should not run")
			return nil
		},
		WithErrorHandler(func(_ context.Context, envelope Envelope[[]byte], err error) {
			handled = envelope
			handledErr = err
		}),
	)
	require.NoError(t, err)
	requireNacked(t, msg)
	require.Equal(t, "m1", handled.UUID)
	require.Equal(t, []byte(`{`), handled.Payload)
	require.ErrorContains(t, handledErr, "unmarshal message")
}

func TestConsumeNacksHandlerErrors(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	msg := message.NewMessage("m1", []byte(`{"id":"j1"}`))
	subscriber := newTestSubscriber(msg)

	expected := errors.New("fail")
	err := Consume(
		ctx,
		subscriber,
		JSONCodec{},
		"jobs.pdf",
		func(context.Context, Envelope[job]) error {
			return expected
		},
		WithStopOnError(true),
	)
	require.ErrorIs(t, err, expected)
	requireNacked(t, msg)
}

func TestBusCloseHandlesPublisherAndSubscriber(t *testing.T) {
	t.Parallel()

	publisher := &testPublisher{}
	subscriber := newTestSubscriber()
	bus := NewBus(publisher, subscriber)

	require.NoError(t, bus.Close())
	require.True(t, publisher.closed)
	require.True(t, subscriber.closed)
}

func TestBusCloseAllowsMissingPublisherOrSubscriber(t *testing.T) {
	t.Parallel()

	publisher := &testPublisher{}
	require.NoError(t, NewBus(nil, nil).Close())
	require.NoError(t, NewBus(publisher, nil).Close())
	require.True(t, publisher.closed)
}

func TestBusCloseReturnsPublisherError(t *testing.T) {
	t.Parallel()

	expected := errors.New("close publisher failed")
	err := NewBus(&testPublisher{closeErr: expected}, newTestSubscriber()).Close()
	require.ErrorIs(t, err, expected)
}

func TestBusCloseReturnsSubscriberError(t *testing.T) {
	t.Parallel()

	expected := errors.New("close subscriber failed")
	err := NewBus(&testPublisher{}, &testSubscriber{closeErr: expected}).Close()
	require.ErrorIs(t, err, expected)
}

func TestJSONCodecReturnsErrors(t *testing.T) {
	t.Parallel()

	_, err := JSONCodec{}.Marshal(make(chan int))
	require.ErrorContains(t, err, "json marshal")

	var value job
	err = JSONCodec{}.Unmarshal([]byte(`{`), &value)
	require.ErrorContains(t, err, "json unmarshal")
}

func TestNewNATSRequiresURL(t *testing.T) {
	t.Parallel()

	bus, err := NewNATS(NATSConfig{}, nil)
	require.Nil(t, bus)
	require.ErrorContains(t, err, "nats url is required")
}

func TestNewNATSCreatesBus(t *testing.T) {
	restoreNATSFactories(t)

	publisher := &testPublisher{}
	subscriber := newTestSubscriber()
	codec := &recordingCodec{}

	var publisherConfig watermillnats.PublisherConfig
	var subscriberConfig watermillnats.SubscriberConfig
	newNATSPublisher = func(config watermillnats.PublisherConfig, _ watermill.LoggerAdapter) (message.Publisher, error) {
		publisherConfig = config
		return publisher, nil
	}
	newNATSSubscriber = func(config watermillnats.SubscriberConfig, _ watermill.LoggerAdapter) (message.Subscriber, error) {
		subscriberConfig = config
		return subscriber, nil
	}

	bus, err := NewNATS(NATSConfig{
		URL:              "nats://example:4222",
		Username:         "user",
		Password:         "password",
		QueueGroupPrefix: "workers",
		SubscribersCount: 2,
		CloseTimeout:     time.Second,
		AckWaitTimeout:   2 * time.Second,
		SubscribeTimeout: 3 * time.Second,
		JetStream: JetStream{
			Enabled:       true,
			AutoProvision: true,
			TrackMsgID:    true,
			AckAsync:      true,
			DurablePrefix: "workers",
		},
	}, nil, WithCodec(codec))
	require.NoError(t, err)
	require.Same(t, publisher, bus.Publisher())
	require.Same(t, subscriber, bus.Subscriber())
	require.Same(t, codec, bus.codec)
	require.Equal(t, "nats://example:4222", publisherConfig.URL)
	require.Len(t, publisherConfig.NatsOptions, 1)
	require.False(t, publisherConfig.JetStream.Disabled)
	require.True(t, publisherConfig.JetStream.AutoProvision)
	require.True(t, publisherConfig.JetStream.TrackMsgId)
	require.True(t, publisherConfig.JetStream.AckAsync)
	require.Equal(t, "workers", publisherConfig.JetStream.DurablePrefix)
	require.Equal(t, "workers", subscriberConfig.QueueGroupPrefix)
	require.Equal(t, 2, subscriberConfig.SubscribersCount)
	require.Equal(t, time.Second, subscriberConfig.CloseTimeout)
	require.Equal(t, 2*time.Second, subscriberConfig.AckWaitTimeout)
	require.Equal(t, 3*time.Second, subscriberConfig.SubscribeTimeout)
	require.Len(t, subscriberConfig.NatsOptions, 1)
}

func TestNewNATSReturnsPublisherError(t *testing.T) {
	restoreNATSFactories(t)

	expected := errors.New("publisher failed")
	newNATSPublisher = func(watermillnats.PublisherConfig, watermill.LoggerAdapter) (message.Publisher, error) {
		return nil, expected
	}

	bus, err := NewNATS(NATSConfig{URL: "nats://example:4222"}, testLogger{})
	require.Nil(t, bus)
	require.ErrorIs(t, err, expected)
}

func TestNewNATSClosesPublisherWhenSubscriberFails(t *testing.T) {
	restoreNATSFactories(t)

	publisher := &testPublisher{}
	expected := errors.New("subscriber failed")
	newNATSPublisher = func(watermillnats.PublisherConfig, watermill.LoggerAdapter) (message.Publisher, error) {
		return publisher, nil
	}
	newNATSSubscriber = func(watermillnats.SubscriberConfig, watermill.LoggerAdapter) (message.Subscriber, error) {
		return nil, expected
	}

	bus, err := NewNATS(NATSConfig{URL: "nats://example:4222"}, testLogger{})
	require.Nil(t, bus)
	require.ErrorIs(t, err, expected)
	require.True(t, publisher.closed)
}

func TestNATSFactoriesReturnConnectionErrors(t *testing.T) {
	t.Parallel()

	publisher, err := createNATSPublisher(watermillnats.PublisherConfig{
		URL: "nats://127.0.0.1:0",
	}, testLogger{})
	require.Nil(t, publisher)
	require.ErrorContains(t, err, "connect")

	subscriber, err := createNATSSubscriber(watermillnats.SubscriberConfig{
		URL: "nats://127.0.0.1:0",
	}, testLogger{})
	require.Nil(t, subscriber)
	require.ErrorContains(t, err, "connect")
}

func TestNATSFactoryResultsReturnSuccessfulAdapters(t *testing.T) {
	t.Parallel()

	publisher := &testPublisher{}
	gotPublisher, err := natsPublisherResult(publisher, nil)
	require.NoError(t, err)
	require.Same(t, publisher, gotPublisher)

	subscriber := newTestSubscriber()
	gotSubscriber, err := natsSubscriberResult(subscriber, nil)
	require.NoError(t, err)
	require.Same(t, subscriber, gotSubscriber)
}

type testPublisher struct {
	messages   []*message.Message
	publishErr error
	closeErr   error
	closed     bool
}

func (p *testPublisher) Publish(_ string, messages ...*message.Message) error {
	if p.publishErr != nil {
		return p.publishErr
	}
	p.messages = append(p.messages, messages...)
	return nil
}

func (p *testPublisher) Close() error {
	p.closed = true
	return p.closeErr
}

type testSubscriber struct {
	ch           chan *message.Message
	subscribeErr error
	closeErr     error
	closed       bool
}

func newTestSubscriber(messages ...*message.Message) *testSubscriber {
	ch := make(chan *message.Message, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	close(ch)
	return &testSubscriber{ch: ch}
}

func (s *testSubscriber) Subscribe(context.Context, string) (<-chan *message.Message, error) {
	if s.subscribeErr != nil {
		return nil, s.subscribeErr
	}
	return s.ch, nil
}

func (s *testSubscriber) Close() error {
	s.closed = true
	return s.closeErr
}

type recordingCodec struct{}

func (recordingCodec) Marshal(any) ([]byte, error) {
	return []byte(`null`), nil
}

func (recordingCodec) Unmarshal([]byte, any) error {
	return nil
}

type testLogger struct{}

func (testLogger) Error(string, error, watermill.LogFields) {}
func (testLogger) Info(string, watermill.LogFields)         {}
func (testLogger) Debug(string, watermill.LogFields)        {}
func (testLogger) Trace(string, watermill.LogFields)        {}
func (testLogger) With(watermill.LogFields) watermill.LoggerAdapter {
	return testLogger{}
}

func restoreNATSFactories(t *testing.T) {
	t.Helper()

	originalPublisher := newNATSPublisher
	originalSubscriber := newNATSSubscriber
	t.Cleanup(func() {
		newNATSPublisher = originalPublisher
		newNATSSubscriber = originalSubscriber
	})
}

func requireAcked(t *testing.T, msg *message.Message) {
	t.Helper()

	select {
	case <-msg.Acked():
	case <-time.After(time.Second):
		t.Fatal("message was not acked")
	}
}

func requireNacked(t *testing.T, msg *message.Message) {
	t.Helper()

	select {
	case <-msg.Nacked():
	case <-time.After(time.Second):
		t.Fatal("message was not nacked")
	}
}
