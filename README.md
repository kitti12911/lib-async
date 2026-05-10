# lib-async

Shared event-driven messaging helpers for homelab services.

The public API is intentionally small: publish JSON payloads, consume typed
payloads, and let the library handle ack/nack. The current transport adapter is
NATS with optional JetStream, built on Watermill so the service code can stay
mostly transport-neutral.

## project structure

```bash
lib-async/
├── async.go      # typed publish/consume helpers and bus wrapper
├── codec.go      # codec interface and JSON codec implementation
├── config.go     # NATS and JetStream configuration
├── nats.go       # Watermill NATS adapter wiring
├── options.go    # shared library options
├── Makefile
├── go.mod
└── README.md
```

## install

```bash
go get github.com/kitti12911/lib-async
```

## NATS / JetStream

```go
bus, err := async.NewNATS(async.NATSConfig{
    URL:              "nats://nats.database.svc.cluster.local:4222",
    Username:         "example_user",
    Password:         "example_password",
    QueueGroupPrefix: "worker-sandbox",
    AckWaitTimeout:   30 * time.Second,
    JetStream: async.JetStream{
        Enabled:       true,
        AutoProvision: true,
        TrackMsgID:    true,
        DurablePrefix: "worker-sandbox",
    },
}, nil)
if err != nil {
    return err
}
defer bus.Close()
```

Publish a typed payload:

```go
err = async.Publish(ctx, bus.Publisher(), async.JSONCodec{}, "jobs.pdf", PDFJob{
    ID: "job-1",
})
```

Consume and ack only after the handler succeeds:

```go
err = async.Consume(ctx, bus.Subscriber(), async.JSONCodec{}, "jobs.pdf", func(ctx context.Context, msg async.Envelope[PDFJob]) error {
    return generatePDF(ctx, msg.Payload)
})
```

## tracing

`Publish` starts a producer span and injects W3C trace context into message
metadata. `Consume` extracts that metadata, starts a consumer span, and passes
the connected context into the handler.

By default the library uses the OpenTelemetry tracer named
`github.com/kitti12911/lib-async` and the W3C Trace Context propagator. Services
can override those with `async.WithTracer(...)` and
`async.WithPropagator(...)`.

## delivery modes

- Core NATS is useful for fire-and-forget events. Ack calls are effectively
  no-ops there, so messages can be lost if a worker dies at the wrong time.
- JetStream tracks delivery state. A message is acked only after the handler
  succeeds, so messages are redelivered after failures or timeouts. This avoids
  loss, but handlers must be idempotent because duplicates are possible.

## why Watermill

Watermill already models publisher/subscriber, ack/nack, middleware, retries,
CQRS helpers, and many transports. This library keeps a small homelab-friendly
surface and uses Watermill underneath instead of rebuilding that ecosystem.

## requirements

- go 1.26 or higher

Optional:

- [prettier](https://prettier.io/) for Markdown, YAML, JSON, and JSONC formatting
- [golangci-lint](https://golangci-lint.run/) for `make lint`
- [markdownlint-cli2](https://github.com/DavidAnson/markdownlint-cli2) for `make lint`

## available commands

| Command       | Description                                     |
| ------------- | ----------------------------------------------- |
| `make tidy`   | Run `go mod tidy`                               |
| `make lint`   | Run Go and Markdown linting                     |
| `make vet`    | Run `go vet ./...`                              |
| `make fmt`    | Format Go code with `go fmt`                    |
| `make pretty` | Format Markdown, YAML, JSON, and JSONC          |
| `make format` | Run Go and document/config formatting           |
| `make test`   | Run tests with the race detector                |
| `make cov`    | Generate and open an HTML coverage report       |
| `make fix`    | Apply standard Go source rewrites with `go fix` |
