package async

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/kitti12911/lib-async"

type Option func(*options)

type options struct {
	codec      Codec
	propagator propagation.TextMapPropagator
	tracer     trace.Tracer
}

func defaultOptions() options {
	return options{
		codec:      JSONCodec{},
		propagator: propagation.TraceContext{},
		tracer:     otel.Tracer(instrumentationName),
	}
}

func WithCodec(codec Codec) Option {
	return func(opts *options) {
		if codec != nil {
			opts.codec = codec
		}
	}
}

func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return func(opts *options) {
		if propagator != nil {
			opts.propagator = propagator
		}
	}
}

func WithTracer(tracer trace.Tracer) Option {
	return func(opts *options) {
		if tracer != nil {
			opts.tracer = tracer
		}
	}
}
