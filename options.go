package async

type Option func(*options)

type options struct {
	codec Codec
}

func WithCodec(codec Codec) Option {
	return func(opts *options) {
		if codec != nil {
			opts.codec = codec
		}
	}
}
