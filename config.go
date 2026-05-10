package async

import "time"

type NATSConfig struct {
	URL              string        `mapstructure:"url"               env:"NATS_URL" validate:"required"`
	Username         string        `mapstructure:"username"          env:"NATS_USERNAME"`
	Password         string        `mapstructure:"password"          env:"NATS_PASSWORD"`
	QueueGroupPrefix string        `mapstructure:"queue_group_prefix" env:"NATS_QUEUE_GROUP_PREFIX"`
	SubscribersCount int           `mapstructure:"subscribers_count" env:"NATS_SUBSCRIBERS_COUNT"`
	CloseTimeout     time.Duration `mapstructure:"close_timeout"     env:"NATS_CLOSE_TIMEOUT"`
	AckWaitTimeout   time.Duration `mapstructure:"ack_wait_timeout"  env:"NATS_ACK_WAIT_TIMEOUT"`
	SubscribeTimeout time.Duration `mapstructure:"subscribe_timeout" env:"NATS_SUBSCRIBE_TIMEOUT"`
	JetStream        JetStream     `mapstructure:"jetstream"`
}

type JetStream struct {
	Enabled       bool   `mapstructure:"enabled"        env:"NATS_JETSTREAM_ENABLED"`
	AutoProvision bool   `mapstructure:"auto_provision" env:"NATS_JETSTREAM_AUTO_PROVISION"`
	TrackMsgID    bool   `mapstructure:"track_msg_id"   env:"NATS_JETSTREAM_TRACK_MSG_ID"`
	AckAsync      bool   `mapstructure:"ack_async"      env:"NATS_JETSTREAM_ACK_ASYNC"`
	DurablePrefix string `mapstructure:"durable_prefix" env:"NATS_JETSTREAM_DURABLE_PREFIX"`
}
