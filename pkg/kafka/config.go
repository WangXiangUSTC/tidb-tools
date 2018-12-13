package kafka

import (
	"crypto/tls"
	"net"
	"time"
)

// Config is used to pass multiple configuration options to Sarama's constructors.
type Config struct {
	// Admin is the namespace for ClusterAdmin properties used by the administrative Kafka client.
	// The maximum duration the administrative Kafka client will wait for ClusterAdmin operations,
	// including topics, brokers, configurations and ACLs (defaults to 3 seconds).
	AdminTimeout int `toml:"admin-timeout"`

	// Net is the namespace for network-level properties used by the Broker, and
	// shared by the Client/Producer/Consumer.
	// How many outstanding requests a connection is allowed to have before
	// sending on it blocks (default 5).
	NetMaxOpenRequests int `toml:"net-max-open-requests"`

	// All three of the below configurations are similar to the
	// `socket.timeout.ms` setting in JVM kafka. All of them default
	// to 30 seconds.
	NetDialTimeout  int `toml:"net-dial-timeout"`  // How long to wait for the initial connection.
	NetReadTimeout  int `toml:"net-read-timeout"`  // How long to wait for a response.
	NetWriteTimeout int `toml:"net-write-timeout"` // How long to wait for a transmit.

	// Whether or not to use TLS when connecting to the broker
	// (defaults to false).
	NetTSLEnable bool `toml:"net-tsl-enable"`
	// The TLS configuration to use for secure connections if
	// enabled (defaults to nil).
	NetTSLConfig *tls.Config

	// SASL based authentication with broker. While there are multiple SASL authentication methods
	// the current implementation is limited to plaintext (SASL/PLAIN) authentication
	// Whether or not to use SASL authentication when connecting to the broker
	// (defaults to false).
	NetSASLEnable bool `toml:"net-sasl-enable"`
	// Whether or not to send the Kafka SASL handshake first if enabled
	// (defaults to true). You should only set this to false if you're using
	// a non-Kafka SASL proxy.
	NetSASLHandshake bool `toml:"net-sasl-handshake"`
	//username and password for SASL/PLAIN authentication
	NetSASLUser     string `toml:"net-sasl-user"`
	NetSASLPassword string `toml:"net-sasl-password"`

	// KeepAlive specifies the keep-alive period for an active network connection.
	// If zero, keep-alives are disabled. (default is 0: disabled).
	NetKeepAlive int `toml:"net-keepalive"`

	// LocalAddr is the local address to use when dialing an
	// address. The address must be of a compatible type for the
	// network being dialed.
	// If nil, a local address is automatically chosen.
	NetLocalAddr net.Addr

	// Metadata is the namespace for metadata management properties used by the
	// Client, and shared by the Producer/Consumer.

	// The total number of times to retry a metadata request when the
	// cluster is in the middle of a leader election (default 3).
	MetadataRetryMax int `toml:"metadata-retry-max"`

	// How long to wait for leader election to occur before retrying
	// (default 250ms). Similar to the JVM's `retry.backoff.ms`.
	MetadataRetryBackoff int `toml:"metadata-retry-backoff"`

	// How frequently to refresh the cluster metadata in the background.
	// Defaults to 10 minutes. Set to 0 to disable. Similar to
	// `topic.metadata.refresh.interval.ms` in the JVM version.
	MetadataRefreshFrequency int `toml:"metadata-refresh-frequency"`

	// Whether to maintain a full set of metadata for all topics, or just
	// the minimal set that has been necessary so far. The full set is simpler
	// and usually more convenient, but can take up a substantial amount of
	// memory if you have many topics and partitions. Defaults to true.
	MetadataFull bool `toml:"metadata-full"`

	// Producer is the namespace for configuration related to producing messages,
	// used by the Producer.
	// The maximum permitted size of a message (defaults to 1000000). Should be
	// set equal to or smaller than the broker's `message.max.bytes`.
	ProducerMaxMessageBytes int `toml:"producer-max-message-bytes"`

	// The level of acknowledgement reliability needed from the broker (defaults
	// to WaitForLocal). Equivalent to the `request.required.acks` setting of the
	// JVM producer.
	// value is 0: doesn't send any response, the TCP ACK is all you get.
	// value is 1: WaitForLocal waits for only the local commit to succeed before responding.
	// value is -1: waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	ProducerRequiredAcks int `toml:"producer-required-acks"`

	// The maximum duration the broker will wait the receipt of the number of
	// RequiredAcks (defaults to 10 seconds). This is only relevant when
	// RequiredAcks is set to WaitForAll or a number > 1. Only supports
	// millisecond resolution, nanoseconds will be truncated. Equivalent to
	// the JVM producer's `request.timeout.ms` setting.
	ProducerTimeout int `toml:"producer-timeout"`

	// The type of compression to use on messages (defaults to no compression).
	// Similar to `compression.codec` setting of the JVM producer.
	// 0 for None
	// 1 for GZIP
	// 2 for Snappy
	// 3 for LZ4
	// 4 for ZSTD
	ProducerCompression int `toml:"producer-compression"`

	// The level of compression to use on messages. The meaning depends
	// on the actual compression type used and defaults to default compression
	// level for the codec.
	ProducerCompressionLevel int `toml:"producer-compression-level"`

	// Generates partitioners for choosing the partition to send messages to
	// (defaults to hashing the message key). Similar to the `partitioner.class`
	// setting for the JVM producer.
	// ProducerPartitioner PartitionerConstructor

	// If enabled, the producer will ensure that exactly one copy of each message is
	// written.
	ProducerIdempotent bool `toml:"producer-idempotent"`

	// Return specifies what channels will be populated. If they are set to true,
	// you must read from the respective channels to prevent deadlock. If,
	// however, this config is used to create a `SyncProducer`, both must be set
	// to true and you shall not read from the channels since the producer does
	// this internally.
	// If enabled, successfully delivered messages will be returned on the
	// Successes channel (default disabled).
	ProducerReturnSuccesses bool `toml:"producer-return-successes"`

	// If enabled, messages that failed to deliver will be returned on the
	// Errors channel, including error (default enabled).
	ProducerReturnErrors bool `toml:"producer-return-errors"`

	// The following config options control how often messages are batched up and
	// sent to the broker. By default, messages are sent as fast as possible, and
	// all messages received while the current batch is in-flight are placed
	// into the subsequent batch.
	// The best-effort number of bytes needed to trigger a flush. Use the
	// global sarama.MaxRequestSize to set a hard upper limit.
	ProducerFlushBytes int `toml:"producer-flush-bytes"`
	// The best-effort number of messages needed to trigger a flush. Use
	// `MaxMessages` to set a hard upper limit.
	ProducerFlushMessages int `toml:"producer-flush-messages"`
	// The best-effort frequency of flushes. Equivalent to
	// `queue.buffering.max.ms` setting of JVM producer.
	ProducerFlushFrequency int `toml:"producer-flush-frequency"`
	// The maximum number of messages the producer will send in a single
	// broker request. Defaults to 0 for unlimited. Similar to
	// `queue.buffering.max.messages` in the JVM producer.
	ProducerFlushMaxMessages int `toml:"producer-flush-max-messages"`

	// The total number of times to retry sending a message (default 3).
	// Similar to the `message.send.max.retries` setting of the JVM producer.
	ProducerRetryMax int `toml:"producer-retry-max"`
	// How long to wait for the cluster to settle between retries
	// (default 100ms). Similar to the `retry.backoff.ms` setting of the
	// JVM producer.
	ProducerRetryBackoff int `toml:"producer-retry-backoff"`

	// Consumer is the namespace for configuration related to consuming messages,
	// used by the Consumer.

	// Group is the namespace for configuring consumer group.

	// The timeout used to detect consumer failures when using Kafka's group management facility.
	// The consumer sends periodic heartbeats to indicate its liveness to the broker.
	// If no heartbeats are received by the broker before the expiration of this session timeout,
	// then the broker will remove this consumer from the group and initiate a rebalance.
	// Note that the value must be in the allowable range as configured in the broker configuration
	// by `group.min.session.timeout.ms` and `group.max.session.timeout.ms` (default 10s)
	ConsumerGroupSessionTimeout int `toml:"consumer-group-session-timeout"`

	// The expected time between heartbeats to the consumer coordinator when using Kafka's group
	// management facilities. Heartbeats are used to ensure that the consumer's session stays active and
	// to facilitate rebalancing when new consumers join or leave the group.
	// The value must be set lower than Consumer.Group.Session.Timeout, but typically should be set no
	// higher than 1/3 of that value.
	// It can be adjusted even lower to control the expected time for normal rebalances (default 3s)
	ConsumerGroupHeartbeatInterval int `toml:"consumer-group-heartbeat-interval"`

	// Strategy for allocating topic partitions to members (default BalanceStrategyRange)
	// ConsumerGroupRebalanceStrategy BalanceStrategy
	
	// The maximum allowed time for each worker to join the group once a rebalance has begun.
	// This is basically a limit on the amount of time needed for all tasks to flush any pending
	// data and commit offsets. If the timeout is exceeded, then the worker will be removed from
	// the group, which will cause offset commit failures (default 60s).
	ConsumerGroupRebalanceTimeout int `toml:"consumer-group-rebalance-timeout"`

	// When a new consumer joins a consumer group the set of consumers attempt to "rebalance"
	// the load to assign partitions to each consumer. If the set of consumers changes while
	// this assignment is taking place the rebalance will fail and retry. This setting controls
	// the maximum number of attempts before giving up (default 4).
	ConsumerGroupRebalanceRetryMax int
	// Backoff time between retries during rebalance (default 2s)
	ConsumerGroupRebalanceRetryBackoff time.Duration

	// Custom metadata to include when joining the group. The user data for all joined members
	// can be retrieved by sending a DescribeGroupRequest to the broker that is the
	// coordinator for the group.
	ConsumerGroupMemberUserData string `toml:"consumer-group-member-user-data"`

	// How long to wait after a failing to read from a partition before
	// trying again (default 2s).
	ConsumerRetryBackoff int `toml:"consumer-retry-backoff"`

	// Fetch is the namespace for controlling how many bytes are retrieved by any
	// given request.
	// The minimum number of message bytes to fetch in a request - the broker
	// will wait until at least this many are available. The default is 1,
	// as 0 causes the consumer to spin when no messages are available.
	// Equivalent to the JVM's `fetch.min.bytes`.
	ConsumerFetchMin int32 `toml:"consumer-fetch-min"`
	// The default number of message bytes to fetch from the broker in each
	// request (default 1MB). This should be larger than the majority of
	// your messages, or else the consumer will spend a lot of time
	// negotiating sizes and not actually consuming. Similar to the JVM's
	// `fetch.message.max.bytes`.
	ConsumerFetchDefault int32 `toml:"consumer-fetch-default"`
	// The maximum number of message bytes to fetch from the broker in a
	// single request. Messages larger than this will return
	// ErrMessageTooLarge and will not be consumable, so you must be sure
	// this is at least as large as your largest message. Defaults to 0
	// (no limit). Similar to the JVM's `fetch.message.max.bytes`. The
	// global `sarama.MaxResponseSize` still applies.
	ConsumerFetchMax int32 `toml:"consumer-fetch-max"`

	// The maximum amount of time the broker will wait for Consumer.Fetch.Min
	// bytes to become available before it returns fewer than that anyways. The
	// default is 250ms, since 0 causes the consumer to spin when no events are
	// available. 100-500ms is a reasonable range for most cases. Kafka only
	// supports precision up to milliseconds; nanoseconds will be truncated.
	// Equivalent to the JVM's `fetch.wait.max.ms`.
	ConsumerMaxWaitTime int `toml:"consumer-fetch-time"`

	// The maximum amount of time the consumer expects a message takes to
	// process for the user. If writing to the Messages channel takes longer
	// than this, that partition will stop fetching more messages until it
	// can proceed again.
	// Note that, since the Messages channel is buffered, the actual grace time is
	// (MaxProcessingTime * ChanneBufferSize). Defaults to 100ms.
	// If a message is not written to the Messages channel between two ticks
	// of the expiryTicker then a timeout is detected.
	// Using a ticker instead of a timer to detect timeouts should typically
	// result in many fewer calls to Timer functions which may result in a
	// significant performance improvement if many messages are being sent
	// and timeouts are infrequent.
	// The disadvantage of using a ticker instead of a timer is that
	// timeouts will be less accurate. That is, the effective timeout could
	// be between `MaxProcessingTime` and `2 * MaxProcessingTime`. For
	// example, if `MaxProcessingTime` is 100ms then a delay of 180ms
	// between two messages being sent may not be recognized as a timeout.
	MaxProcessingTime int `toml:"max-processing-time"`

	// Return specifies what channels will be populated. If they are set to true,
	// you must read from them to prevent deadlock.
	// If enabled, any errors that occurred while consuming are returned on
	// the Errors channel (default disabled).
	ConsumerReturnErrors bool `toml:"consumer-return-errors"`

	// Offsets specifies configuration for how and when to commit consumed
	// offsets. This currently requires the manual use of an OffsetManager
	// but will eventually be automated.

	// How frequently to commit updated offsets. Defaults to 1s.
	ConsumerOffsetsCommitInterval int `toml:"consumer-offset-commit-interval"`

	// The initial offset to use if no offset was previously committed.
	// Should be OffsetNewest or OffsetOldest. Defaults to OffsetNewest.
	ConsumerOffsetsInitial int64 `toml: "consumer-offsets-intial"`

	// The retention duration for committed offsets. If zero, disabled
	// (in which case the `offsets.retention.minutes` option on the
	// broker will be used).  Kafka only supports precision up to
	// milliseconds; nanoseconds will be truncated. Requires Kafka
	// broker version 0.9.0 or later.
	// (default is 0: disabled).
	ConsumerOffsetsRetention int `toml:"consumer-offsets-retention"`

	// The total number of times to retry failing commit
	// requests during OffsetManager shutdown (default 3).
	ConsumerOffsetsRetryMax int `toml:"consumer-offsets-retry-max"`

	// A user-provided string sent with every request to the brokers for logging,
	// debugging, and auditing purposes. Defaults to "sarama", but you should
	// probably set it to something specific to your application.
	ClientID string `toml:"client-id"`
	// The number of events to buffer in internal and external channels. This
	// permits the producer and consumer to continue processing some messages
	// in the background while user code is working, greatly improving throughput.
	// Defaults to 256.
	ChannelBufferSize int `toml:"channel-buffer-size"`
	// The version of Kafka that Sarama will assume it is running against.
	// Defaults to the oldest supported stable version. Since Kafka provides
	// backwards-compatibility, setting it to a version older than you have
	// will not break anything, although it may prevent you from using the
	// latest features. Setting it to a version greater than you are actually
	// running may lead to random breakage.
	Version string `toml:"version"`

	// The registry to define metrics into.
	// Defaults to a local registry.
	// If you want to disable metrics gathering, set "metrics.UseNilMetrics" to "true"
	// prior to starting Sarama.
	// See Examples on how to use the metrics registry
	// MetricRegistry metrics.Registry
}

/*
// Config is the fork config of sarama config.
type Config struct {
	Admin    Admin    `toml:"admin"`
	Net      Net      `toml:"net"`
	Metadata Metadata `toml:"metadata"`
	Producer Producer `toml:"producer"`
	Consumer Consumer `toml:"consumer"`
}

// Admin is the namespace for ClusterAdmin properties used by the administrative Kafka client.
type Admin struct {
	// The maximum duration the administrative Kafka client will wait for ClusterAdmin operations,
	// including topics, brokers, configurations and ACLs (defaults to 3 seconds).
	Timeout int `toml:"timeout"`
}

// Net is the namespace for network-level properties used by the Broker, and
// shared by the Client/Producer/Consumer.
type Net struct {
	// How many outstanding requests a connection is allowed to have before
	// sending on it blocks (default 5).
	MaxOpenRequests int `toml:"max-open-requests"`

	// All three of the below configurations are similar to the
	// `socket.timeout.ms` setting in JVM kafka. All of them default
	// to 30 seconds.
	DialTimeout  int `toml:"dial-timeout"`  // How long to wait for the initial connection.
	ReadTimeout  int `toml:"read-timeout"`  // How long to wait for a response.
	WriteTimeout int `toml:"write-timeout"` // How long to wait for a transmit.

	TLS TLS `toml:"tls"`

	// KeepAlive specifies the keep-alive period for an active network connection.
	// If zero, keep-alives are disabled. (default is 0: disabled).
	KeepAlive int `toml:"keep-alive"`

	// LocalAddr is the local address to use when dialing an
	// address. The address must be of a compatible type for the
	// network being dialed.
	// If nil, a local address is automatically chosen.
	LocalAddr string `toml:"local-addr"`
}

type TLS struct {
	// Whether or not to use TLS when connecting to the broker
	// (defaults to false).
	Enable bool `toml:"enable"`
	// The TLS configuration to use for secure connections if
	// enabled (defaults to nil).
	Config *tls.Config
}

// SASL based authentication with broker. While there are multiple SASL authentication methods
// the current implementation is limited to plaintext (SASL/PLAIN) authentication
type SASL struct {
	// Whether or not to use SASL authentication when connecting to the broker
	// (defaults to false).
	Enable bool `toml:"enable"`
	// Whether or not to send the Kafka SASL handshake first if enabled
	// (defaults to true). You should only set this to false if you're using
	// a non-Kafka SASL proxy.
	Handshake bool `toml:"handshake"`
	//username and password for SASL/PLAIN authentication
	User     string `toml:"user"`
	Password string `toml:"password"`
}

// Metadata is the namespace for metadata management properties used by the
// Client, and shared by the Producer/Consumer.
type Metadata struct {
	Retry Retry `toml:"retry"`
	// How frequently to refresh the cluster metadata in the background.
	// Defaults to 10 minutes. Set to 0 to disable. Similar to
	// `topic.metadata.refresh.interval.ms` in the JVM version.
	RefreshFrequency int `toml:"refresh-frequency"`

	// Whether to maintain a full set of metadata for all topics, or just
	// the minimal set that has been necessary so far. The full set is simpler
	// and usually more convenient, but can take up a substantial amount of
	// memory if you have many topics and partitions. Defaults to true.
	Full bool `toml:"full"`
}

type Retry struct {
	// The total number of times to retry a metadata request when the
	// cluster is in the middle of a leader election (default 3).
	Max int `toml:"max"`
	// How long to wait for leader election to occur before retrying
	// (default 250ms). Similar to the JVM's `retry.backoff.ms`.
	Backoff int `toml:"backoff"`
}

// Producer is the namespace for configuration related to producing messages,
// used by the Producer.
type Producer struct {
	// The maximum permitted size of a message (defaults to 1000000). Should be
	// set equal to or smaller than the broker's `message.max.bytes`.
	MaxMessageBytes int `toml:"max-message-bytes"`
	// The level of acknowledgement reliability needed from the broker (defaults
	// to WaitForLocal). Equivalent to the `request.required.acks` setting of the
	// JVM producer.
	RequiredAcks RequiredAcks `toml:"required-acks"`
	// The maximum duration the broker will wait the receipt of the number of
	// RequiredAcks (defaults to 10 seconds). This is only relevant when
	// RequiredAcks is set to WaitForAll or a number > 1. Only supports
	// millisecond resolution, nanoseconds will be truncated. Equivalent to
	// the JVM producer's `request.timeout.ms` setting.
	Timeout int `toml:"timeout"`
	// The type of compression to use on messages (defaults to no compression).
	// Similar to `compression.codec` setting of the JVM producer.
	Compression CompressionCodec `toml:"compression"`
	// The level of compression to use on messages. The meaning depends
	// on the actual compression type used and defaults to default compression
	// level for the codec.
	CompressionLevel int `toml:"compression-level"`
	// Generates partitioners for choosing the partition to send messages to
	// (defaults to hashing the message key). Similar to the `partitioner.class`
	// setting for the JVM producer.
	Partitioner PartitionerConstructor `toml:"partitioner"`
	// If enabled, the producer will ensure that exactly one copy of each message is
	// written.
	Idempotent bool `toml:"idempotent"`

	// Return specifies what channels will be populated. If they are set to true,
	// you must read from the respective channels to prevent deadlock. If,
	// however, this config is used to create a `SyncProducer`, both must be set
	// to true and you shall not read from the channels since the producer does
	// this internally.
	Return Return `toml:"return"`

	// The following config options control how often messages are batched up and
	// sent to the broker. By default, messages are sent as fast as possible, and
	// all messages received while the current batch is in-flight are placed
	// into the subsequent batch.
	Flush Flush `toml:"flush"`

	Retry Retry `toml:"retry"`
}

// Return specifies what channels will be populated. If they are set to true,
// you must read from the respective channels to prevent deadlock. If,
// however, this config is used to create a `SyncProducer`, both must be set
// to true and you shall not read from the channels since the producer does
// this internally.
type Return struct {
	// If enabled, successfully delivered messages will be returned on the
	// Successes channel (default disabled).
	Successes bool `toml:"successes"`

	// If enabled, messages that failed to deliver will be returned on the
	// Errors channel, including error (default enabled).
	Errors bool `toml:"errors"`
}

// The following config options control how often messages are batched up and
// sent to the broker. By default, messages are sent as fast as possible, and
// all messages received while the current batch is in-flight are placed
// into the subsequent batch.
type Flush struct {
	// The best-effort number of bytes needed to trigger a flush. Use the
	// global sarama.MaxRequestSize to set a hard upper limit.
	Bytes int `toml:"bytes"`
	// The best-effort number of messages needed to trigger a flush. Use
	// `MaxMessages` to set a hard upper limit.
	Messages int `toml:"messages"`
	// The best-effort frequency of flushes. Equivalent to
	// `queue.buffering.max.ms` setting of JVM producer.
	Frequency int `toml:"frequency"`
	// The maximum number of messages the producer will send in a single
	// broker request. Defaults to 0 for unlimited. Similar to
	// `queue.buffering.max.messages` in the JVM producer.
	MaxMessages int `toml:"max-messages"`
}

type Retry struct {
	// The total number of times to retry sending a message (default 3).
	// Similar to the `message.send.max.retries` setting of the JVM producer.
	Max int `toml:"max"`
	// How long to wait for the cluster to settle between retries
	// (default 100ms). Similar to the `retry.backoff.ms` setting of the
	// JVM producer.
	Backoff int `toml:"Backoff"`
}

// Consumer is the namespace for configuration related to consuming messages,
// used by the Consumer.
type Consumer struct {

	// Group is the namespace for configuring consumer group.
	Group struct {
		Session struct {
			// The timeout used to detect consumer failures when using Kafka's group management facility.
			// The consumer sends periodic heartbeats to indicate its liveness to the broker.
			// If no heartbeats are received by the broker before the expiration of this session timeout,
			// then the broker will remove this consumer from the group and initiate a rebalance.
			// Note that the value must be in the allowable range as configured in the broker configuration
			// by `group.min.session.timeout.ms` and `group.max.session.timeout.ms` (default 10s)
			Timeout time.Duration
		}
		Heartbeat struct {
			// The expected time between heartbeats to the consumer coordinator when using Kafka's group
			// management facilities. Heartbeats are used to ensure that the consumer's session stays active and
			// to facilitate rebalancing when new consumers join or leave the group.
			// The value must be set lower than Consumer.Group.Session.Timeout, but typically should be set no
			// higher than 1/3 of that value.
			// It can be adjusted even lower to control the expected time for normal rebalances (default 3s)
			Interval time.Duration
		}
		Rebalance struct {
			// Strategy for allocating topic partitions to members (default BalanceStrategyRange)
			Strategy BalanceStrategy
			// The maximum allowed time for each worker to join the group once a rebalance has begun.
			// This is basically a limit on the amount of time needed for all tasks to flush any pending
			// data and commit offsets. If the timeout is exceeded, then the worker will be removed from
			// the group, which will cause offset commit failures (default 60s).
			Timeout time.Duration

			Retry struct {
				// When a new consumer joins a consumer group the set of consumers attempt to "rebalance"
				// the load to assign partitions to each consumer. If the set of consumers changes while
				// this assignment is taking place the rebalance will fail and retry. This setting controls
				// the maximum number of attempts before giving up (default 4).
				Max int
				// Backoff time between retries during rebalance (default 2s)
				Backoff time.Duration
			}
		}
		Member struct {
			// Custom metadata to include when joining the group. The user data for all joined members
			// can be retrieved by sending a DescribeGroupRequest to the broker that is the
			// coordinator for the group.
			UserData []byte
		}
	}

	Retry struct {
		// How long to wait after a failing to read from a partition before
		// trying again (default 2s).
		Backoff time.Duration
	}

	// Fetch is the namespace for controlling how many bytes are retrieved by any
	// given request.
	Fetch struct {
		// The minimum number of message bytes to fetch in a request - the broker
		// will wait until at least this many are available. The default is 1,
		// as 0 causes the consumer to spin when no messages are available.
		// Equivalent to the JVM's `fetch.min.bytes`.
		Min int32
		// The default number of message bytes to fetch from the broker in each
		// request (default 1MB). This should be larger than the majority of
		// your messages, or else the consumer will spend a lot of time
		// negotiating sizes and not actually consuming. Similar to the JVM's
		// `fetch.message.max.bytes`.
		Default int32
		// The maximum number of message bytes to fetch from the broker in a
		// single request. Messages larger than this will return
		// ErrMessageTooLarge and will not be consumable, so you must be sure
		// this is at least as large as your largest message. Defaults to 0
		// (no limit). Similar to the JVM's `fetch.message.max.bytes`. The
		// global `sarama.MaxResponseSize` still applies.
		Max int32
	}
	// The maximum amount of time the broker will wait for Consumer.Fetch.Min
	// bytes to become available before it returns fewer than that anyways. The
	// default is 250ms, since 0 causes the consumer to spin when no events are
	// available. 100-500ms is a reasonable range for most cases. Kafka only
	// supports precision up to milliseconds; nanoseconds will be truncated.
	// Equivalent to the JVM's `fetch.wait.max.ms`.
	MaxWaitTime time.Duration

	// The maximum amount of time the consumer expects a message takes to
	// process for the user. If writing to the Messages channel takes longer
	// than this, that partition will stop fetching more messages until it
	// can proceed again.
	// Note that, since the Messages channel is buffered, the actual grace time is
	// (MaxProcessingTime * ChanneBufferSize). Defaults to 100ms.
	// If a message is not written to the Messages channel between two ticks
	// of the expiryTicker then a timeout is detected.
	// Using a ticker instead of a timer to detect timeouts should typically
	// result in many fewer calls to Timer functions which may result in a
	// significant performance improvement if many messages are being sent
	// and timeouts are infrequent.
	// The disadvantage of using a ticker instead of a timer is that
	// timeouts will be less accurate. That is, the effective timeout could
	// be between `MaxProcessingTime` and `2 * MaxProcessingTime`. For
	// example, if `MaxProcessingTime` is 100ms then a delay of 180ms
	// between two messages being sent may not be recognized as a timeout.
	MaxProcessingTime time.Duration

	// Return specifies what channels will be populated. If they are set to true,
	// you must read from them to prevent deadlock.
	Return struct {
		// If enabled, any errors that occurred while consuming are returned on
		// the Errors channel (default disabled).
		Errors bool
	}

	// Offsets specifies configuration for how and when to commit consumed
	// offsets. This currently requires the manual use of an OffsetManager
	// but will eventually be automated.
	Offsets struct {
		// How frequently to commit updated offsets. Defaults to 1s.
		CommitInterval time.Duration

		// The initial offset to use if no offset was previously committed.
		// Should be OffsetNewest or OffsetOldest. Defaults to OffsetNewest.
		Initial int64

		// The retention duration for committed offsets. If zero, disabled
		// (in which case the `offsets.retention.minutes` option on the
		// broker will be used).  Kafka only supports precision up to
		// milliseconds; nanoseconds will be truncated. Requires Kafka
		// broker version 0.9.0 or later.
		// (default is 0: disabled).
		Retention time.Duration

		Retry struct {
			// The total number of times to retry failing commit
			// requests during OffsetManager shutdown (default 3).
			Max int
		}
	}
}
*/

/*
// Config is used to pass multiple configuration options to Sarama's constructors.
type Config struct {
	// Admin is the namespace for ClusterAdmin properties used by the administrative Kafka client.
	Admin struct {
		// The maximum duration the administrative Kafka client will wait for ClusterAdmin operations,
		// including topics, brokers, configurations and ACLs (defaults to 3 seconds).
		Timeout time.Duration
	}

	// Net is the namespace for network-level properties used by the Broker, and
	// shared by the Client/Producer/Consumer.
	Net struct {
		// How many outstanding requests a connection is allowed to have before
		// sending on it blocks (default 5).
		MaxOpenRequests int

		// All three of the below configurations are similar to the
		// `socket.timeout.ms` setting in JVM kafka. All of them default
		// to 30 seconds.
		DialTimeout  time.Duration // How long to wait for the initial connection.
		ReadTimeout  time.Duration // How long to wait for a response.
		WriteTimeout time.Duration // How long to wait for a transmit.

		TLS struct {
			// Whether or not to use TLS when connecting to the broker
			// (defaults to false).
			Enable bool
			// The TLS configuration to use for secure connections if
			// enabled (defaults to nil).
			Config *tls.Config
		}

		// SASL based authentication with broker. While there are multiple SASL authentication methods
		// the current implementation is limited to plaintext (SASL/PLAIN) authentication
		SASL struct {
			// Whether or not to use SASL authentication when connecting to the broker
			// (defaults to false).
			Enable bool
			// Whether or not to send the Kafka SASL handshake first if enabled
			// (defaults to true). You should only set this to false if you're using
			// a non-Kafka SASL proxy.
			Handshake bool
			//username and password for SASL/PLAIN authentication
			User     string
			Password string
		}

		// KeepAlive specifies the keep-alive period for an active network connection.
		// If zero, keep-alives are disabled. (default is 0: disabled).
		KeepAlive time.Duration

		// LocalAddr is the local address to use when dialing an
		// address. The address must be of a compatible type for the
		// network being dialed.
		// If nil, a local address is automatically chosen.
		LocalAddr net.Addr
	}

	// Metadata is the namespace for metadata management properties used by the
	// Client, and shared by the Producer/Consumer.
	Metadata struct {
		Retry struct {
			// The total number of times to retry a metadata request when the
			// cluster is in the middle of a leader election (default 3).
			Max int
			// How long to wait for leader election to occur before retrying
			// (default 250ms). Similar to the JVM's `retry.backoff.ms`.
			Backoff time.Duration
		}
		// How frequently to refresh the cluster metadata in the background.
		// Defaults to 10 minutes. Set to 0 to disable. Similar to
		// `topic.metadata.refresh.interval.ms` in the JVM version.
		RefreshFrequency time.Duration

		// Whether to maintain a full set of metadata for all topics, or just
		// the minimal set that has been necessary so far. The full set is simpler
		// and usually more convenient, but can take up a substantial amount of
		// memory if you have many topics and partitions. Defaults to true.
		Full bool
	}

	// Producer is the namespace for configuration related to producing messages,
	// used by the Producer.
	Producer struct {
		// The maximum permitted size of a message (defaults to 1000000). Should be
		// set equal to or smaller than the broker's `message.max.bytes`.
		MaxMessageBytes int
		// The level of acknowledgement reliability needed from the broker (defaults
		// to WaitForLocal). Equivalent to the `request.required.acks` setting of the
		// JVM producer.
		RequiredAcks RequiredAcks
		// The maximum duration the broker will wait the receipt of the number of
		// RequiredAcks (defaults to 10 seconds). This is only relevant when
		// RequiredAcks is set to WaitForAll or a number > 1. Only supports
		// millisecond resolution, nanoseconds will be truncated. Equivalent to
		// the JVM producer's `request.timeout.ms` setting.
		Timeout time.Duration
		// The type of compression to use on messages (defaults to no compression).
		// Similar to `compression.codec` setting of the JVM producer.
		Compression CompressionCodec
		// The level of compression to use on messages. The meaning depends
		// on the actual compression type used and defaults to default compression
		// level for the codec.
		CompressionLevel int
		// Generates partitioners for choosing the partition to send messages to
		// (defaults to hashing the message key). Similar to the `partitioner.class`
		// setting for the JVM producer.
		Partitioner PartitionerConstructor
		// If enabled, the producer will ensure that exactly one copy of each message is
		// written.
		Idempotent bool

		// Return specifies what channels will be populated. If they are set to true,
		// you must read from the respective channels to prevent deadlock. If,
		// however, this config is used to create a `SyncProducer`, both must be set
		// to true and you shall not read from the channels since the producer does
		// this internally.
		Return struct {
			// If enabled, successfully delivered messages will be returned on the
			// Successes channel (default disabled).
			Successes bool

			// If enabled, messages that failed to deliver will be returned on the
			// Errors channel, including error (default enabled).
			Errors bool
		}

		// The following config options control how often messages are batched up and
		// sent to the broker. By default, messages are sent as fast as possible, and
		// all messages received while the current batch is in-flight are placed
		// into the subsequent batch.
		Flush struct {
			// The best-effort number of bytes needed to trigger a flush. Use the
			// global sarama.MaxRequestSize to set a hard upper limit.
			Bytes int
			// The best-effort number of messages needed to trigger a flush. Use
			// `MaxMessages` to set a hard upper limit.
			Messages int
			// The best-effort frequency of flushes. Equivalent to
			// `queue.buffering.max.ms` setting of JVM producer.
			Frequency time.Duration
			// The maximum number of messages the producer will send in a single
			// broker request. Defaults to 0 for unlimited. Similar to
			// `queue.buffering.max.messages` in the JVM producer.
			MaxMessages int
		}

		Retry struct {
			// The total number of times to retry sending a message (default 3).
			// Similar to the `message.send.max.retries` setting of the JVM producer.
			Max int
			// How long to wait for the cluster to settle between retries
			// (default 100ms). Similar to the `retry.backoff.ms` setting of the
			// JVM producer.
			Backoff time.Duration
		}
	}

	// Consumer is the namespace for configuration related to consuming messages,
	// used by the Consumer.
	Consumer struct {

		// Group is the namespace for configuring consumer group.
		Group struct {
			Session struct {
				// The timeout used to detect consumer failures when using Kafka's group management facility.
				// The consumer sends periodic heartbeats to indicate its liveness to the broker.
				// If no heartbeats are received by the broker before the expiration of this session timeout,
				// then the broker will remove this consumer from the group and initiate a rebalance.
				// Note that the value must be in the allowable range as configured in the broker configuration
				// by `group.min.session.timeout.ms` and `group.max.session.timeout.ms` (default 10s)
				Timeout time.Duration
			}
			Heartbeat struct {
				// The expected time between heartbeats to the consumer coordinator when using Kafka's group
				// management facilities. Heartbeats are used to ensure that the consumer's session stays active and
				// to facilitate rebalancing when new consumers join or leave the group.
				// The value must be set lower than Consumer.Group.Session.Timeout, but typically should be set no
				// higher than 1/3 of that value.
				// It can be adjusted even lower to control the expected time for normal rebalances (default 3s)
				Interval time.Duration
			}
			Rebalance struct {
				// Strategy for allocating topic partitions to members (default BalanceStrategyRange)
				Strategy BalanceStrategy
				// The maximum allowed time for each worker to join the group once a rebalance has begun.
				// This is basically a limit on the amount of time needed for all tasks to flush any pending
				// data and commit offsets. If the timeout is exceeded, then the worker will be removed from
				// the group, which will cause offset commit failures (default 60s).
				Timeout time.Duration

				Retry struct {
					// When a new consumer joins a consumer group the set of consumers attempt to "rebalance"
					// the load to assign partitions to each consumer. If the set of consumers changes while
					// this assignment is taking place the rebalance will fail and retry. This setting controls
					// the maximum number of attempts before giving up (default 4).
					Max int
					// Backoff time between retries during rebalance (default 2s)
					Backoff time.Duration
				}
			}
			Member struct {
				// Custom metadata to include when joining the group. The user data for all joined members
				// can be retrieved by sending a DescribeGroupRequest to the broker that is the
				// coordinator for the group.
				UserData []byte
			}
		}

		Retry struct {
			// How long to wait after a failing to read from a partition before
			// trying again (default 2s).
			Backoff time.Duration
		}

		// Fetch is the namespace for controlling how many bytes are retrieved by any
		// given request.
		Fetch struct {
			// The minimum number of message bytes to fetch in a request - the broker
			// will wait until at least this many are available. The default is 1,
			// as 0 causes the consumer to spin when no messages are available.
			// Equivalent to the JVM's `fetch.min.bytes`.
			Min int32
			// The default number of message bytes to fetch from the broker in each
			// request (default 1MB). This should be larger than the majority of
			// your messages, or else the consumer will spend a lot of time
			// negotiating sizes and not actually consuming. Similar to the JVM's
			// `fetch.message.max.bytes`.
			Default int32
			// The maximum number of message bytes to fetch from the broker in a
			// single request. Messages larger than this will return
			// ErrMessageTooLarge and will not be consumable, so you must be sure
			// this is at least as large as your largest message. Defaults to 0
			// (no limit). Similar to the JVM's `fetch.message.max.bytes`. The
			// global `sarama.MaxResponseSize` still applies.
			Max int32
		}
		// The maximum amount of time the broker will wait for Consumer.Fetch.Min
		// bytes to become available before it returns fewer than that anyways. The
		// default is 250ms, since 0 causes the consumer to spin when no events are
		// available. 100-500ms is a reasonable range for most cases. Kafka only
		// supports precision up to milliseconds; nanoseconds will be truncated.
		// Equivalent to the JVM's `fetch.wait.max.ms`.
		MaxWaitTime time.Duration

		// The maximum amount of time the consumer expects a message takes to
		// process for the user. If writing to the Messages channel takes longer
		// than this, that partition will stop fetching more messages until it
		// can proceed again.
		// Note that, since the Messages channel is buffered, the actual grace time is
		// (MaxProcessingTime * ChanneBufferSize). Defaults to 100ms.
		// If a message is not written to the Messages channel between two ticks
		// of the expiryTicker then a timeout is detected.
		// Using a ticker instead of a timer to detect timeouts should typically
		// result in many fewer calls to Timer functions which may result in a
		// significant performance improvement if many messages are being sent
		// and timeouts are infrequent.
		// The disadvantage of using a ticker instead of a timer is that
		// timeouts will be less accurate. That is, the effective timeout could
		// be between `MaxProcessingTime` and `2 * MaxProcessingTime`. For
		// example, if `MaxProcessingTime` is 100ms then a delay of 180ms
		// between two messages being sent may not be recognized as a timeout.
		MaxProcessingTime time.Duration

		// Return specifies what channels will be populated. If they are set to true,
		// you must read from them to prevent deadlock.
		Return struct {
			// If enabled, any errors that occurred while consuming are returned on
			// the Errors channel (default disabled).
			Errors bool
		}

		// Offsets specifies configuration for how and when to commit consumed
		// offsets. This currently requires the manual use of an OffsetManager
		// but will eventually be automated.
		Offsets struct {
			// How frequently to commit updated offsets. Defaults to 1s.
			CommitInterval time.Duration

			// The initial offset to use if no offset was previously committed.
			// Should be OffsetNewest or OffsetOldest. Defaults to OffsetNewest.
			Initial int64

			// The retention duration for committed offsets. If zero, disabled
			// (in which case the `offsets.retention.minutes` option on the
			// broker will be used).  Kafka only supports precision up to
			// milliseconds; nanoseconds will be truncated. Requires Kafka
			// broker version 0.9.0 or later.
			// (default is 0: disabled).
			Retention time.Duration

			Retry struct {
				// The total number of times to retry failing commit
				// requests during OffsetManager shutdown (default 3).
				Max int
			}
		}
	}

	// A user-provided string sent with every request to the brokers for logging,
	// debugging, and auditing purposes. Defaults to "sarama", but you should
	// probably set it to something specific to your application.
	ClientID string
	// The number of events to buffer in internal and external channels. This
	// permits the producer and consumer to continue processing some messages
	// in the background while user code is working, greatly improving throughput.
	// Defaults to 256.
	ChannelBufferSize int
	// The version of Kafka that Sarama will assume it is running against.
	// Defaults to the oldest supported stable version. Since Kafka provides
	// backwards-compatibility, setting it to a version older than you have
	// will not break anything, although it may prevent you from using the
	// latest features. Setting it to a version greater than you are actually
	// running may lead to random breakage.
	Version KafkaVersion
	// The registry to define metrics into.
	// Defaults to a local registry.
	// If you want to disable metrics gathering, set "metrics.UseNilMetrics" to "true"
	// prior to starting Sarama.
	// See Examples on how to use the metrics registry
	MetricRegistry metrics.Registry
}
*/
