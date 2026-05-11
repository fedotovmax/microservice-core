package kafka

import (
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
)

const TracerName = "kafka"

const TraceSystemKey = "kafka"

const TraceConsumerHandleMark = TraceSystemKey + " " + "mark"

const TraceConsumerHandler = "consumer.handler"

func TraceProducerSendTopic(topic string) string {
	return topic + " send"
}

func TraceHeaderKey(key string) string {
	return "messaging.kafka.header." + strings.ToLower(key)
}

type TelemetryMetaWrapper struct {
	Original  interface{}
	Span      trace.Span
	StartTime time.Time
}

const ConsumerMeterName = "kafka.consumer"
const ProducerMeterName = "kafka.producer"
