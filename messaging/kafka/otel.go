package kafka

import (
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
)

const TelemetryKafka = "kafka"

const TraceSystemKey = "kafka"

const TelemetryConsumer = "consumer"
const TelemetryConsumerHandler = TelemetryConsumer + "." + "handler"
const TelemetryProducer = "producer"

const PlatformTelemetryProducer = TelemetryKafka + "." + TelemetryProducer
const PlatformTelemetryConsumerHandler = TelemetryKafka + "." + TelemetryConsumerHandler
const PlatformTelemetryConsumer = TelemetryKafka + "." + TelemetryConsumer

func TraceConsumerBusinessLogic(name string) string {
	return PlatformTelemetryConsumerHandler + " business logic : " + name
}

func TraceConsumerHandleMark(topic string) string {
	return PlatformTelemetryConsumerHandler + " mark: " + topic
}

func TraceConsumeTopic(topic string) string {
	return PlatformTelemetryConsumerHandler + " consume: " + topic
}

func TraceProducerBusinessLogic(name string) string {
	return PlatformTelemetryProducer + " business logic : " + name
}

func TraceProducerSendTopic(topic string) string {
	return PlatformTelemetryProducer + " send to: " + topic
}

func TraceHeaderKey(key string) string {
	return "messaging.kafka.header." + strings.ToLower(key)
}

type TelemetryMetaWrapper struct {
	Original  interface{}
	Span      trace.Span
	StartTime time.Time
}

const ConsumerMeterName = TelemetryKafka + "." + TelemetryConsumer
const ProducerMeterName = TelemetryKafka + "." + TelemetryProducer
