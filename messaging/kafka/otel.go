package kafka

import (
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// Системные константы для OTel
const (
	TraceSystemKey = "kafka"
	TelemetryKafka = "kafka"
)

const (
	PlatformTelemetryProducer = TelemetryKafka + ".producer"
	PlatformTelemetryConsumer = TelemetryKafka + ".consumer"
)

// Хелперы для имен спанов (теперь без дублирования префиксов)
// Мы не пишем "kafka.consumer", так как это уже есть в имени трейсера

func TraceConsumeTopic(topic string) string {
	return "consume: " + topic
}

func TraceConsumerHandleMark(topic string) string {
	return "mark: " + topic
}

func TraceBusinessLogic(name string) string {
	return "business logic: " + name
}

func TraceProducerSendTopic(topic string) string {
	return "send to: " + topic
}

// Утилиты для атрибутов
func TraceHeaderKey(key string) string {
	return "messaging.kafka.header." + strings.ToLower(key)
}

// TelemetryMetaWrapper используется для проброса контекста
// и таймингов в асинхронных операциях (например, в Producer.Completion)
type TelemetryMetaWrapper struct {
	Original  interface{}
	Span      trace.Span
	StartTime time.Time
}
