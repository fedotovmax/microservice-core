```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/logger/zap"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/middlewares"
	"github.com/fedotovmax/microservice-core/messaging/kafka/sarama/consumer"
	"github.com/fedotovmax/microservice-core/messaging/kafka/sarama/producer"
	httpMiddlewares "github.com/fedotovmax/microservice-core/transport/http/middlewares"
	"github.com/fedotovmax/microservice-core/transport/http/server"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

const (
	topicName = "test-topic"
	timeout   = 5 * time.Second
)

func initTracer() func(context.Context) error {
	exporter, _ := otlptracehttp.New(context.Background(), otlptracehttp.WithInsecure(), otlptracehttp.WithEndpoint("localhost:4318"))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String("my-service"))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown
}

func main() {
	// 1. Инициализация логгера и трейсера
	log, err := zap.New(zap.NewConfigMust())
	if err != nil {
		panic(err)
	}

	shutdownTracer := initTracer()
	defer shutdownTracer(context.Background())

	// 2. Инициализация Продюсера
	producerCfg := kafka.NewProducerConfigMust([]string{"localhost:9092"}, kafka.WithProducerTracing(true))
	prod, err := producer.New(log, producerCfg)
	if err != nil {
		panic(err)
	}

	// Запускаем твои обработчики каналов в фоне
	go prod.HandleSuccesses(timeout, func(ctx context.Context, e kafka.SuccessMessage) error {
		log.Info(fmt.Sprintf("Message delivered! Meta: %v", e.Meta()))
		return nil
	})
	go prod.HandleErrors(timeout, func(ctx context.Context, e kafka.FailedMessage) error {
		log.Error("Message failed", logger.Err(err))
		return nil
	})

	// 3. Инициализация Консюмера
	groupCfg := kafka.NewGroupConfigMust([]string{"localhost:9092"}, []string{topicName}, "test-group", kafka.WithGroupTracing(true))
	consumerGroup, err := consumer.NewGroup(log, groupCfg)
	if err != nil {
		panic(err)
	}

	// Запускаем консюмер
	consumerGroup.Start(kafka.ConsumerGroupStartReadParams{
		Middlewares: []kafka.Middleware{
			middlewares.ConsumerTracing(),
		},
		MessageHandler: func(ctx context.Context, ev kafka.ConsumeMessage) error {
			// Дочерний спан для проверки проброса контекста
			ctx, span := otel.Tracer("consumer.handler").Start(ctx, "process_business_logic")
			defer span.End()

			log.Info(fmt.Sprintf("Consumed message: %s", string(ev.Payload())))
			return nil
		},
	}, func(ctx context.Context, err error) {
		log.Error("Consume error", logger.Err(err))
	})

	// 4. Инициализация HTTP Сервера
	router := server.NewRouter()

	// Подключаем твой middleware для трейсинга входящих HTTP запросов
	router.Use(httpMiddlewares.OtelTrace("my-http-server"))

	// Регистрируем роут отправки сообщения
	router.RegisterRoute(server.Route{
		Method: http.MethodPost,
		Path:   "/send",
		Handler: server.ToHandler(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			// Если нужно передать какие-то свои бизнес-заголовки
			customHeaders := []kafka.Header{
				{
					Key:   []byte("X-Request-ID"),
					Value: []byte("req-12345"),
				},
				{
					Key:   []byte("X-Source"),
					Value: []byte("http-api"),
				},
			}

			// Вызываем твой конструктор
			msg := kafka.NewMessage(
				"user-1",                     // key
				topicName,                    // topic
				[]byte(`{"action": "test"}`), // payload
				customHeaders,                // твои []Header (или nil, если заголовков нет)
				"my-meta-data",               // meta (WriterData)
			)

			// Отправляем через твой метод Send
			if err := prod.Send(ctx, msg); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}),
	})

	srvConfig := server.NewConfigMust(":8080")

	srv, err := server.New(srvConfig, log, router)
	if err != nil {
		panic(err)
	}

	// Запускаем сервер асинхронно
	srv.StartAsync(context.Background(), func(ctx context.Context, err error) {
		log.Error("Server error", logger.Err(err))
	})

	log.Info("Service started on :8080")

	// 5. Ожидание сигналов ОС для Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down...")

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = srv.Stop(stopCtx)
	if err != nil {
		log.Error(err.Error())
	}
	err = consumerGroup.Stop(stopCtx)
	if err != nil {
		log.Error(err.Error())
	}
	log.Info("consumerGroup stopped")
	err = prod.Stop(stopCtx)
	if err != nil {
		log.Error(err.Error())
	}
	log.Info("prod stopped")

	// Для консюмера, если у тебя есть метод Stop, тоже вызови здесь
}
```
