package mail

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gopkg.in/gomail.v2"
)

// Обновим мок, используя возможности testify/mock
type mockDialer struct {
	mock.Mock
}

func newWitMock(d idialer) *Sender {
	return &Sender{d: d}
}

func (m *mockDialer) DialAndSend(msg ...*gomail.Message) error {
	args := m.Called(msg[0]) // Передаем сообщение для возможности проверки в On()
	return args.Error(0)
}

func (m *mockDialer) GetUsername() string {
	args := m.Called()
	return args.String(0)
}

func TestService_Validation(t *testing.T) {
	tests := []struct {
		name    string
		msg     interface{ Validate() error } // Универсальный интерфейс для обоих типов сообщений
		wantErr bool
	}{
		{"Valid Text", TextMessage{To: "a@b.com", Title: "T", Text: "B"}, false},
		{"Empty Text Email", TextMessage{To: "", Title: "T", Text: "B"}, true},
		{"Empty Text Title", TextMessage{To: "a@b.com", Title: "", Text: "B"}, true},
		{"Valid HTML", HTMLMessage{To: "a@b.com", Title: "T", HTML: "<h1>B</h1>"}, false},
		{"Empty HTML Body", HTMLMessage{To: "a@b.com", Title: "T", HTML: ""}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.msg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestService_SendHTML(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		m := new(mockDialer)
		s := newWitMock(m)

		m.On("GetUsername").Return("sender@test.com")
		m.On("DialAndSend", mock.Anything).Return(nil)

		err := s.SendHTML(context.Background(), HTMLMessage{To: "u@t.com", Title: "T", HTML: "H"})

		assert.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("Validation Fail", func(t *testing.T) {
		m := new(mockDialer)
		s := newWitMock(m)

		// Плохие данные: пустой HTML
		err := s.SendHTML(context.Background(), HTMLMessage{To: "u@t.com", Title: "T", HTML: ""})

		assert.Error(t, err)
		m.AssertNotCalled(t, "DialAndSend", mock.Anything)
	})
}

func TestService_Send_ContextTimeout(t *testing.T) {
	m := new(mockDialer)
	s := newWitMock(m)

	m.On("GetUsername").Return("sender@test.com")
	// Имитируем долгую работу, чтобы сработал таймаут
	m.On("DialAndSend", mock.Anything).After(50 * time.Millisecond).Return(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	err := s.SendTextMessage(ctx, TextMessage{To: "u@t.com", Title: "T", Text: "B"})

	assert.Error(t, err)
	// Здесь мы проверяем именно на DeadlineExceeded, так как это логика работы с контекстом
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(errors.Unwrap(err), context.DeadlineExceeded))
}

func TestService_Send_SMTP_Error(t *testing.T) {
	m := new(mockDialer)
	s := newWitMock(m)

	m.On("GetUsername").Return("sender@test.com")
	m.On("DialAndSend", mock.Anything).Return(errors.New("any smtp error"))

	err := s.SendTextMessage(context.Background(), TextMessage{To: "u@t.com", Title: "T", Text: "B"})

	assert.Error(t, err)
}
