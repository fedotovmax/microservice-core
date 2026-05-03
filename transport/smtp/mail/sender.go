package mail

import (
	"context"
	"fmt"

	"gopkg.in/gomail.v2"
)

type dialer struct {
	*gomail.Dialer
	dispayName string
}

func (d *dialer) GetUsername() string {
	return fmt.Sprintf("%s <%s>", d.dispayName, d.Username)
}

type idialer interface {
	DialAndSend(m ...*gomail.Message) error
	GetUsername() string
}

type Sender struct {
	d idialer
}

func New(config *Config) (*Sender, error) {

	const op = "core.transport.smtp.mail.New"

	gomailDialer := gomail.NewDialer(
		config.Host,
		config.Port,
		config.Sender,
		config.Secret,
	)

	dialer := &dialer{Dialer: gomailDialer, dispayName: config.DisplayName}

	return &Sender{
		d: dialer,
	}, nil
}

func (s *Sender) SendHTML(ctx context.Context, m HTMLMessage) error {

	const op = "core.transport.smtp.mail.Service.SendHTML"

	if err := m.Validate(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	message := gomail.NewMessage()
	message.SetHeader("From", s.d.GetUsername())
	message.SetHeader("To", m.To)
	message.SetHeader("Subject", m.Title)
	message.SetBody("text/html", m.HTML)

	err := s.send(ctx, message)

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil

}

func (s *Sender) SendTextMessage(ctx context.Context, m TextMessage) error {

	const op = "core.transport.smtp.mail.Service.SendTextMessage"

	if err := m.Validate(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	message := gomail.NewMessage()
	message.SetHeader("From", s.d.GetUsername())
	message.SetHeader("To", m.To)
	message.SetHeader("Subject", m.Title)
	message.SetBody("text/plain", m.Text)

	err := s.send(ctx, message)

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil

}

func (s *Sender) send(ctx context.Context, m *gomail.Message) error {

	const op = "core.transport.smtp.mail.Service.send"

	errCh := make(chan error, 1)

	go func() {
		err := s.d.DialAndSend(m)
		errCh <- err
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		return nil
	}
}
