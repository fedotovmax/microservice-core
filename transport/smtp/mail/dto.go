package mail

import (
	"fmt"
	"net/mail"
	"unicode/utf8"
)

type TextMessage struct {
	To    string
	Title string
	Text  string
}

func (m TextMessage) Validate() error {

	_, err := mail.ParseAddress(m.To)

	if err != nil {
		return err
	}

	if utf8.RuneCountInString(m.Title) == 0 {
		return fmt.Errorf("title is empty")
	}

	if utf8.RuneCountInString(m.Text) == 0 {
		return fmt.Errorf("text is empty")
	}

	return nil

}

type HTMLMessage struct {
	To    string
	Title string
	HTML  string
}

func (m HTMLMessage) Validate() error {

	_, err := mail.ParseAddress(m.To)

	if err != nil {
		return err
	}

	if utf8.RuneCountInString(m.Title) == 0 {
		return fmt.Errorf("title is empty")
	}

	if len(m.HTML) == 0 {
		return fmt.Errorf("html template is empty")
	}

	return nil

}
