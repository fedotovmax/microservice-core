package password

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

type Argon2Config struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// HashPassword генерирует Argon2id хеш с заданными параметрами
func HashPassword(password string, c Argon2Config) (string, error) {
	// Генерация случайной соли
	salt := make([]byte, c.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	// Генерация ключа (хеша)
	hash := argon2.IDKey([]byte(password), salt, c.Iterations, c.Memory, c.Parallelism, c.KeyLength)

	// Кодирование в формат PHC (стандарт для хранения хешей)
	// Формат: $argon2id$v=19$m=65536,t=3,p=2$salt$hash
	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	encodedHash := fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, c.Memory, c.Iterations, c.Parallelism, b64Salt, b64Hash)

	return encodedHash, nil
}

// CheckPassword сравнивает пароль с хешем, извлекая параметры из самого хеша
func CheckPassword(password, encodedHash string) error {
	// Разбираем строку хеша на части
	parts := strings.Split(encodedHash, "$")
	if len(parts) != 6 {
		return ErrArgonInvalidHash
	}

	// Дополнительная проверка типа алгоритма
	if parts[1] != "argon2id" {
		return ErrArgonInvalidHash
	}

	// Проверяем версию
	var version int
	_, err := fmt.Sscanf(parts[2], "v=%d", &version)
	if err != nil {
		return err
	}
	if version != argon2.Version {
		return ErrArgonIncompatibleVersion
	}

	c := Argon2Config{}
	_, err = fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &c.Memory, &c.Iterations, &c.Parallelism)
	if err != nil {
		return err
	}

	// Декодируем соль
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return err
	}

	// Декодируем существующий хеш для сравнения длины ключа
	decodedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return err
	}
	c.KeyLength = uint32(len(decodedHash))

	// Генерируем хеш из введенного пароля с теми же параметрами
	comparisonHash := argon2.IDKey([]byte(password), salt, c.Iterations, c.Memory, c.Parallelism, c.KeyLength)

	// Защищенное сравнение (Constant Time Compare) для предотвращения атак по времени
	if subtle.ConstantTimeCompare(decodedHash, comparisonHash) == 1 {
		return nil
	}

	return ErrInvalidPassword
}
