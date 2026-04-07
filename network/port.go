package network

import "fmt"

func Port(port int) error {

	if port < 1024 || port > 65535 {
		return fmt.Errorf("port must be between 1024 and 65535")
	}

	return nil
}

func IsPrivilegedPort(port int) bool {
	return port > 0 && port < 1024
}
