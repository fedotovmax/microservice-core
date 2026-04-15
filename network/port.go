package network

import "fmt"

const maxPort = 65535

func Port(port int) error {

	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1024 and 65535")
	}

	return nil
}

func IsPrivilegedPort(port int) bool {
	return port > 0 && port < 1024
}

func NonPrivilegedPort(port int) error {
	if IsPrivilegedPort(port) {
		return fmt.Errorf("cannot use privileged port")
	}

	if port > maxPort {
		return fmt.Errorf("maximum port value is %d", maxPort)
	}

	return nil
}
