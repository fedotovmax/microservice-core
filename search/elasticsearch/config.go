package elasticsearch

type Option func(*Config)

func WithAuthByCredentials(a AuthByCredentials) Option {
	return func(c *Config) {
		c.AuthMethod = AuthMethodCredentials
		c.AuthByCredentials = &a
	}
}

func WithAuthByBearerToken(a AuthByBearerToken) Option {
	return func(c *Config) {
		c.AuthMethod = AuthMethodBearerToken
		c.AuthByBearerToken = &a
	}
}

func WithMaxRetries(r int) Option {
	return func(c *Config) {
		c.MaxRetries = r
	}
}

func WithTelemetry() Option {
	return func(c *Config) {
		c.Telemetry = true
	}
}

func WithTelemetryShowSearchBodyInTraces(f bool) Option {
	return func(c *Config) {
		c.TelemetryShowSearchBodyInTraces = f
	}
}

type AuthByCredentials struct {
	Username string
	Passwrod string
}

type AuthByBearerToken struct {
	Token string
}

type AuthMethod int

const (
	AuthMethodNone AuthMethod = iota
	AuthMethodCredentials
	AuthMethodBearerToken
)

type Config struct {
	Addresses                       []string
	MaxRetries                      int
	AuthMethod                      AuthMethod
	Telemetry                       bool
	TelemetryShowSearchBodyInTraces bool
	AuthByCredentials               *AuthByCredentials
	AuthByBearerToken               *AuthByBearerToken
}

func (c Config) Validate() error {
	return nil
}

func defaulConfig() Config {
	return Config{
		AuthMethod:                      AuthMethodNone,
		MaxRetries:                      3,
		Telemetry:                       false,
		TelemetryShowSearchBodyInTraces: false,
	}
}

func NewConfig(addresses []string, opts ...Option) (Config, error) {

	cfg := defaulConfig()

	cfg.Addresses = addresses

	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func NewConfigMust(addresses []string, opts ...Option) Config {
	cfg, err := NewConfig(addresses, opts...)

	if err != nil {
		panic(err)
	}

	return cfg
}
