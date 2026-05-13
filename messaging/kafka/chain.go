package kafka

func ChainMiddlewares(handler MessageHandler, mws ...Middleware) MessageHandler {
	if len(mws) == 0 {
		return handler
	}

	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}

	return handler
}
