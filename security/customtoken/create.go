package customtoken

func Create() (Created, error) {
	token, err := Generate()

	if err != nil {
		return Created{}, err
	}

	hash := Hash(token)

	resulst := Created{
		Nohashed: token,
		Hashed:   hash,
	}

	return resulst, nil
}
