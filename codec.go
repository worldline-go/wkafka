package wkafka

import "encoding/json"

type codecJSON[T any] struct{}

func (codecJSON[T]) Encode(data T) ([]byte, error) {
	return json.Marshal(data)
}

func (codecJSON[T]) Decode(raw []byte) (T, error) {
	var data T
	err := json.Unmarshal(raw, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
