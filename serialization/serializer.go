package serialization

import (
	"encoding/json"
	"fmt"
)

type defaultSerializer struct{}

func (this defaultSerializer) ContentType() string { return "application/json" }

func (this defaultSerializer) Serialize(source any) ([]byte, error) {
	if raw, err := json.Marshal(source); err != nil {
		return nil, fmt.Errorf("%w: [%s]", ErrSerializeUnsupportedType, err)
	} else {
		return raw, nil
	}
}

func (this defaultSerializer) Deserialize(source []byte, target any) error {
	if err := json.Unmarshal(source, target); err != nil {
		return fmt.Errorf("%w: [%s]", ErrDeserializeMalformedPayload, err)
	}

	return nil
}
