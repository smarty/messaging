package messaging

import (
	"encoding/json"
	"log"
)

type JSONSerializer struct {
	panicFail bool
}

func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{}
}

func (this *JSONSerializer) PanicWhenSerializationFails() {
	this.panicFail = true
}

func (this *JSONSerializer) Serialize(message interface{}) ([]byte, error) {
	content, err := json.Marshal(message)
	if this.panicFail && err != nil {
		log.Panic("[ERROR] Could not serialize message:", err)
	} else if err != nil {
		log.Println("[ERROR] Could not serialize message:", err)
	}
	return content, err
}

func (this *JSONSerializer) ContentType() string     { return "application/json" }
func (this *JSONSerializer) ContentEncoding() string { return "" }
