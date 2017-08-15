package mq

import (
	"crypto/tls"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/smartystreets/messaging/rabbit"
	"github.com/streadway/amqp"
)

type Connector struct{}

func NewConnector() *Connector {
	return &Connector{}
}

func (this *Connector) Connect(target url.URL) (rabbit.Connection, error) {
	config := amqp.Config{
		TLSClientConfig: buildTLS(target),
		Heartbeat:       time.Second * 15,
	}

	log.Println("[INFO] Establishing connection to AMQP broker.")
	if connection, err := amqp.DialConfig(target.String(), config); err != nil {
		log.Println("[WARN] Unable to establish connection", err)
		return nil, err
	} else {
		log.Println("[INFO] AMQP connection established.")
		return newConnection(connection), nil
	}
}
func buildTLS(target url.URL) *tls.Config {
	if strings.ToLower(target.Scheme) != "amqps" {
		return nil
	}

	// FUTURE: customize TLS, e.g. acceptable list of ciphers, etc.
	return &tls.Config{
		ServerName: strings.Split(target.Host, ":")[0],
		MinVersion: tls.VersionTLS12,
	}
}
