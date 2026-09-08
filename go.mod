module github.com/smarty/messaging/v4

go 1.25

require (
	github.com/rabbitmq/amqp091-go v1.14.0
	github.com/smarty/gunit v1.6.0
)

// The v4.0.0-alpha tags belong to an abandoned 2021 streaming experiment,
// unrelated to the v4.0.0 release line.
retract (
	v4.0.0-alpha.1
	v4.0.0-alpha.0
)
