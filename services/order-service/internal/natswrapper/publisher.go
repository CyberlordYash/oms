// Package natswrapper exposes a narrow Publisher interface so that handler code
// is not coupled to the concrete NATS client type.  This makes unit testing
// straightforward — just implement the interface with a fake.
package natswrapper

// Publisher is satisfied by *nats.Client (pkg/nats) and by test fakes.
type Publisher interface {
	Publish(subject string, data []byte) error
}
