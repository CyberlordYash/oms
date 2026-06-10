package natswrapper

type Publisher interface {
	Publish(subject string, data []byte) error
}
