package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Client struct {
	conn *nats.Conn
	JS   jetstream.JetStream
}

type Config struct {
	URL            string
	ConnectTimeout time.Duration
	MaxReconnects  int
}

func New(cfg Config) (*Client, error) {
	opts := []nats.Option{
		nats.Name("oms-order-service"),
		nats.MaxReconnects(cfg.MaxReconnects),
	}
	if cfg.ConnectTimeout > 0 {
		opts = append(opts, nats.Timeout(cfg.ConnectTimeout))
	}

	url := cfg.URL
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats: connect to %s: %w", url, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: init jetstream: %w", err)
	}

	return &Client{conn: nc, JS: js}, nil
}

func (c *Client) Publish(subject string, data []byte) error {
	if err := c.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("nats: publish to %s: %w", subject, err)
	}
	return nil
}

func (c *Client) PublishJS(ctx context.Context, subject string, data []byte) error {
	if _, err := c.JS.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("nats: js publish to %s: %w", subject, err)
	}
	return nil
}

func (c *Client) Close() {
	_ = c.conn.Drain()
}
