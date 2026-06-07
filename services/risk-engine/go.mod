module github.com/yourusername/oms/services/risk-engine

go 1.25.0

require (
	github.com/redis/go-redis/v9 v9.6.1
	github.com/yourusername/oms/gen v0.0.0
	google.golang.org/grpc v1.65.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240730163845-b1a4ccb954bf // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace github.com/yourusername/oms/gen => ../../gen
