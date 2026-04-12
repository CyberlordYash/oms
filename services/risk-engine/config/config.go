package config

import (
	"os"
	"strconv"
)

// Config holds all runtime configuration for the Risk Engine.
type Config struct {
	GRPCPort  int
	RedisAddr string
}

// Load reads configuration from environment variables with sane defaults.
func Load() Config {
	return Config{
		GRPCPort:  envInt("GRPC_PORT", 50052),
		RedisAddr: envStr("REDIS_ADDR", "localhost:6379"),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
