// market-data-sim seeds last_price:{symbol} in Redis with realistic NSE prices
// and advances them with small random ticks so the risk engine's circuit-breaker
// check (±20% deviation) fires on grossly out-of-range orders.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// initialPrices are NSE spot prices expressed in paise (₹1 = 100 paise).
var initialPrices = map[string]float64{
	"RELIANCE":  280000, // ₹2,800
	"TCS":       410000, // ₹4,100
	"INFY":      170000, // ₹1,700
	"HDFCBANK":  160000, // ₹1,600
	"ICICIBANK": 120000, // ₹1,200
	"WIPRO":     50000,  // ₹500
	"SBIN":      80000,  // ₹800
	"ITC":       45000,  // ₹450
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6380"
	}

	tickMs := 2000
	if v := os.Getenv("TICK_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			tickMs = n
		}
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping %s: %w", redisAddr, err)
	}
	logger.Info("redis connected", "addr", redisAddr)

	// Seed initial prices so the circuit breaker has a baseline from the start.
	prices := make(map[string]float64, len(initialPrices))
	seedCtx := context.Background()
	for sym, price := range initialPrices {
		prices[sym] = price
		if err := rdb.Set(seedCtx, "last_price:"+sym, strconv.FormatFloat(price, 'f', 2, 64), 0).Err(); err != nil {
			logger.Warn("failed to seed price", "symbol", sym, "error", err)
		}
	}
	logger.Info("initial prices seeded", "symbols", len(prices))

	ticker := time.NewTicker(time.Duration(tickMs) * time.Millisecond)
	defer ticker.Stop()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-quit:
			logger.Info("shutting down")
			return nil

		case <-ticker.C:
			tickCtx := context.Background()
			pipe := rdb.Pipeline()
			for sym := range prices {
				// ±1% random walk each tick.
				pct := (rand.Float64()*2 - 1) * 0.01
				prices[sym] = prices[sym] * (1 + pct)
				// Clamp to a floor of ₹1 (100 paise) to avoid zero/negative prices.
				if prices[sym] < 100 {
					prices[sym] = 100
				}
				pipe.Set(tickCtx, "last_price:"+sym, strconv.FormatFloat(prices[sym], 'f', 2, 64), 0)
			}
			if _, err := pipe.Exec(tickCtx); err != nil {
				logger.Warn("price update failed", "error", err)
			}
		}
	}
}
