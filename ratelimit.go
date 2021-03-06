package ratelimit

import (
	"context"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"sync"
	"time"
)

// time_unit is the second of unit
// 最小精度是秒
const SCRIPT = `
local current_timestamp = redis.call("TIME")
local key_prefix = KEYS[1]
local time_window = tonumber(ARGV[1])
local throughput = tonumber(ARGV[2])
local batch_size = tonumber(ARGV[3])
local start_tw = math.ceil(tonumber(current_timestamp[1])/time_window)
local key = key_prefix .. ":" .. tostring(start_tw)
local n = redis.call("GET", key)

if n == false then
    n = 0
else
    n = tonumber(n)
end

if n >= throughput then
    return 0,start_tw
end

local increment = math.min(throughput - n, batch_size)
redis.replicate_commands();
redis.call("INCRBY", key, increment)
redis.call("EXPIRE", key, time_window * 3)
return increment,start_tw
`

type bucket struct {
	keyPrefix string
	N         int
	DeadTime  time.Time
}

type RateLimiter struct {
	pool   *redis.Pool
	script *redis.Script
	// config
	timeWindow int
	throughput int
	batchSize  int

	// todo replace with lru
	keyPrefix string

	lock    sync.Mutex
	buckets map[string]*bucket
}

// duration 精度最小到秒
func NewLimiter(pool *redis.Pool, keyPrefix string, period time.Duration, throughput int, batchSize int) *RateLimiter {
	windowSize := period / time.Second
	if windowSize < 1 {
		windowSize = 1
	}

	r := &RateLimiter{
		pool:       pool,
		script:     redis.NewScript(1, SCRIPT),
		keyPrefix:  keyPrefix,
		timeWindow: int(windowSize),
		throughput: throughput,
		batchSize:  batchSize,
		buckets:    make(map[string]*bucket),
	}

	return r
}

func (r *RateLimiter) Take(token string, amount int) (bool, error) {
	var synced bool
	r.lock.Lock()
	defer r.lock.Unlock()
	b, exist := r.buckets[token]
	if !exist || !b.isValid() {
		if err := r.fillBucket(token); err != nil {
			return false, err
		}
		b = r.buckets[token]
		synced = true
	}

	// exist && valid
	if b.N < amount && !synced {
		if err := r.fillBucket(token); err != nil {
			return false, err
		}
	}

	if b.N >= amount {
		b.N -= amount
		return true, nil
	}

	return false, nil
}

func (r *RateLimiter) Do(conn redis.Conn, token string) (interface{}, error) {
	v, err := redis.DoWithTimeout(conn, time.Millisecond*100, "EVALSHA", r.script.Hash(), token, r.timeWindow, r.throughput, r.batchSize)
	if e, ok := err.(redis.Error); ok && strings.HasPrefix(string(e), "NOSCRIPT ") {
		v, err = redis.DoWithTimeout(conn, time.Millisecond*100, "EVAL", SCRIPT, token, r.timeWindow, r.throughput, r.batchSize)
	}
	return v, err
}

func (r *RateLimiter) fillBucket(token string) error {
	ctx, cf := context.WithTimeout(context.TODO(), time.Millisecond*300)
	defer cf()

	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return err
	}

	reply, err := redis.Int64s(r.Do(conn, token))
	if err != nil {
		return err
	}

	if len(reply) != 2 {
		return fmt.Errorf("bad redis reply %+v", reply)
	}

	count, tws := int(reply[0]), int(reply[1])
	if count <= 0 {
		count = 0
	}

	twe := time.Now().Add(time.Duration((tws+1)*r.timeWindow) * time.Second)
	b, exist := r.buckets[token]
	if exist {
		b.fill(count, twe)
	} else {
		r.buckets[token] = &bucket{keyPrefix: r.keyPrefix + ":" + token, N: count, DeadTime: twe}
	}

	return nil
}

func (b *bucket) isValid() bool {
	if b.DeadTime.Before(time.Now()) {
		return false
	}
	return true
}

func (b *bucket) fill(amount int, dead time.Time) {
	if !b.isValid() {
		b.N = amount
	} else {
		b.N += amount
	}

	b.DeadTime = dead
}
