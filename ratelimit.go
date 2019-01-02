package ratelimit

import (
	"context"
	"fmt"
	"github.com/garyburd/redigo/redis"
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

var scriptSHA1 string
var once sync.Once

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
	sync.Mutex
	keyPrefix string
	buckets   map[string]*bucket
}

// duration 精度最小到秒
func NewLimiter(pool *redis.Pool, keyPrefix string, period time.Duration, throughput int, batchSize int) *RateLimiter {
	timeWindow := period / time.Second
	if timeWindow < 1 {
		timeWindow = 1
	}

	r := &RateLimiter{
		pool:       pool,
		keyPrefix:  keyPrefix,
		timeWindow: int(timeWindow),
		throughput: throughput,
		batchSize:  batchSize,
		buckets:    make(map[string]*bucket),
		script:     redis.NewScript(1, SCRIPT),
	}

	return r
}

func (r *RateLimiter) Take(token string, amount int) (bool, error) {
	r.Lock()
	defer r.Unlock()
	b, exist := r.buckets[token]
	if !exist || !b.isValid() {
		if err := r.fillBucket(token); err != nil {
			return false, err
		}
		b = r.buckets[token]
	}

	// exist && valid
	if b.N < amount {
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

func (r *RateLimiter) fillBucket(token string) error {
	ctx, cf := context.WithTimeout(context.TODO(), time.Millisecond*300)
	defer cf()

	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return err
	}

	reply, err := redis.Int64s(r.script.Do(conn, token, r.timeWindow, r.throughput, r.batchSize))
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
		b.N = b.N + int(count)
		b.DeadTime = twe
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
