package ratelimit
import (
	"fmt"
	"log"
	"crypto/sha1"
	"github.com/go-redis/redis"
	"errors"
	"github.com/shekhar-kamble/ratelimit/redisfactory"
	"time"
	"math"
)
const MaxLimit = math.MaxInt64
const rateTokenizerScript = `
local limit = tonumber(ARGV[1])
local duration = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local bucket = ':' .. duration .. ':' .. math.floor(now / duration)
for j, id in ipairs(KEYS) do
	local key = id .. bucket
	local count = redis.call('INCR', key)
	redis.call('EXPIRE', key, duration)
	if tonumber(count) > limit then
		return 1
	end
end
return 0
`
var rateTokenizerHash = fmt.Sprintf("%x", sha1.Sum([]byte(rateTokenizerScript)))

func runLimiterHash(client *redis.Client) (err error) {
	if client == nil {
		return errors.New("Cannot Connect to Redis Client")
	}
	exists, err := client.ScriptExists(rateTokenizerHash).Result()
	if err != nil {
		return
	}
	if !exists[0] {
		_, err = client.ScriptLoad(rateTokenizerScript).Result()
		if err != nil {
			return
		}
	}
	return nil
}

func New(limit int64, duration int64, key string) (limiter *Limiter, err error) {
	rConn, err := redisfactory.NewRedisConnection()
	if err != nil {
		return nil, errors.New("please set LIMITER_REDIS_URL to point to a valid url")
	}
	rClient, err := rConn.NewRedisClient()
	if err != nil {
		return nil, errors.New("Cannot Connect to Redis Client")
	}

	limiter = newLimiter(rClient, limit, duration, key)
	go func() {
		timer := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-timer.C:
				runLimiterHash(rClient)
			}
		}
	}()
	return limiter, runLimiterHash(rClient)
}

type Limiter struct {
	redisClient *redis.Client
	limit 		int64
	duration 	int64
	key 		string
}

func newLimiter(redisClient *redis.Client, limit int64, duration int64, key string) *Limiter {
	return &Limiter{
		redisClient: redisClient,
		limit: limit,
		duration: duration,
		key:   key,
	}
}

func (lim *Limiter) Allow() bool {
	if lim.redisClient == nil {
		log.Println("Cannot Connect to Redis Client")
		return true
	}

	results, err := lim.redisClient.EvalSha(
		rateTokenizerHash,
		[]string{lim.key},
		lim.limit,
		lim.duration,
		time.Now().Unix(),
	).Result()
	if err != nil {
		log.Println("Failed to call Rate Limit function : ", err)
		return true
	}
	return results == int64(0)
}
