package redisFactory

import (
	"log"
	"net/url"
	"os"
	"github.com/go-redis/redis"
)

type RedisConnection struct {
	Host        string 
	Auth        string 
	IdleTimeout int    
}

func newRedisConnection() (conn RedisConnection, err error) {
	redisURL, err := url.Parse(os.Getenv("LIMITER_REDIS_URL"))
	if err != nil {
		return
	}
	conn = RedisConnection{}
	conn.Host = redisURL.Host
	conn.Auth = ""
	if redisURL.User != nil {
		if password, ok := redisURL.User.Password(); ok {
			conn.Auth = password
		}
	}
	return
}

func newRedisClient(config RedisConnection) (client *redis.Client, err error) {
	client = redis.NewClient(&redis.Options{
		Addr:     config.Host,
		Password: config.Auth,
		PoolSize: 200,
	})

	if _, err = client.Ping().Result(); err != nil {
		log.Println("fail to initialize redis client: ", err)
		client = nil
		return
	}

	return 
}
