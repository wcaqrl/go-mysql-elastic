package common

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/config"
	"time"
)

var redisDb *redis.Client

//创建redis 连接
func NewRedisConn(redisConfig config.RedisConfig) (rdb *redis.Client, err error) {
	rdb = redis.NewClient(&redis.Options{
		Addr:     redisConfig.Addr,
		Password: redisConfig.Password,
		DB:       redisConfig.Db,
	})

	if _, err = rdb.Ping(context.Background()).Result(); err != nil {
		log.Panic(fmt.Sprintf("Ping redis failed, error: %s", err.Error()))
	}
	redisDb = rdb
	return
}

func GetByKey(key string) (string, error) {
	var cmd *redis.StringCmd
	cmd = redisDb.Get(context.Background(), key)
	return cmd.Val(), cmd.Err()
}

func SetEX(key string, value interface{}, exp int64) error {
	return redisDb.SetEX(context.Background(), key, value, time.Duration(exp*1e9)).Err()
}

func ZRevrangebyscore(key string, min, max string, offset, count int64) []string {
	var opt = redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: offset,
		Count:  count,
	}
	return redisDb.ZRevRangeByScore(context.Background(), key, &opt).Val()
}

func ZRangebyscore(key string, min, max string, offset, count int64) []string {
	var opt = redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: offset,
		Count:  count,
	}
	return redisDb.ZRangeByScore(context.Background(), key, &opt).Val()
}

func ZCard(key string) (int64, error) {
	var cmd *redis.IntCmd
	cmd = redisDb.ZCard(context.Background(), key)
	return cmd.Val(), cmd.Err()
}

func ZRem(key string, members []interface{}) (int64, error) {
	var cmd *redis.IntCmd
	cmd = redisDb.ZRem(context.Background(), key, members...)
	return cmd.Val(), cmd.Err()
}

func ZAdd(key string, members []*redis.Z) (int64, error) {
	var cmd *redis.IntCmd
	cmd = redisDb.ZAdd(context.Background(), key, members...)
	return cmd.Val(), cmd.Err()
}

func Incr(key string) int64 {
	return redisDb.Incr(context.Background(), key).Val()
}

func Del(keys []string) int64 {
	return redisDb.Del(context.Background(), keys...).Val()
}

func Lpush(key string, values []string) (int64, error) {
	var cmd *redis.IntCmd
	cmd = redisDb.LPush(context.Background(), key, values)
	return cmd.Val(), cmd.Err()
}

func RPop(key string) (string, error) {
	var cmd *redis.StringCmd
	cmd = redisDb.RPop(context.Background(), key)
	return cmd.Val(), cmd.Err()
}
