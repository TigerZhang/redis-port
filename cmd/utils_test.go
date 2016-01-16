package main
import (
	"testing"
	"../pkg/libs/assert"

	"github.com/garyburd/redigo/redis"
	"flag"
	"fmt"

	pelicantun "github.com/mailgun/pelican-protocol/tun"
	"reflect"
)

var (
	redisAddress   = flag.String("redis-address", ":6379", "Address to the Redis server")
	maxConnections = flag.Int("max-connections", 10, "Max connections to Redis")
)

func Test_yunba_tfs_set_to_zset(t *testing.T) {
	redisPool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", *redisAddress)

		if err != nil {
			return nil, err
		}

		return c, err
	}, *maxConnections)

	defer redisPool.Close()

	var ignore, modify bool
	var data []byte

	key_f := "etf:F:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0"
	key_fs := "etf:FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:59"
	key_fs_to_zset := "etf:F:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0"
	key_f_uid := "etf:F:/uid_topics/2582603978711672192"

	// score: 0, value: 2578207829183604096
//	data_fs_hard_coded := "\x0b\x10\b\x00\x00\x00\x01\x00\x00\x00\x80\xa5\x9eW\\\xa2\xc7#\x06\x00)\xff\xc3}\x8fj\xc2?"

	c := redisPool.Get()
	defer c.Close()

	c.Do("DEL", key_fs)
	c.Do("DEL", key_fs_to_zset)

	// must be alphabetically sorted
	uid_list := []string{"123", "2578207829183604096", "456", "789"}
	for _, uid := range uid_list {
		c.Do("SADD", key_fs, []byte(uid))
	}

	data_fs, _ := c.Do("DUMP", key_fs)
//	fmt.Printf("data dump: %v\n", data_fs)

	yunba_tfs_set_to_zset_restore_cmd(nil, &ignore, &modify, []byte(key_f), data)
	assert.Must(ignore == true)
	assert.Must(modify == false)

//	yunba_tfs_set_to_zset_restore_cmd(c, &ignore, &modify, []byte(key_fs), []byte(data_fs_hard_coded))
//	assert.Must(ignore == false)
//	assert.Must(modify == true)

	yunba_tfs_set_to_zset_restore_cmd(c, &ignore, &modify, []byte(key_fs), data_fs.([]byte))
	assert.Must(ignore == false)
	assert.Must(modify == true)

	i, b, err := pelicantun.Base36toBigInt([]byte("abj"))
	fmt.Printf("abj-> %v %v\n", i, b)

	b, s := pelicantun.BigIntToBase36(i)
	fmt.Printf("abj<- %s %v\n", b, s)

	i, b, err = pelicantun.Base36toBigInt([]byte("front"))
	fmt.Printf("front-> %v %v\n", i, b)

	b, s = pelicantun.BigIntToBase36(i)
	fmt.Printf("front<- %s %v\n", b, s)


	i, b, err = pelicantun.Base36toBigInt([]byte("zzb1"))
	fmt.Printf("zzb1-> %v %v\n", i, b)

	b, s = pelicantun.BigIntToBase36(i)
	fmt.Printf("zzb1<- %s %v\n", b, s)

	i, b, err = pelicantun.Base36toBigInt([]byte("zzzzz"))
	fmt.Printf("zzzzz-> %v %v\n", i, b)

	b, s = pelicantun.BigIntToBase36(i)
	fmt.Printf("zzzzz<- %s %v\n", b, s)

	setArgs := make([][]byte, 0)
	setArgs = append(setArgs, []byte(key_fs))
	setArgs = append(setArgs, []byte{'1', '2'})
	zsetArgs, err := yunba_tfs_set_cmd_to_zset_cmd(setArgs)
	assert.Must(err == nil)
	fmt.Printf("zsetArgs key: %s\n", string(zsetArgs[0]))
	assert.Must(reflect.DeepEqual(zsetArgs[0], []byte(key_fs_to_zset)))
	assert.Must(reflect.DeepEqual(zsetArgs[1], []byte{'0'}))
	assert.Must(reflect.DeepEqual(zsetArgs[2], []byte{'1', '2'}))

	reply, err := redis.Strings(c.Do("ZRANGE", key_fs_to_zset, "0", "-1", "withscores"))
	if err != nil {
		fmt.Printf("ZCARD error %s\n", err.Error())
	}
//	fmt.Printf("reply: %s\n", reply)
	for i, uid := range uid_list {
//		fmt.Printf("reply[%d]: %s uid %s\n", 2*i, reply[2*i], uid)
		assert.Must(reply[2*i] == uid)
		assert.Must(reply[2*i+1] == "0")
	}

	yunba_tfs_set_to_zset_restore_cmd(nil, &ignore, &modify, []byte(key_f_uid), data)
	assert.Must(ignore == false)
	assert.Must(modify == false)
}
