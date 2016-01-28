// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/wandoulabs/redis-port/pkg/libs/atomic2"
	"github.com/wandoulabs/redis-port/pkg/libs/errors"
	"github.com/wandoulabs/redis-port/pkg/libs/log"
	"github.com/wandoulabs/redis-port/pkg/libs/stats"
	"github.com/wandoulabs/redis-port/pkg/rdb"
	"github.com/wandoulabs/redis-port/pkg/redis"
	"fmt"
	"bytes"

	goredis "gopkg.in/redis.v3"
)

var (
	tfs_fs_prefix			= []byte("etf:FS:")
	tfs_f_prefix			= []byte("etf:F:")
	tfs_f_uid_topics_prefix	= []byte("etf:F:/uid_topics")
)

type ScorePair struct {
	Score  int64
	Member []byte
}

func openRedisConn(target, passwd string) redigo.Conn {
	return redigo.NewConn(openNetConn(target, passwd), 0, 0)
}

func openNetConn(target, passwd string) net.Conn {
	c, err := net.Dial("tcp", target)
	if err != nil {
		log.PanicErrorf(err, "cannot connect to '%s'", target)
	}
	authPassword(c, passwd)
	return c
}

func openNetConnSoft(target, passwd string) net.Conn {
	c, err := net.Dial("tcp", target)
	if err != nil {
		return nil
	}
	authPassword(c, passwd)
	return c
}

func openReadFile(name string) (*os.File, int64) {
	f, err := os.Open(name)
	if err != nil {
		log.PanicErrorf(err, "cannot open file-reader '%s'", name)
	}
	s, err := f.Stat()
	if err != nil {
		log.PanicErrorf(err, "cannot stat file-reader '%s'", name)
	}
	return f, s.Size()
}

func openWriteFile(name string) *os.File {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.PanicErrorf(err, "cannot open file-writer '%s'", name)
	}
	return f
}

func openReadWriteFile(name string) *os.File {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		log.PanicErrorf(err, "cannot open file-readwriter '%s'", name)
	}
	return f
}

func authPassword(c net.Conn, passwd string) {
	if passwd == "" {
		return
	}
	_, err := c.Write(redis.MustEncodeToBytes(redis.NewCommand("auth", passwd)))
	if err != nil {
		log.PanicError(errors.Trace(err), "write auth command failed")
	}
	var b = make([]byte, 5)
	if _, err := io.ReadFull(c, b); err != nil {
		log.PanicError(errors.Trace(err), "read auth response failed")
	}
	if strings.ToUpper(string(b)) != "+OK\r\n" {
		log.Panic("auth failed")
	}
}

func openSyncConn(target string, passwd string) (net.Conn, <-chan int64) {
	c := openNetConn(target, passwd)
	if _, err := c.Write(redis.MustEncodeToBytes(redis.NewCommand("sync"))); err != nil {
		log.PanicError(errors.Trace(err), "write sync command failed")
	}
	return c, waitRdbDump(c)
}

func waitRdbDump(r io.Reader) <-chan int64 {
	size := make(chan int64)
	go func() {
		var rsp string
		for {
			b := []byte{0}
			if _, err := r.Read(b); err != nil {
				log.PanicErrorf(err, "read sync response = '%s'", rsp)
			}
			if len(rsp) == 0 && b[0] == '\n' {
				size <- 0
				continue
			}
			rsp += string(b)
			if strings.HasSuffix(rsp, "\r\n") {
				break
			}
		}
		if rsp[0] != '$' {
			log.Panicf("invalid sync response, rsp = '%s'", rsp)
		}
		n, err := strconv.Atoi(rsp[1 : len(rsp)-2])
		if err != nil || n <= 0 {
			log.PanicErrorf(err, "invalid sync response = '%s', n = %d", rsp, n)
		}
		size <- int64(n)
	}()
	return size
}

func sendPSyncFullsync(br *bufio.Reader, bw *bufio.Writer) (string, int64, <-chan int64) {
	cmd := redis.NewCommand("psync", "?", -1)
	if err := redis.Encode(bw, cmd, true); err != nil {
		log.PanicError(err, "write psync command failed, fullsync")
	}
	r, err := redis.Decode(br)
	if err != nil {
		log.PanicError(err, "invalid psync response, fullsync")
	}
	if e, ok := r.(*redis.Error); ok {
		log.Panicf("invalid psync response, fullsync, %s", e.Value)
	}
	x, err := redis.AsString(r, nil)
	if err != nil {
		log.PanicError(err, "invalid psync response, fullsync")
	}
	xx := strings.Split(x, " ")
	if len(xx) != 3 || strings.ToLower(xx[0]) != "fullresync" {
		log.Panicf("invalid psync response = '%s', should be fullsync", x)
	}
	v, err := strconv.ParseInt(xx[2], 10, 64)
	if err != nil {
		log.PanicError(err, "parse psync offset failed")
	}
	runid, offset := xx[1], v-1
	return runid, offset, waitRdbDump(br)
}

func sendPSyncContinue(br *bufio.Reader, bw *bufio.Writer, runid string, offset int64) {
	cmd := redis.NewCommand("psync", runid, offset+2)
	if err := redis.Encode(bw, cmd, true); err != nil {
		log.PanicError(err, "write psync command failed, continue")
	}
	r, err := redis.Decode(br)
	if err != nil {
		log.PanicError(err, "invalid psync response, continue")
	}
	if e, ok := r.(*redis.Error); ok {
		log.Panicf("invalid psync response, continue, %s", e.Value)
	}
	x, err := redis.AsString(r, nil)
	if err != nil {
		log.PanicError(err, "invalid psync response, continue")
	}
	xx := strings.Split(x, " ")
	if len(xx) != 1 || strings.ToLower(xx[0]) != "continue" {
		log.Panicf("invalid psync response = '%s', should be continue", x)
	}
}

func sendPSyncAck(bw *bufio.Writer, offset int64) error {
	cmd := redis.NewCommand("replconf", "ack", offset)
	return redis.Encode(bw, cmd, true)
}

func selectDB(c redigo.Conn, db uint32) {
	s, err := redigo.String(c.Do("select", db))
	if err != nil {
		log.PanicError(err, "select command error")
	}
	if s != "OK" {
		log.Panicf("select command response = '%s', should be 'OK'", s)
	}
}

func byte_array_startswith(ba []byte, prefix []byte) (bool) {
	len := len(ba)
	result := true
	for i := range prefix {
		if i >= len {
			result = false
			break
		}

		if ba[i] != prefix[i] {
			result = false
			break
		}
	}

	return result
}

func modify_or_ignore(key []byte, ignore, modify *bool) {
	*modify = false
	*ignore = true

	is_fs := byte_array_startswith(key, tfs_fs_prefix)
	if is_fs {
		*ignore = false
	}

	is_f_uid_topics := byte_array_startswith(key, tfs_f_uid_topics_prefix)
	if is_f_uid_topics {
		*ignore = false
	} else {
		is_f := byte_array_startswith(key, tfs_f_prefix)
		if is_f {
			*ignore = true
		}
	}

	if is_f_uid_topics {
		// do NOT modify the key/value
		return
	} else if is_fs {
		*modify = true
		// insert the UIDs to a new zset with the key of File
	}
}

func splitCustomerIdServerId(customer_id_server_id string) (uint32, uint32) {
	i := 0
	hasCustomerId := false
	for i < len(customer_id_server_id) {
		if customer_id_server_id[i] >= '0' && customer_id_server_id[i] <= '9' {
			if i > 0 {
				hasCustomerId = true
			}
			break
		}

		i += 1
	}

	if hasCustomerId {
		a := []byte(customer_id_server_id)

		customer_id := a[:i]
		server_id := a[i:]
		c := customerIdStringToInt(string(customer_id))
		s, _ := strconv.ParseUint(string(server_id), 10, 32)

		return c, uint32(s)
	} else {
		s, _ := strconv.ParseUint(customer_id_server_id, 10, 32)

		return 0, uint32(s)
	}
}

func customerIdStringToInt(customerId string) uint32 {
	switch customerId {
	case "xp":
		return 1
	case "jb":
		return 2
	case "zb":
		return 3
	case "bbg":
		return 4
	}

	return 0
}

// IDC number * 0xFFFFFF + customer ID * 0xFFFF + server ID
//
// IDCs
// abj: 1
//
// customer ID
// xp: 1
// jb: 2
// zb: 3
// bbg: 4
//
func front_tag_to_score(front_tag string) uint32 {
	var score uint32
	score = 0
	var idc, customer_id, server_id uint32
	idc = 0
	customer_id = 0
	server_id = 0

	split := strings.Split(front_tag, "-")
	if len(split) == 3 {
		switch split[0] {
		case "abj":
			idc = 1
		}

		customer_id, server_id = splitCustomerIdServerId(split[2])

		score = idc << 24 | customer_id << 16 | server_id
	}

	return score
}

// TFS data demo
//
// $ redis-cli smembers etf:F:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0
// 1) "FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:59"
// 2) "FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:40"
// 3) "FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:4"
// 4) "FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:64"
// 5) "FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:51"
// 6) "FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:39"
//
// 8) "etf:F:/uid_topics/2582603978711672192"
// 9) "etf:F:/uid_topics/2577045856928281216"
//
// $ redis-cli smembers etf:FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:65
// 1) "2580471980996094976"
// 2) "2580472943194598400"
// 3) "2580601451761804672"

// use zset instead of set for yunba tfs
func yunba_tfs_set_to_zset_restore_cmd(c redigo.Conn, ignore *bool, modify *bool, key []byte, data []byte, redisrt *goredis.Client) error {
	//                 |  previous value    |    after value
	//-----------------|--------------------|----------------------------------------------------------
	// topic File      | set of File Slice  | zset of UIDs.
	//                 |                    | Use the front ID as score of UID, if the user is oneline.
	//                 |                    | The score will be 0 if the UID is offline.
	//-----------------|--------------------|----------------------------------------------------------
	// topic FileSlice | set of UIDs        | None. Will be ignored
	//-----------------|--------------------|----------------------------------------------------------
	// subed topics of | set of topics      |
	//     UID         |                    |

	modify_or_ignore(key, ignore, modify)

	if c == nil {
		// ignore write operation
		return nil
	}

	if data == nil {
		return nil
	}

	b := make([][]byte, 16)
	b[0] = key

	o, err := rdb.DecodeDump(data)
	if err != nil {
		return err
	}

	if *ignore {
		return nil
	}

	switch value := o.(type) {
	case rdb.Set:
		// convert to zset
		// "etf:FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:59"
	    // "etf:FS:/<appkey>/<topic>[/topic]:<slice-num>"
		newKey, _ := set_key_to_zset_key(key)
		if c != nil {
//			sp := make([]ScorePair, len(value))
			for _, v := range value {
//				sp[0].Score = 0
//				sp[0].Member = v
				score := scoreFromRouteTable(redisrt, string(v))
				c.Do("zadd", newKey, score, v)
			}
		}
	case rdb.String:
		value = value
	case rdb.Hash:
		value = value
	case rdb.ZSet:
		// ignore
		value = value
	default:
		fmt.Printf("invalid data type %T", o)
	}

	return nil
}

func scoreFromRouteTable(redisrt *goredis.Client, uid string) string {
	score := "0"
	if redisrt != nil {
		//					fmt.Printf("read route table redis: %v\n", redisrt)
		if routeItem, err := redisrt.Get(uid).Result(); err == nil {
			fmt.Printf("get route table of %s: %s\n", uid, routeItem)
			score = routeTableStringToScore(routeItem)
		} else {
			fmt.Println("read route of %s table failed: %s", uid, err.Error())
		}
	}

	return score
}

func routeTableStringToScore(route string) string {
	// route table format: <fonrt-tag>$<online-flag>
	// online-flag: 1 - online, 0 - offline
	score := "0"
	split := strings.Split(route, "$")
	if len(split) == 2 {
		if split[1] == "1" {
			score = fmt.Sprint(front_tag_to_score(split[0]))
		}
	}

	return score
}

func yunba_tfs_sadd_cmd_to_zadd_cmd(redisrt *goredis.Client, args [][]byte) ([][]byte, error) {
	// convert sadd to zadd
	var ignore, modify bool
	yunba_tfs_set_to_zset_restore_cmd(nil, &ignore, &modify, args[0], nil, nil)
	fmt.Printf("ignore %v modify %v\n", ignore, modify)
	if ignore == false {
		if modify {
			newKey, _ := set_key_to_zset_key([]byte(args[0]))
			fmt.Printf("newkey: %s\n", string(newKey))

			zsetArgs := make([][]byte, 0)
			zsetArgs = append(zsetArgs, newKey)
			for _, elem := range args[1:] {
				score := scoreFromRouteTable(redisrt, string(elem))
				zsetArgs = append(zsetArgs, []byte(score))
				zsetArgs = append(zsetArgs, elem)
			}

			return zsetArgs, nil
		}
	}

	return nil, errors.Errorf("ignored")
}

func yunba_tfs_srem_cmd_to_zrem_cmd(args [][]byte) ([][]byte, error) {
	var ignore, modify bool

	key := args[0]
	modify_or_ignore(key, &ignore, &modify)
	if ignore == false {
		if modify {
			newKey, _ := set_key_to_zset_key(key)
			fmt.Printf("newkey: %s\n", string(newKey))

			zremArgs := make([][]byte, 0)
			zremArgs = append(zremArgs, newKey)
			for _, elem := range args[1:] {
				zremArgs = append(zremArgs, elem)
			}

			return zremArgs, nil
		}
	}

	return nil, errors.Errorf("ignored")
}

func set_key_to_zset_key(key []byte) ([]byte, error) {
	splits := bytes.Split(key, []byte{':'})
	//		fmt.Printf("etf: %s\n", string(splits[0]))
	//		fmt.Printf("FS: %s\n", string(splits[1]))
	//		fmt.Printf("Topic: %s\n", string(splits[2]))
	//		fmt.Printf("Slice: %s\n", string(splits[3]))
	// new key
	newKey := make([]byte, 0)
	newKey = append(newKey, splits[0]...)
	newKey = append(newKey, []byte{':', 'F', ':'}...)
	newKey = append(newKey, splits[2]...)

	return newKey, nil
}

func restoreRdbEntry(c redigo.Conn, e *rdb.BinEntry, redisrt *goredis.Client) {
	var ttlms uint64
	if e.ExpireAt != 0 {
		now := uint64(time.Now().Add(args.shift).UnixNano())
		now /= uint64(time.Millisecond)
		if now >= e.ExpireAt {
			ttlms = 1
		} else {
			ttlms = e.ExpireAt - now
		}
	}

	var ignore, modify bool

	yunba_tfs_set_to_zset_restore_cmd(c, &ignore, &modify, e.Key, e.Value, redisrt)

	ttlms = ttlms

//    s := string(e.Key[:])
//    if strings.Contains(s, "564c13b8f085fc471efdfff8") {
//        s, err := redigo.String(c.Do("restore", e.Key, ttlms, e.Value, "REPLACE"))
//        if err != nil {
//            log.Info(err, "restore command error key:%s", e.Key)
//        }
//        if s != "OK" {
//            log.Info("restore command response = '%s', should be 'OK'", s)
//        }
//    }
}

func iocopy(r io.Reader, w io.Writer, p []byte, max int) int {
	if max <= 0 || len(p) == 0 {
		log.Panicf("invalid max = %d, len(p) = %d", max, len(p))
	}
	if len(p) > max {
		p = p[:max]
	}
	if n, err := r.Read(p); err != nil {
		log.PanicError(err, "read error")
	} else {
		p = p[:n]
	}
	if _, err := w.Write(p); err != nil {
		log.PanicError(err, "write error")
	}
	return len(p)
}

func flushWriter(w *bufio.Writer) {
	if err := w.Flush(); err != nil {
		log.PanicError(err, "flush error")
	}
}

func newRDBLoader(reader *bufio.Reader, rbytes *atomic2.Int64, size int) chan *rdb.BinEntry {
	pipe := make(chan *rdb.BinEntry, size)
	go func() {
		defer close(pipe)
		l := rdb.NewLoader(stats.NewCountReader(reader, rbytes))
		if err := l.Header(); err != nil {
			log.PanicError(err, "parse rdb header error")
		}
		for {
			if entry, err := l.NextBinEntry(); err != nil {
				log.PanicError(err, "parse rdb entry error")
			} else {
				if entry != nil {
					pipe <- entry
				} else {
					if err := l.Footer(); err != nil {
						log.PanicError(err, "parse rdb checksum error")
					}
					return
				}
			}
		}
	}()
	return pipe
}
