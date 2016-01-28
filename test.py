import unittest
import testing.redis
import redis
import subprocess
import time

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.key_fs = "etf:FS:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0:59"
        self.key_fs_to_zset = "etf:F:/564c13b8f085fc471efdfff8/user_broadcast_277732900865/p/0"
        self.tags_scores = [
            {'tag': 'abj-front-1$0', 'score': 0},
            {'tag':'abj-front-1$1', 'score': (1<<24) + 1},
            {'tag': 'abj-front-zb1$1', 'score': (1<<24) + (3<<16) + 1}
        ]

        self.redis_src = testing.redis.RedisServer()
        self.redis_target = testing.redis.RedisServer()
        self.redis_rt = testing.redis.RedisServer()

        self.redis_src.start()
        self.redis_target.start()
        self.redis_rt.start()

        self.redis_src_info = self.redis_src.dsn()
        self.redis_target_info = self.redis_target.dsn()
        self.redis_rt_info = self.redis_rt.dsn()

        print "wait server started"
        time.sleep(3)

        print "connecting to source redis"
        self.rs = redis.StrictRedis(host=self.redis_src_info['host'], port=self.redis_src_info['port'], db=self.redis_src_info['db'])
        self.rs.sadd(self.key_fs, 1)

        print "connecting to target redis"
        self.rt = redis.StrictRedis(host=self.redis_target_info['host'], port=self.redis_target_info['port'], db=self.redis_target_info['db'])
        print "connecting to route table redis"
        self.rr = redis.StrictRedis(host=self.redis_rt_info['host'], port=self.redis_rt_info['port'], db=self.redis_rt_info['db'])
        self.rr.set(1, self.tags_scores[1]['tag'])

        print "starting redis-port"
        self.redis_port = subprocess.Popen([
            "bin/redis-port", "sync",
            "--from=%s:%d" % (self.redis_src_info['host'], self.redis_src_info['port']),
            "--target=%s:%d" % (self.redis_target_info['host'], self.redis_target_info['port']),
            "--redisrt=%s:%d" % (self.redis_rt_info['host'], self.redis_rt_info['port'])
        ])

        # wait seconds to make sure rdb restore finished
        time.sleep(5)

    def tearDown(self):
        self.redis_port.kill()

        self.redis_src.stop()
        self.redis_target.stop()
        self.redis_rt.stop()

    def test_port(self):
        # case: restore rdb
        zset = self.rt.zrange(self.key_fs_to_zset, 0, -1, withscores=True)
        self.assertEqual(len(zset), 1)
        self.assertEqual(zset[0][0], '1')
        self.assertEqual(zset[0][1], self.tags_scores[1]['score'])

        # case: TODO restore rdb: not route table
        # ...

        # case: TODO re-sadd uid 1
        # ...

        # case: sadd add a new uid
        self.rr.set(2, self.tags_scores[2]['tag'])
        self.rs.sadd(self.key_fs, 2)
        # wait 1s to make sure sync happend
        time.sleep(1)
        zset = self.rt.zrange(self.key_fs_to_zset, 0, -1, withscores=True)
        # print "zset", zset
        self.assertEqual(len(zset), 2)
        self.assertEqual(zset[1][0], '2')
        self.assertEqual(zset[1][1], self.tags_scores[2]['score'])

        # case: sadd: no route table
        self.rs.sadd(self.key_fs, 100)
        time.sleep(1)
        zset = self.rt.zrange(self.key_fs_to_zset, 0, -1, withscores=True)
        print "zset", zset
        self.assertEqual(len(zset), 3)
        self.assertEqual(zset[0][0], '100')
        self.assertEqual(zset[0][1], 0)

if __name__ == '__main__':
    unittest.main()

