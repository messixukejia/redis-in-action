/********分布式锁需要考虑的场景********/
//1、获取锁之后，进程故障。需要给锁加过期时间，需要保证原子性。所以使用SET，如果使用SETNX后加超时，需要靠其他进程检查到无过期时间后设置。
//2、del误删场景。判断是否自己加的，是才删。判断、删要保持原子性，可以通过lua实现。
//3、获取锁后，业务处理时间超过锁过期时间，需要开守护线程续航。

/************SET注意事项**************/
//命令 SET resource-name anystring NX EX max-lock-time 是一种在 Redis 中实现锁的简单方法。
//客户端执行以上的命令：
//如果服务器返回 OK ，那么这个客户端获得锁。
//如果服务器返回 NIL ，那么客户端获取锁失败，可以在稍后再重试。
//设置的过期时间到达之后，锁将自动释放。

//可以通过以下修改，让这个锁实现更健壮：
//不使用固定的字符串作为键的值，而是设置一个不可猜测（non-guessable）的长随机字符串，作为口令串（token）。
//不使用 DEL 命令来释放锁，而是发送一个 Lua 脚本，这个脚本只在客户端传入的值和键的口令串相匹配时，才对键进行删除。
//这两个改动可以防止持有过期锁的客户端误删现有锁的情况出现。

//NX ：只在键不存在时，才对键进行设置操作。 SET key value NX 效果等同于 SETNX key value 。
//XX ：只在键已经存在时，才对键进行设置操作。
//NX用于初始创建，XX用于续时间

package dist_lock

import (
	"crypto/rand"
	"encoding/base64"
	"sync"
	"time"

	"errors"
	"github.com/gomodule/redigo/redis"
)

type diskLock interface {
	//首次获取锁
	Lock(c redis.Conn, name string, acquireTime, lockTime time.Duration) error
	//释放锁
	Unlock(c redis.Conn) error
}

type DistLock struct {
	name      string
	lockTime  time.Duration
	token     string
	lockMutex sync.Mutex
	releasec  chan interface{}
}

func (m *DistLock) Lock(c redis.Conn, name string, acquireTime, lockTime time.Duration) error {
	m.lockMutex.Lock()
	defer m.lockMutex.Unlock()

	token, err := m.genToken()
	if err != nil {
		return err
	}

	m.name = name
	m.lockTime = lockTime
	m.token = token

	endTime := time.Duration(time.Now().Unix()) + acquireTime
	for {
		if time.Duration(time.Now().Unix()) >= endTime {
			return errors.New("lock timeout")
		}

		if ok := m.acquire(c); ok {
			m.releasec = make(chan interface{})
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	//守护Goroutine，用于续时间
	go func() {
		for {
			delayTime := m.lockTime * time.Second * 9 / 10 //适当提前一点时间
			select {
			case <-time.After(delayTime):
				m.reNew(c)
			case <-m.releasec:
				break
			}
		}
	}()
	return nil
}

func (m *DistLock) Unlock(c redis.Conn) error {
	m.lockMutex.Lock()
	defer m.lockMutex.Unlock()

	var ok bool
	if ok = m.release(c); ok {
		if m.releasec != nil {
			close(m.releasec)
			m.releasec = nil
		}
	}
	return nil
}

func (m *DistLock) genToken() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (m *DistLock) acquire(c redis.Conn) bool {
	reply, err := redis.String(c.Do("SET", m.name, m.token, "NX", "EX", int(m.lockTime)))
	return err == nil && reply == "OK"
}

var deleteScript = redis.NewScript(1, `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

func (m *DistLock) release(c redis.Conn) bool {
	status, err := deleteScript.Do(c, m.name, m.token)
	return err == nil && status != 0
}

var renewScript = redis.NewScript(1, `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("SET", KEYS[1], ARGV[1], "XX", "PX", ARGV[2])
	else
		return "ERR"
	end
`)

//锁占有时长超过lockTime时长，会自动释放。需要续时间。
func (m *DistLock) reNew(c redis.Conn) bool {
	status, err := redis.String(renewScript.Do(c, m.name, m.token, m.lockTime))
	return err == nil && status != "ERR"
}
