package dist_lock

import (
	"errors"
	"github.com/gomodule/redigo/redis"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		t.Error("connect server failed:", err)
		return
	}

	//清空测试数据
	defer func() {
		c.Close()
	}()

	var testLock1, testLock2 DistLock
	lockchan := make(chan struct{})

	go func() {
		if err := testLock1.Lock(c, "Lock", 5, 10); err != nil {
			t.Error("Lock failed", err)
		}
		testLock1.Unlock(c)
		lockchan <- struct{}{}
	}()

	go func() {
		select {
		case <-lockchan:
		case <-time.After(4 * time.Second):
		}
		if err := testLock2.Lock(c, "Lock", 5, 10); err != errors.New("lock timeout") {
			t.Error("Lock failed", err)
		}
		testLock2.Unlock(c)
	}()

}

func TestLockTime(t *testing.T) {
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		t.Error("connect server failed:", err)
		return
	}

	//清空测试数据
	defer func() {
		c.Close()
	}()

	var testLock1, testLock2 DistLock
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		if err := testLock1.Lock(c, "Lock", 5, 10); err != nil {
			t.Error("Lock failed", err)
		}
		wg.Done()
	}()

	//等待1s，让test1先抢到锁
	time.Sleep(time.Second)

	wg.Add(1)
	go func() {
		err := testLock2.Lock(c, "Lock", 1, 10)
		if !reflect.DeepEqual(err, errors.New("lock timeout")) {
			t.Error("Lock failed", err, time.Now())
		}
		wg.Done()
	}()

	wg.Wait()
}
