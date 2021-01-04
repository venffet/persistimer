package persistimer

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
)

func Test_timer(t *testing.T) {
	redis := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		Password:     "",
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		DialTimeout:  1 * time.Second,
		PoolSize:     16,
		MaxRetries:   3,
	})

	mgr, err := NewManager("timer_name", 128, redis)
	if err != nil {
		t.Fatalf("faild to new timer manager: %s", err)
	}

	type testcase struct {
		id      string        // timer id
		ctx     string        // 定时器上下文
		timeout time.Duration // 定时器超时时间
		ts      time.Time     // 定时器开始时间
	}

	all := map[string]*testcase{
		"1": &testcase{id: "1", ctx: "ctx1", timeout: 3 * time.Second},
		"2": &testcase{id: "2", ctx: "ctx2", timeout: 8 * time.Second},
	}

	for id, c := range all {
		c.ts = time.Now()
		mgr.AddTimer(&Timer{
			ID:       id,
			Ctx:      c.ctx,
			Deadline: time.Now().Add(c.timeout),
		})
	}

	ch := mgr.GetNotifys()
	for i := 0; i < len(all); i++ {
		select {
		case timer := <-ch:
			c, ok := all[timer.ID]
			if !ok {
				t.Fatalf("failed to get case: %s", timer.ID)
			}
			if timer.Ctx != c.ctx {
				t.Fatalf("wrong ctx, expect %s, actual %s", c.ctx, timer.Ctx)
			}
			if delta := time.Now().Sub(c.ts); delta < c.timeout {
				t.Fatalf("wrong deadline, expect interval %f, actual %f", c.timeout.Seconds(), delta.Seconds())
			}
			t.Logf("get timeout event: %+v", c)
		case <-time.After(50 * time.Second):
			t.Fatalf("failed to get timeout event: blocking")
		}
	}
}
