package persistimer

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

// Timer 定时器
type Timer struct {
	ID       string    // 定时器ID，需要保持唯一
	Ctx      string    // 定时器上下文，用于存储上层业务数据
	Deadline time.Time // 定时发生时间，只有一次
}

// Manager 简单的不易失定时器管理器： 进程重启不丢失定时器
// 缺点：
//   1. 不能持久化回调函数
//   2. 单个zset保持所有定时器，如果定时器过多，存在负载均衡问题，需要对定时器分片.
type Manager struct {
	name    string        // 全局唯一
	redis   *redis.Client // redis客户端
	notifys chan *Timer   // 超时的定时器
}

// NewManager 生成定时器管理器对象
func NewManager(name string, cap int, redis *redis.Client) (*Manager, error) {

	mgr := &Manager{
		name:    name,
		redis:   redis,
		notifys: make(chan *Timer, cap),
	}

	go mgr.background()

	return mgr, nil
}

// GetNotifys 获取定时通知管道
func (mgr *Manager) GetNotifys() <-chan *Timer {
	return mgr.notifys
}

func (mgr *Manager) background() {
	for {
		result, err := mgr.redis.BZPopMin(time.Minute, mgr.name).Result()
		if err != nil {
			if err != redis.Nil { // not timeout?
				zap.L().Warn(fmt.Sprintf("failed to bzpopmin %s: %s", mgr.name, err))
				time.Sleep(3 * time.Second)
			}
			continue
		}
		id, ok := result.Member.(string)
		if !ok {
			zap.L().Warn(fmt.Sprintf("failed to bzpopmin %s: member type is %T", mgr.name, result.Member))
			continue
		}

		deadline := int64(result.Score)
		if delta := deadline - time.Now().Unix(); delta > 0 { // 未到期?
			// 重新设置回去，并休眠等待
			mgr.redis.ZAdd(mgr.name, redis.Z{Member: id, Score: float64(deadline)})
			time.Sleep(time.Duration(delta) * time.Second)
			continue
		}

		// 发生超时
		ctx, err := mgr.redis.Get(mgr.ContextKey(id)).Result()
		if err != nil {
			zap.L().Warn(fmt.Sprintf("failed to get timer context: %s", err))
			continue
		}
		t := &Timer{
			ID:       id,
			Ctx:      ctx,
			Deadline: time.Unix(deadline, 0),
		}
		// ok
		select {
		case mgr.notifys <- t:
			// NOOP
		case <-time.After(3 * time.Second):
			zap.L().Error(fmt.Sprintf("failed to put into timer: notify channel overflow"))
		}
	}
}

// AddTimer 增加定时器
func (mgr *Manager) AddTimer(t *Timer) error {
	pipe := mgr.redis.Pipeline()
	defer pipe.Close()

	// ttl
	exp := time.Minute * 10 // 冗余量
	if now := time.Now(); t.Deadline.After(now) {
		exp += t.Deadline.Sub(now)
	}
	pipe.Set(mgr.ContextKey(t.ID), t.Ctx, exp)
	pipe.ZAdd(mgr.name, redis.Z{
		Member: t.ID,
		Score:  float64(t.Deadline.Unix()),
	})
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("failed to add timer: %s", err)
	}

	return nil
}

// DelTimer 删除定时器
// 注意：当收到定时器通知事件时，该定时器已经pop出来，不需要再显示删除;
func (mgr *Manager) DelTimer(id string) error {
	pipe := mgr.redis.Pipeline()
	defer pipe.Close()

	pipe.ZRem(mgr.name, id)
	pipe.Del(mgr.ContextKey(id))
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("failed to del timer: %s", err)
	}

	return nil
}

// ContextKey 定时器上下文键名
func (mgr *Manager) ContextKey(id string) string {
	return fmt.Sprintf("%s_%s", mgr.name, id)
}
