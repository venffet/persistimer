# persistimer
implement a persistent timer using Redis

简单的不易失定时器管理器： 进程重启不丢失定时器.  

缺点：
1. 不能持久化回调函数
2. 单个zset保持所有定时器，如果定时器过多，存在负载均衡问题，需要对定时器分片.
