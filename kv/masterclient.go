package kv

import (
	"context"
	"rache/utils"
	"sync"
	"time"
)

// Master集群的Client
type MClient struct {
	servers     []MasterClient //所有的shard master节点组成一个raft集群，其中一个是Leader
	id          int64
	sequence    int64
	mu          sync.Mutex
	networkDrop []bool
}

func NewMClient(servers []MasterClient) *MClient {
	return &MClient{
		servers:     servers,
		id:          utils.RandNum64(),
		sequence:    0,
		mu:          sync.Mutex{},
		networkDrop: make([]bool, len(servers)),
	}
}

// 将多组新的服务器加入到集群中
func (c *MClient) Join(groups map[int32]*Servers) {
	args := new(JoinArgs)
	c.mu.Lock()
	args.Sequence = c.sequence
	c.sequence++
	c.mu.Unlock()
	args.ClientId = c.id
	args.Groups = groups

	for {
		// try each known server
		for _, server := range c.servers {
			reply := new(JoinReply)
			reply, err := server.Join(context.Background(), args)
			if err != nil {
				continue
			}
			if err == nil && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 将多个group从集群中删掉
func (c *MClient) Leave(groupIds []int32) {
	args := new(LeaveArgs)
	c.mu.Lock()
	args.Sequence = c.sequence
	c.sequence++
	c.mu.Unlock()
	args.ClientId = c.id
	args.GIDs = groupIds

	for {
		for _, server := range c.servers {
			reply := new(LeaveReply)
			reply, err := server.Leave(context.Background(), args)
			if err != nil {
				continue
			}
			if err == nil && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 将某个分片从原来group转移到目标group
func (c *MClient) Move(shard int32, groupId int32) {
	args := new(MoveArgs)
	c.mu.Lock()
	args.Sequence = c.sequence
	c.sequence++
	c.mu.Unlock()
	args.ClientId = c.id
	args.Shard = shard
	args.GID = groupId

	for {
		for _, server := range c.servers {
			reply := new(MoveReply)
			reply, err := server.Move(context.Background(), args)
			if err != nil {
				continue
			}
			if err == nil && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 获取Master当前的最新配置
func (c *MClient) Query(num int32) *Config {
	args := new(QueryArgs)
	c.mu.Lock()
	args.Sequence = c.sequence
	c.sequence++
	c.mu.Unlock()
	args.ClientId = c.id
	args.Num = num

	for {
		for _, server := range c.servers {
			reply := new(QueryReply)
			reply, err := server.Query(context.Background(), args)
			if err != nil {
				continue
			}
			if err == nil && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
