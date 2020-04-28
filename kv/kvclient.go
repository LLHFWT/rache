package kv

import (
	"context"
	"rache/utils"
	"sync"
	"time"
)

var consistentMap *utils.ConsistentMap

func init() {
	consistentMap = utils.NewConsistentHashMap(10, nil)
	consistentMap.Add(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
}

var RacheClient *KVClient

// 访问分片数据库的client
type KVClient struct {
	config    *Config  //当前的master配置
	masterCli *MClient // 访问master的client
	mu        sync.Mutex
	id        int64
	sequence  int64
}

func NewKVClient(masters []MasterClient) *KVClient {
	return &KVClient{
		masterCli: NewMClient(masters),
		mu:        sync.Mutex{},
		id:        utils.RandNum64(),
		sequence:  0,
		config: &Config{
			Num:    0,
			Shards: make([]int32, NShards),
			Groups: nil,
		},
	}
}

// 查询一个key-value
func (c *KVClient) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: c.id,
	}
	c.mu.Lock()
	args.Sequence = c.sequence
	c.sequence++
	c.mu.Unlock()

	for {
		// 确定当前key所属的分片
		shard := consistentMap.Get(key)
		// 找到对应存储该分片的group
		groupId := c.config.Shards[shard]
		// 遍历该group的所有节点查询value
		if servers, ok := c.config.Groups[groupId]; ok {
			// try each server for the shard.
			for i := 0; i < len(servers.ServerList); i++ {
				// 找到对应的client
				cli := GroupClientMap[servers.ServerList[i]]
				reply := new(GetReply)
				reply, err := cli.Get(context.Background(), &args)
				// 如果找到了key或者没有key，返回结果
				if err == nil && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				// r如果不属于当前组则换组查询
				if err == nil && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask kv for the latest configuration.
		c.config = c.masterCli.Query(-1)
	}
}

// Put or Append 一个key-value
func (c *KVClient) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: c.id,
	}
	c.mu.Lock()
	args.Sequence = c.sequence
	c.sequence++
	c.mu.Unlock()

	for {
		// 确定key所属的分片
		shard := consistentMap.Get(key)
		// 找到该分片对应的group
		gid := c.config.Shards[shard]
		// 遍历该group下的所有节点
		if servers, ok := c.config.Groups[gid]; ok {
			for si := 0; si < len(servers.ServerList); si++ {
				// 找到节点对应的group
				cli := GroupClientMap[servers.ServerList[si]]
				reply := new(PutAppendReply)
				reply, err := cli.PutAppend(context.Background(), &args)

				if err == nil && reply.WrongLeader == false && reply.Err == OK {
					return
				}
				// 不属于当前组需要换组查询
				if err == nil && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		c.config = c.masterCli.Query(-1)
	}
}

func (c *KVClient) Put(key string, value string) {
	c.PutAppend(key, value, PUT)
}
func (c *KVClient) Append(key string, value string) {
	c.PutAppend(key, value, APPEND)
}
