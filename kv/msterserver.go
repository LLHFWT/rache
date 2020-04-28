package kv

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"rache/raft"
	"sync"
	"time"
)

const (
	LEAVE   = "Leave"
	JOIN    = "Join"
	MOVE    = "Move"
	QUERY   = "Query"
	NShards = 10
)

// 是master集群的一个节点，master集群负责管理config以及shard的分配
type Master struct {
	mu         sync.Mutex
	me         int32
	rf         *raft.Raft
	applyChan  chan raft.ApplyMsg
	cliToSeq   map[int64]int64 // 保存有每个Client最新的Sequence，用于查重
	resultChan map[int64]chan *MasterResult
	configs    []Config //保存历史配置信息
	stopChan   chan struct{}
	UnimplementedMasterServer
}

// 调用结果
type MasterResult struct {
	Command     string
	ClientId    int64
	Sequence    int64
	Ok          bool
	WrongLeader bool
	Err         string
	Config      Config //Query方法的返回结果，只有它需要返回结果
}

// 实现Join RPC方法，增加一系列的复制组，提交结果到raft，并根据applyJoin的结果进行返回
func (m *Master) Join(ctx context.Context, args *JoinArgs) (*JoinReply, error) {
	op := MasterOp{
		Command:  JOIN,
		ClientId: args.ClientId,
		Sequence: args.Sequence,
		Groups:   args.Groups,
	}
	// 写入Raft,并返回结果，结果为applyJoin的返回值
	reply := new(JoinReply)
	result := m.sendToRaft(op)
	if !result.Ok {
		reply.WrongLeader = true
		return reply, nil
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	return reply, nil
}

// 实现Leave RPC方法，将给定的复制组从集群中删除
func (m *Master) Leave(ctx context.Context, args *LeaveArgs) (*LeaveReply, error) {
	op := MasterOp{
		Command:  LEAVE,
		ClientId: args.ClientId,
		Sequence: args.Sequence,
		GIDs:     args.GIDs,
	}
	// 写入Raft,并返回结果，结果为applyLeave的返回值
	reply := new(LeaveReply)
	result := m.sendToRaft(op)
	if !result.Ok {
		reply.WrongLeader = true
		return reply, nil
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	return reply, nil
}

//实现Move RPC方法，将一个shard移到另一个group
func (m *Master) Move(ctx context.Context, args *MoveArgs) (*MoveReply, error) {
	op := MasterOp{
		Command:  MOVE,
		ClientId: args.ClientId,
		Sequence: args.Sequence,
		Shard:    args.Shard,
		GID:      args.GID,
	}
	// 写入Raft,并返回结果，结果为applyMove的返回值
	reply := new(MoveReply)
	result := m.sendToRaft(op)
	if !result.Ok {
		reply.WrongLeader = true
		return reply, nil
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	return reply, nil
}

// 实现Query RPC方法，返回指定的配置
func (m *Master) Query(ctx context.Context, args *QueryArgs) (*QueryReply, error) {
	op := MasterOp{
		Command:  QUERY,
		ClientId: args.ClientId,
		Sequence: args.Sequence,
		Num:      args.Num,
	}
	// 写入Raft,并返回结果，结果为applyQuery的返回值
	reply := new(QueryReply)
	result := m.sendToRaft(op)
	if !result.Ok {
		reply.WrongLeader = true
		return reply, nil
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Config = &result.Config
	return reply, nil
}

// 将操作写入Raft
func (m *Master) sendToRaft(op MasterOp) *MasterResult {
	// 提交日志到Raft，如果不是Leader则返回false，客户端重试
	opBytes, _ := proto.Marshal(&op)
	index, _, isLeader := m.rf.Submit(opBytes)
	if !isLeader {
		return &MasterResult{Ok: false}
	}
	// 初始化通知通道
	m.mu.Lock()
	if _, ok := m.resultChan[index]; !ok {
		m.resultChan[index] = make(chan *MasterResult, 1)
	}
	m.mu.Unlock()

	// 等待日志写入Raft的结果，并限制写入超时
	select {
	case result := <-m.resultChan[index]:
		if op.ClientId == result.ClientId && op.Sequence == result.Sequence {
			return result
		}
		return &MasterResult{Ok: false}
	case <-time.After(240 * time.Millisecond):
		return &MasterResult{Ok: false}
	}
}

// 退出
func (m *Master) Kill() {
	m.rf.Kill()
	m.stopChan <- struct{}{}
}

// 创建一个master节点
func StartServer(me int32, persist *raft.Persist, raftClients []raft.RaftClient) *Master {
	m := new(Master)
	m.me = me
	// 初始化0号config，不包含任何group，所有的shard都分配给了0号group(虚拟)
	m.configs = make([]Config, 1)
	m.configs[0].Groups = map[int32]*Servers{}
	m.configs[0].Shards = make([]int32, NShards)
	m.applyChan = make(chan raft.ApplyMsg)
	// 创建该server的核心Raft节点
	m.rf = raft.NewRaftPeer(me, persist, m.applyChan, raftClients)
	m.stopChan = make(chan struct{})

	m.cliToSeq = make(map[int64]int64)
	m.resultChan = make(map[int64]chan *MasterResult)

	// 启动server
	go m.Run()
	return m
}

// 核心执行流程
func (m *Master) Run() {
	for {
		select {
		case <-m.stopChan:
			return
		case msg := <-m.applyChan:
			if msg.CommandValid {
				op := new(MasterOp)
				_ = proto.Unmarshal(msg.Command, op)
				// 初始化执行结果
				result := &MasterResult{
					Command:     op.Command,
					ClientId:    op.ClientId,
					Sequence:    op.Sequence,
					Ok:          true,
					WrongLeader: false,
				}
				m.mu.Lock()
				// 按照操作的类型去处理
				switch op.Command {
				case JOIN:
					if !m.isDupReq(op) {
						m.applyJoin(op)
					}
					result.Err = OK
				case LEAVE:
					if !m.isDupReq(op) {
						m.applyLeave(op)
					}
					result.Err = OK
				case MOVE:
					if !m.isDupReq(op) {
						m.applyMove(op)
					}
					result.Err = OK
				case QUERY:
					// query操作只读，不会改变数据，所以不用去重
					// 如果指定的Num==-1或者超出configs长度，返回最新config
					if op.Num == -1 || int(op.Num) >= len(m.configs) {
						result.Config = m.configs[len(m.configs)-1]
					} else {
						result.Config = m.configs[op.Num]
					}
					result.Err = OK
				}
				// 记录客户端的最新请求序号，避免重复
				m.cliToSeq[op.ClientId] = op.Sequence
				// 发送执行完成的结果的通知，选出通知channel-->发出通知
				if ch, ok := m.resultChan[msg.CommandIndex]; ok {
					select {
					case <-ch: //如果已经有数据，需要排空
					default:
					}
				} else {
					m.resultChan[msg.CommandIndex] = make(chan *MasterResult, 1)
				}
				m.resultChan[msg.CommandIndex] <- result
				m.mu.Unlock()
			}
		}
	}
}

// 执行Join操作，创建一个新的config，加入给定的group，并重新均衡shard至各group,在加锁下执行
func (m *Master) applyJoin(op *MasterOp) {
	// 创建新的config
	newConf := m.makeConfig()
	// 添加新加入的groups
	for gid, servers := range op.Groups {
		// 添加group
		newConf.Groups[gid] = servers
		//将还未分配group的shards分配给该group，之后再均衡
		for i := 0; i < NShards; i++ {
			if newConf.Shards[i] == 0 {
				newConf.Shards[i] = gid
			}
		}
	}
	// 进行均衡分配shards
	m.balance(&newConf)
	// 追加新的配置，Join完成
	m.configs = append(m.configs, newConf)
}

// 执行Leave操作，创建一个新的config，将给定的groups从master删掉，并重新均衡,在加锁下执行
func (m *Master) applyLeave(op *MasterOp) {
	newConf := m.makeConfig()
	//找到一个不被删除的group，然后把要删除的group的shard先分配给它，之后再均衡
	sid := int32(0)
	for gid := range newConf.Groups {
		stay := true
		for _, deletedGid := range op.GIDs {
			if gid == deletedGid {
				stay = false
			}
		}
		if stay {
			sid = gid
			break
		}
	}
	// 删除指定的groups，并转移他们的shard
	for _, gid := range op.GIDs {
		for i := 0; i < len(newConf.Shards); i++ {
			if newConf.Shards[i] == gid {
				newConf.Shards[i] = sid
			}
		}
		delete(newConf.Groups, gid)
	}
	// 均衡后追加新config
	m.balance(&newConf)
	m.configs = append(m.configs, newConf)
}

// 执行Move操作，创建一个新的config，将指定的shard转移到目标group，因为是人为转移，只转移一个所以不需要均衡,在加锁下执行
func (m *Master) applyMove(op *MasterOp) {
	newConf := m.makeConfig()
	// 将shard指定给group
	newConf.Shards[op.Shard] = op.GID
	m.configs = append(m.configs, newConf)
}

// 检查是否是重复的请求
func (m *Master) isDupReq(op *MasterOp) bool {
	if lastSequence, ok := m.cliToSeq[op.ClientId]; ok && lastSequence >= op.Sequence {
		return true
	}
	return false
}

// 基于当前最新的config创建一个新的config，将序号加一,其他内容不变
func (m *Master) makeConfig() Config {
	curConf := m.configs[len(m.configs)-1]
	newConf := Config{
		Num:    curConf.Num + 1,
		Shards: make([]int32, len(curConf.Shards)),
		Groups: map[int32]*Servers{},
	}
	// 惨痛的bug，注意copy一下，不然指向相同地址
	copy(newConf.Shards, curConf.Shards)
	for gid, servers := range curConf.Groups {
		newConf.Groups[gid] = servers
	}
	return newConf
}

// 将shards在各节点间进行均衡，核心目的是让各个group拥有尽可能一样多的shard
func (m *Master) balance(config *Config) {
	// groupId->shards统计每个group的shards
	gidToShards := make(map[int32][]int)
	for gid := range config.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}
	// 如果没有group时，将所有shards分配给虚拟的0组
	if len(config.Groups) == 0 {
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = 0
		}
	} else {
		// 不断将拥有最多shards的group的shard移动到最小的那个group，每次移动一个shard
		for {
			// src是拥有shards最多的group，dst是拥有shards最少的group
			src, dst, max, min := int32(0), int32(0), 0, 0
			for gid, shards := range gidToShards {
				if src == 0 || max < len(shards) {
					max = len(shards)
					src = gid
				}
				if dst == 0 || min > len(shards) {
					dst = gid
					min = len(shards)
				}
			}
			// 最大和最小差值不大于1时不需要交换
			if max-min <= 1 {
				break
			}
			// 将src group中的一个shard移到dst group
			N := len(gidToShards[src]) - 1
			config.Shards[gidToShards[src][N]] = dst
			gidToShards[dst] = append(gidToShards[dst], gidToShards[src][N])
			gidToShards[src] = gidToShards[src][:N]
		}
	}
}

func (m *Master) Raft() *raft.Raft {
	return m.rf
}
