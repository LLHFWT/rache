package kv

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/gogo/protobuf/proto"
	"rache/raft"
	"sync"
	"time"
)

const (
	// 操作返回的错误类型
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
	// server执行的操作类型
	RECONFIGURE = "Reconfigure"
	DELETE      = "Delete"
	GET         = "Get"
	PUT         = "Put"
	APPEND      = "Append"
)

var GroupClientMap = make(map[string]GroupClient)

// 每个ShardKV结构代表了一个group中的server，负责管理相应的shards数据
type ShardKV struct {
	mu           sync.Mutex
	me           int32              // 在group中的index
	rf           *raft.Raft         //每个group由一系列的raft节点组成
	applyCh      chan raft.ApplyMsg //raft应用成功的消息接收通道
	gid          int32              //该ShardKV的group ID
	masters      []MasterClient     // shard masters
	maxRaftState int                // snapshot if log grows this big

	storage    [NShards]map[string]string // 存放数据
	cliToSeq   map[int64]int64            //客户端发送的操作的最新序号，用于去重
	resultChan map[int64]chan KVResult    //执行结果的通知
	config     *Config                    //当前配置
	masterCli  *MClient                   // master节点的客户端
	stopChan   chan struct{}              //用于停止应用
	persist    *raft.Persist
	UnimplementedGroupServer
}

// 操作的执行结果
type KVResult struct {
	Command     string //操作类型
	OK          bool   //OK==false代表请求的当前节点不是Leader，需要更换请求节点
	WrongLeader bool
	Err         string //错误类型，OK代表成功
	ClientId    int64
	Sequence    int64
	Value       string //返回值
	ConfigNum   int32  //用于更新配置
}

// 实现Get RPC操作
func (kv *ShardKV) Get(ctx context.Context, args *GetArgs) (*GetReply, error) {
	op := KVOp{
		Command:  GET,
		ClientId: args.ClientId,
		Sequence: args.Sequence,
		Key:      args.Key,
	}
	// 将操作提交至raft
	result := kv.sendToRaft(op)
	// 操作执行失败说明当前节点不是Leader，需要客户端调整
	reply := new(GetReply)
	if !result.OK {
		reply.WrongLeader = true
		return reply, nil
	}
	// OK 返回结果
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Value = result.Value
	return reply, nil
}

// 实现PutAppend RPC操作
func (kv *ShardKV) PutAppend(ctx context.Context, args *PutAppendArgs) (*PutAppendReply, error) {
	op := KVOp{
		Command:  args.Op,
		ClientId: args.ClientId,
		Sequence: args.Sequence,
		Key:      args.Key,
		Value:    args.Value,
	}
	// 将操作提交至Raft
	result := kv.sendToRaft(op)
	reply := new(PutAppendReply)
	if !result.OK {
		reply.WrongLeader = true
		return reply, nil
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	return reply, nil
}

// 清理Shards的RPC操作
func (kv *ShardKV) DeleteShard(ctx context.Context, args *ClearShardArgs) (*ClearShardReply, error) {
	reply := new(ClearShardReply)
	// 写操作只有Leader可以执行
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return reply, nil
	}

	// 确保自己的config应比参数的更新才可以清理
	if args.Num > kv.config.Num {
		reply.WrongLeader = false
		reply.Err = ErrNotReady
		return reply, nil
	}

	// 提交到Raft同意后进行清理
	op := KVOp{
		Command:    DELETE,
		ConfigNum:  args.Num,
		ShardIndex: args.ShardIndex,
	}
	kv.sendToRaft(op)
	// 不要阻塞去等待结果，异步即可
	reply.WrongLeader = false
	reply.Err = OK

	return reply, nil
}

// RPC方法：返回shards数据以及ack信息给请求方
func (kv *ShardKV) TransferShard(ctx context.Context, args *TransferShardArgs) (*TransferShardReply, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := new(TransferShardReply)

	if kv.config.Num < args.Num {
		// 如果自己还处于旧的配置，还不能转移数据，可能正在处理请求
		reply.Err = ErrNotReady
		return reply, nil
	}

	// 初始化返回数据
	reply.Data = make([]*KVData, NShards)
	for i := 0; i < NShards; i++ {
		reply.Data[i] = new(KVData)
		reply.Data[i].Store = make(map[string]string)
	}
	// 将请求的shards数据返回给请求方
	for _, shardId := range args.ShardIds {
		for k, v := range kv.storage[shardId] {
			reply.Data[shardId].Store[k] = v
		}
	}
	// 返回已处理的client-sequence的映射关系
	reply.ClientToSeq = make(map[int64]int64)
	for clientId, requestId := range kv.cliToSeq {
		reply.ClientToSeq[clientId] = requestId
	}
	reply.Err = OK
	return reply, nil
}

// 将操作提交至raft
func (kv *ShardKV) sendToRaft(op KVOp) KVResult {
	//提交日志到Raft
	opBytes, _ := proto.Marshal(&op)
	index, _, isLeader := kv.rf.Submit(opBytes)
	if !isLeader {
		// 如果当前节点不是leader，则反馈OK==false，
		return KVResult{OK: false}
	}
	// 初始化结果通道
	kv.mu.Lock()
	if _, ok := kv.resultChan[index]; !ok {
		kv.resultChan[index] = make(chan KVResult, 1)
	}
	kv.mu.Unlock()
	// 等待结果
	select {
	case result := <-kv.resultChan[index]:
		// 检查返回的result是否对应的是当前op
		if op.Command == RECONFIGURE && op.MasterConfig.Num == result.ConfigNum {
			// 重配置操作，验证当前配置的索引和返回结果的索引是否一致
			return result
		}
		if op.Command == DELETE && op.ConfigNum == result.ConfigNum {
			// 清理操作，验证是否是当前配置下进行的
			return result
		}
		// 其他操作需验证客户端ID和请求序号
		if op.ClientId == result.ClientId && op.Sequence == result.Sequence {
			return result
		}
		return KVResult{OK: false}
	case <-time.After(240 * time.Millisecond):
		// 等待超时
		return KVResult{OK: false}
	}
}

// 发送清理无用shards的RPC请求，通知其他group进行清理
func (kv *ShardKV) sendClearShard(gid int32, lastConfig *Config, args *ClearShardArgs) (*ClearShardReply, bool) {
	reply := new(ClearShardReply)
	for _, server := range lastConfig.Groups[gid].ServerList {
		cli := GroupClientMap[server]

		reply, err := cli.ClearShard(context.Background(), args)
		if err == nil {
			if reply.Err == OK {
				return reply, true
			}
			if reply.Err == ErrNotReady {
				return reply, false
			}
		}
	}
	return reply, true
}

//发送RPC请求，从对方gid处获取shards数据以及CliToSeq
func (kv *ShardKV) sendTransferShard(gid int32, args *TransferShardArgs) (*TransferShardReply, bool) {
	// 向对方group的所有server发送请求，任何一个准备好就行，因为他们的数据是一样的
	reply := new(TransferShardReply)
	for _, server := range kv.config.Groups[gid].ServerList {
		cli := GroupClientMap[server]
		reply, err := cli.TransferShard(context.Background(), args)
		if err == nil {
			if reply.Err == OK {
				return reply, true
			}
			if reply.Err == ErrNotReady {
				return reply, false
			}
		}
	}
	return reply, true
}

// 停止运行
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.stopChan <- struct{}{}
}

// 创建一个server
func NewKVServer(raftClients []raft.RaftClient, me int32, persist *raft.Persist, maxraftstate int, gid int32, masters []MasterClient) *ShardKV {
	gob.Register(Config{})

	// 初始化数据
	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxraftstate
	kv.gid = gid
	kv.masters = masters
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persist = persist
	kv.rf = raft.NewRaftPeer(me, persist, kv.applyCh, raftClients)
	kv.masterCli = NewMClient(masters)
	kv.cliToSeq = make(map[int64]int64)
	kv.stopChan = make(chan struct{})
	kv.resultChan = make(map[int64]chan KVResult)
	kv.config = &Config{
		Num:    0,
		Shards: make([]int32, NShards),
		Groups: nil,
	}
	for i := 0; i < NShards; i++ {
		kv.storage[i] = make(map[string]string)
	}

	snapshot, _ := kv.persist.ReadSnapshot()
	kv.decodeSnapshot(snapshot)

	// 启动goroutine，执行核心逻辑
	go kv.Run()
	// 配置发生变化时负责重配置，只有Leader才会执行
	go kv.Reconfigure()

	return kv
}

// 核心处理逻辑，负责与客户端交互
func (kv *ShardKV) Run() {
	for {
		select {
		case <-kv.stopChan:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid { //该值为false说明收到了snapshot
				// 从Command中提取snapshot
				snapshot := msg.Command
				kv.mu.Lock()
				kv.decodeSnapshot(snapshot)
				kv.mu.Unlock()
			} else {
				//执行op并发送result
				op := KVOp{}
				_ = proto.Unmarshal(msg.Command, &op)
				kv.mu.Lock()
				result := KVResult{
					Command:     op.Command,
					OK:          true,
					WrongLeader: false,
					ClientId:    op.ClientId,
					Sequence:    op.Sequence,
				}

				//根据操作类型执行op
				switch op.Command {
				case GET:
					// 执行Get操作
					kv.applyGet(op, &result)
				case PUT:
					// 执行Put操作
					kv.applyPut(op, &result)
				case APPEND:
					//执行Append操作
					kv.applyAppend(op, &result)
				case DELETE:
					// 清理内存
					kv.applyCleanup(op)
				case RECONFIGURE:
					// 配置更新，转移shard
					kv.applyReconfigure(op, &result)
				}
				// 初始化结果channel并发送结果
				if ch, ok := kv.resultChan[msg.CommandIndex]; ok {
					select {
					case <-ch: // drain bad data
					default:
					}
				} else {
					kv.resultChan[msg.CommandIndex] = make(chan KVResult, 1)
				}
				kv.resultChan[msg.CommandIndex] <- result

				// 检查是否需要snapshot
				kv.checkIfSnapshot(msg.CommandIndex)
				kv.mu.Unlock()
			}
		}
	}
}

// 执行下面的操作都需要检查key是否属于该group，并对操作进行重复检查
// 执行PUT操作
func (kv *ShardKV) applyPut(op KVOp, result *KVResult) {
	if !kv.checkKey(op.Key) {
		result.Err = ErrWrongGroup
		return
	}
	// 如果不是重复请求，则将值进行PUT
	if kv.checkDup(op) {
		kv.storage[consistentMap.Get(op.Key)][op.Key] = op.Value
		kv.cliToSeq[op.ClientId] = op.Sequence
	}
	result.Err = OK
}

//执行Append操作
func (kv *ShardKV) applyAppend(op KVOp, result *KVResult) {
	if !kv.checkKey(op.Key) {
		result.Err = ErrWrongGroup
		return
	}
	if kv.checkDup(op) {
		kv.storage[consistentMap.Get(op.Key)][op.Key] += op.Value
		kv.cliToSeq[op.ClientId] = op.Sequence
	}
	result.Err = OK
}

// 执行Get操作
func (kv *ShardKV) applyGet(op KVOp, result *KVResult) {
	if !kv.checkKey(op.Key) {
		result.Err = ErrWrongGroup
		return
	}
	// 追加去重
	if kv.checkDup(op) {
		kv.cliToSeq[op.ClientId] = op.Sequence
	}
	// 存在key时返回结果，否则返回ErrNoKey
	if value, ok := kv.storage[consistentMap.Get(op.Key)][op.Key]; ok {
		result.Value = value
		result.Err = OK
	} else {
		result.Err = ErrNoKey
	}
}

// 执行内存清理操作
func (kv *ShardKV) applyCleanup(op KVOp) {
	// 当group的config保持最新，并且参数中的shard已不属于该group时，清空该shard的内存
	if kv.config.Num >= op.ConfigNum && kv.gid != kv.config.Shards[op.ShardIndex] {
		kv.storage[op.ShardIndex] = make(map[string]string)
	}
}

// 执行Reconfigure操作
func (kv *ShardKV) applyReconfigure(op KVOp, result *KVResult) {
	result.ConfigNum = op.MasterConfig.Num
	// 仅执行下一个config，保证顺序性
	if op.MasterConfig.Num == kv.config.Num+1 {
		// 更新自己的shards
		for shardIndex, shardData := range op.Data {
			for k, v := range shardData.Store {
				kv.storage[shardIndex][k] = v
			}
		}
		// 合并自己的CliToSeq,仅当参数中的sequence更大时才进行更新
		for clientId := range op.ClientToSeq {
			if _, ok := kv.cliToSeq[clientId]; !ok || kv.cliToSeq[clientId] < op.ClientToSeq[clientId] {
				kv.cliToSeq[clientId] = op.ClientToSeq[clientId]
			}
		}

		// 更新config
		lastConfig := kv.config
		kv.config = op.MasterConfig

		// 自己已经成功应用了这些数据，可以通知其他节点删除
		for shardIndex, shardData := range op.Data {
			if len(shardData.Store) > 0 {
				gid := lastConfig.Shards[shardIndex]
				args := ClearShardArgs{
					Num:        lastConfig.Num,
					ShardIndex: int32(shardIndex),
				}
				go kv.sendClearShard(gid, lastConfig, &args)
			}
		}
	}
	result.Err = OK
}

//检查key是否属于该group
func (kv *ShardKV) checkKey(key string) bool {
	return kv.config.Shards[consistentMap.Get(key)] == kv.gid
}

//检查是否是重复请求,当不重复时返回true
func (kv *ShardKV) checkDup(op KVOp) bool {
	if seq, ok := kv.cliToSeq[op.ClientId]; ok && seq >= op.Sequence {
		return false
	}
	return true
}

// 检查是否需要snapshot，将结果发送给raft节点处理
func (kv *ShardKV) checkIfSnapshot(index int64) {
	if kv.maxRaftState < 0 {
		// 值为负数代表没启用snapshot功能
		return
	}
	size, _ := kv.persist.ReadStateSize()
	if size < kv.maxRaftState*9/10 {
		return
	}
	// 将storage和clientToSeq截取为snapshot，发送给raft更新index并保存
	snapshot := kv.encodeSnapshot()
	go kv.rf.TakeSnapshot(index, snapshot)
}

// 创建snapshot,内容包括cliToSeq、config和数据
func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.cliToSeq)
	e.Encode(kv.config)
	e.Encode(kv.storage)
	return w.Bytes()
}

// 解码snapshot
func (kv *ShardKV) decodeSnapshot(snapshot []byte) {
	r := bytes.NewReader(snapshot)
	e := gob.NewDecoder(r)
	e.Decode(&kv.cliToSeq)
	e.Decode(&kv.config)
	e.Decode(&kv.storage)
}

// 负责监控配置的变化，移动数据,每100ms拉一次配置
func (kv *ShardKV) Reconfigure() {
	// 如果当前server是group raft集群中的Leader，它需要定期从master处获取最新配置并更新配置
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			latestConfig := kv.masterCli.Query(-1)
			for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
				// 顺序应用此期间的所有配置
				config := kv.masterCli.Query(i)
				// 获取新的config下需要拉取的数据
				op, ok := kv.getReconfigureOp(config)
				// 如果获取失败则等待下次继续获取
				if !ok {
					break
				}
				// 将重配置的Op提交给Raft，并执行applyReconfigure操作，只有Leader可以提交日志
				result := kv.sendToRaft(op)
				if !result.OK {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 从其他group收集重配置所需要的shards，并构建Op交给Raft处理
func (kv *ShardKV) getReconfigureOp(newConfig *Config) (KVOp, bool) {
	// 初始化Op
	op := KVOp{
		Command:      RECONFIGURE,
		ClientToSeq:  make(map[int64]int64),
		MasterConfig: newConfig,
		Data:         make([]*KVData, NShards),
	}
	for i := 0; i < NShards; i++ {
		op.Data[i] = new(KVData)
		op.Data[i].Store = make(map[string]string)
	}

	// 该Map记录了该group需要从其他group处拉取的shards(当前config不拥有这些shards，下个config拥有)，gid-->[]shards index
	transferShards := make(map[int32][]int32)
	// 遍历所有的shards，查找当前config不属于自己，但下个config属于自己的shards，记录group-->shards的映射关系
	for i := 0; i < NShards; i++ {
		if kv.config.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
			// 当前shard所属的group
			gid := kv.config.Shards[i]
			if gid != 0 {
				if _, ok := transferShards[gid]; !ok {
					transferShards[gid] = make([]int32, 0)
				}
				// 记录gid所含有的shards
				transferShards[gid] = append(transferShards[gid], int32(i))
			}
		}
	}

	// 并行从对方group处拉取shards
	ok := true
	var ackMu sync.Mutex
	var wg sync.WaitGroup
	for gid, shardIds := range transferShards {
		wg.Add(1)

		go func(gid int32, args TransferShardArgs) {
			defer wg.Done()
			// 向对方group发送请求shards的RPC请求
			reply, yes := kv.sendTransferShard(gid, &args)
			if yes {
				// 如果请求成功，获取到了对方的shards数据
				ackMu.Lock()
				// 将获取到的数据加入到op中，注意排除空数据
				for _, shardId := range args.ShardIds {
					//对应shard保存数据的map
					shardData := reply.Data[shardId]
					// 将该 shard map中的key/value加入到Op中
					for k, v := range shardData.Store {
						op.Data[shardId].Store[k] = v
					}
				}
				// 将对方group的CliToSeq合并到自己的，仅当对方的Sequence更大时才进行替换
				for clientId := range reply.ClientToSeq {
					if _, ok := op.ClientToSeq[clientId]; !ok || op.ClientToSeq[clientId] < reply.ClientToSeq[clientId] {
						op.ClientToSeq[clientId] = reply.ClientToSeq[clientId]
					}
				}
				ackMu.Unlock()
			} else {
				// 记录获取失败
				ok = false
			}
		}(gid, TransferShardArgs{Num: newConfig.Num, ShardIds: shardIds})
	}
	wg.Wait()
	return op, ok
}

func (kv *ShardKV) Raft() *raft.Raft {
	return kv.rf
}
