package raft

import (
	"context"
	"errors"
	"github.com/golang/protobuf/proto"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Leader               = "Leader"
	Candidate            = "Candidate"
	Follower             = "Follower"
	VoteNil              = -1
	StayTerm             = -1
	NilTerm              = -1
	HeartbeatTerm        = 100 * time.Millisecond
	ElectionTimeoutBase  = 400 * time.Millisecond
	ElectionTimeoutExtra = 100
	RpcCallTimeout       = HeartbeatTerm
)

// message for applying command successfully
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int64
}

// timer for election timeout
type electionTimer struct {
	d time.Duration
	t *time.Timer
}

// Raft server
type Raft struct {
	mu sync.RWMutex
	// RPC clients of all raft peers
	peers []RaftClient
	// this raft peer's ip:port
	addrList []string
	// this raft peer's index in peers
	me int32
	// this peer's state, Leader/Candidate/Follower
	state string
	// election timer
	timer *electionTimer
	// channel for receiving ApplyMsg
	applyChan chan ApplyMsg
	// true if this peer stopped
	stopped bool
	// every Raft peer has a condition, use for triggering AppendEntries RPC
	conditions []*sync.Cond
	// persist peer's state to disk
	persist *Persist

	// peer's current term, increase automatically from 0
	currentTerm int64
	// candidate who received vote
	votedFor int32
	// all log entry
	logs []*LogEntry
	// 已经committed的日志索引位置
	commitIndex int64
	// index of log entry applied
	lastApplied int64
	// for each peer, next log entry to send
	nextIndex []int64
	// for each server, highest index of log entry matched
	matchIndex []int64
	// 上一次快照的位置，包含该index
	lastIncludedIndex int64
	// 上一次快照的term
	lastIncludedTerm int64
	// 网络是否断开，用于测试，false代表网络链接断开
	networkDrop bool
	// 用于测试网络不可靠，true代表网络连接不可靠
	networkUnreliable bool
	// RPC server implementation
	UnimplementedRaftServer
}

// submit command to logs，返回之前最新的日志index，当前term，以及是否是leader
func (rf *Raft) Submit(command []byte) (int64, int64, bool) {
	if !rf.isRunningLeader() {
		return -1, -1, false
	}

	// 更新日志
	rf.mu.Lock()
	curTerm := rf.currentTerm
	e := &LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, e)
	_ = rf.persistState()
	last := rf.lastIndex()
	rf.matchIndex[rf.me] = last
	rf.nextIndex[rf.me] = last + 1
	rf.mu.Unlock()

	// 发起通知，有新的日志到来
	for i := range rf.peers {
		if int32(i) == rf.me {
			continue
		}
		rf.conditions[i].Broadcast()
	}
	return last, curTerm, true
}

//  RequestVote RPC 服务端方法
func (rf *Raft) RequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
	// 模拟网络断开，收不到消息，最终超时
	if rf.networkDrop {
		time.Sleep(2 * RpcCallTimeout)
		return nil, errors.New("can not connect to the target server")
	}
	if rf.networkUnreliable {
		// 网络不可靠时先延时一会
		ms := rand.Int() % 27
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// 以一定概率丢弃部分消息（1/10）
		if rand.Int()%1000 < 100 {
			return nil, errors.New("can not connect to the target server")
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &RequestVoteReply{
		Term:        rf.currentTerm,
		VoteGranted: false,
	}

	// 1. 校验term
	if args.Term < rf.currentTerm {
		return reply, nil
	}
	// 保证reply的term不小于currentTerm，且转为follower
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.convertToFollower(args.Term, VoteNil)
	}
	// 现在term相同了

	// 检查是否已经投票给了其他候选人
	if rf.votedFor != VoteNil && rf.votedFor != args.CandidateId {
		return reply, nil
	}

	lastIndex, lastTerm := rf.lastIndex(), rf.lastTerm()
	// 检查term是否过期
	if lastTerm > args.LastLogTerm {
		return reply, nil
	}
	// 检查term和index是否匹配
	if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		return reply, nil
	}
	// 现在最后的term和index都匹配了，投票给该候选人，转为follower，并重置定时器

	reply.VoteGranted = true
	rf.convertToFollower(args.Term, args.CandidateId)
	rf.resetElectTimer()

	return reply, nil
}

// AppendEntries RPC 服务端方法
func (rf *Raft) AppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	// 模拟网络断开，收不到消息，最终超时
	if rf.networkDrop {
		time.Sleep(2 * RpcCallTimeout)
		return nil, errors.New("can not connect to the target server")
	}
	if rf.networkUnreliable {
		// 网络不可靠时先延时一会
		ms := rand.Int() % 27
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// 以一定概率丢弃部分消息（1/10）
		if rand.Int()%1000 < 100 {
			return nil, errors.New("can not connect to the target server")
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &AppendEntriesReply{
		Term:          rf.currentTerm,
		Success:       false,
		ConflictTerm:  0,
		ConflictIndex: 0,
	}

	// 检查term
	if args.Term < rf.currentTerm {
		return reply, nil // leader expired
	}
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.convertToFollower(args.Term, VoteNil)
	}
	// term一致，重置定时器
	rf.resetElectTimer()

	// 不包含args中的日志，返回冲突index
	last := rf.lastIndex()
	if last < args.PrevLogIndex {
		reply.ConflictTerm = NilTerm
		reply.ConflictIndex = last + 1
		return reply, nil
	}

	// index和term一致性检查
	prevIdx := args.PrevLogIndex
	prevTerm := int64(0)
	if prevIdx >= rf.lastIncludedIndex {
		prevTerm = rf.getLog(prevIdx).Term
	}
	// 如果term不匹配
	if prevTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevTerm
		for i, e := range rf.logs {
			if e.Term == prevTerm {
				reply.ConflictIndex = int64(i) + rf.lastIncludedIndex
				break
			}
		}
		return reply, nil
	}
	// 想再拥有相同的index和term

	for i, e := range args.Entries {
		// 更新日志
		j := prevIdx + int64(1+i)
		if j <= last {
			// 如果有相同的日志可以跳过
			if rf.getLog(j).Term == e.Term {
				continue
			}
			// 如果出现了不匹配的日志，则将其后的删除后再替换为参数中的日志
			rf.logs = rf.logs[:rf.subIndex(j)]
		}
		// 追加新的日志
		rf.logs = append(rf.logs, args.Entries[i:]...)
		_ = rf.persistState()
		break
	}
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int64(math.Max(float64(args.LeaderCommit), float64(rf.lastIndex()))) // need to commit
		rf.apply()
	}
	rf.convertToFollower(args.Term, args.LeaderId)

	reply.Success = true
	return reply, nil
}

// handler of InstallSnapshot RPC
func (rf *Raft) InstallSnapshot(ctx context.Context, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	// 模拟网络断开，收不到消息，最终超时
	if rf.networkDrop {
		time.Sleep(2 * RpcCallTimeout)
		return nil, errors.New("can not connect to the target server")
	}
	if rf.networkUnreliable {
		// 网络不可靠时先延时一会
		ms := rand.Int() % 27
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// 以一定概率丢弃部分消息（1/10）
		if rand.Int()%1000 < 100 {
			return nil, errors.New("can not connect to the target server")
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &InstallSnapshotReply{
		Term: rf.currentTerm,
	}

	// 同样的操作，检查term，重置定时器
	if args.Term < rf.currentTerm {
		return reply, nil
	}
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.convertToFollower(args.Term, VoteNil)
	}
	rf.resetElectTimer()

	// 检查leader的LastIncludedIndex是否领先于自己
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return reply, nil
	}

	// 如果leader快照位置小于自己的当前日志长度，则进行截取，保留之后的
	if args.LastIncludedIndex < rf.lastIndex() {
		rf.logs = rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:]
	} else {
		// 当前日志长度小于leader快照位置，则直接丢弃全部日志，记得初始化0号日志
		rf.logs = []*LogEntry{}
		rf.logs = append(rf.logs, &LogEntry{
			Term:    args.LastIncludedTerm,
			Command: nil,
		})
	}

	// 更新index
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	_ = rf.persistStatesAndSnapshot(args.Data)
	rf.commitIndex = int64(math.Max(float64(rf.commitIndex), float64(args.LastIncludedIndex)))
	rf.lastApplied = int64(math.Max(float64(rf.lastApplied), float64(args.LastIncludedIndex)))

	// 发出通知
	rf.applyChan <- ApplyMsg{
		CommandValid: false, // false代表是snapshot
		CommandIndex: -1,
		Command:      args.Data,
	}
	return reply, nil
}

// 代理一下RPC接口，用于进行测试
func (rf *Raft) sendRPC(peer int, args interface{}, reply interface{}) bool {
	ctx := context.Background()
	// 用于测试，模拟网络断开、不可靠、长时延
	// 如果网络不可用，模拟超时并返回false，此时不可发送消息
	if rf.networkDrop {
		time.Sleep(2 * RpcCallTimeout)
		return false
	}
	if rf.networkUnreliable {
		// 网络不可靠时先延时一会
		ms := rand.Int() % 27
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// 以一定概率丢弃部分消息（1/10）
		if rand.Int()%1000 < 100 {
			return false
		}
	}
	switch args.(type) {
	case *RequestVoteArgs:
		// 发送RequestVote RPC请求，并处理返回结果
		args, reply := args.(*RequestVoteArgs), reply.(*RequestVoteReply)
		resp, err := rf.peers[peer].RequestVote(ctx, args)
		if err != nil {
			//log.Printf("[Raft] raft peer%v send RequestVote RPC error: %s\n", rf.me, err)
			return false
		}
		reply.VoteGranted = resp.VoteGranted
		reply.Term = resp.Term
		return true
	case *AppendEntriesArgs:
		// 发送AppendEntries RPC请求，并处理返回结果
		args, reply := args.(*AppendEntriesArgs), reply.(*AppendEntriesReply)
		resp, err := rf.peers[peer].AppendEntries(ctx, args)
		if err != nil {
			//log.Printf("[Raft] raft peer%v send AppendEntries RPC error: %s\n", rf.me, err)
			return false
		}
		reply.Term = resp.Term
		reply.Success = resp.Success
		reply.ConflictIndex = resp.ConflictIndex
		reply.ConflictTerm = resp.ConflictTerm
		return true
	case *InstallSnapshotArgs:
		// 发送InstallSnapshot RPC请求，并处理返回结果
		args, reply := args.(*InstallSnapshotArgs), reply.(*InstallSnapshotReply)
		resp, err := rf.peers[peer].InstallSnapshot(ctx, args)
		if err != nil {
			//log.Printf("[Raft] raft peer%v send InstallSnapshot RPC error: %s\n", rf.me, err)
			return false
		}
		reply.Term = resp.Term
		return true
	}
	return false
}

// make a nee raft peer
func NewRaftPeer(me int32, persist *Persist, applyChan chan ApplyMsg, clients []RaftClient) *Raft {
	rf := &Raft{
		mu:          sync.RWMutex{},
		me:          me,
		state:       Follower,
		applyChan:   applyChan,
		stopped:     false,
		conditions:  make([]*sync.Cond, len(clients)),
		persist:     persist,
		currentTerm: 0,
		votedFor:    VoteNil,
		logs:        make([]*LogEntry, 0),
		nextIndex:   make([]int64, len(clients)),
		matchIndex:  make([]int64, len(clients)),
		peers:       clients,
	}
	// 日志从1开始计数
	rf.logs = append(rf.logs, &LogEntry{
		Term:    0,
		Command: nil,
	})
	// initialize conditions
	for i := 0; i < len(clients); i++ {
		rf.conditions[i] = sync.NewCond(&rf.mu)
	}
	// initialize election timer
	timout := randElectTime()
	rf.timer = &electionTimer{
		d: timout,
		t: time.NewTimer(timout),
	}
	// read persisted state
	_ = rf.readPersistedState()
	// snapshot's index
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// nextIndex initialized to last log index + 1
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastIndex() + 1
	}
	log.Printf("[Raft] raft peer%v start successfully!\n", me)

	// launch a goroutine to run the raft peer
	go rf.runPeer()

	return rf
}

// 该raft Peer的主循环，由选举超时驱动
func (rf *Raft) runPeer() {
	for {
		select {
		case <-rf.timer.t.C: // election timeout
			rf.run()
		}
	}
}

func (rf *Raft) run() {
	log.Printf("[Raft] raft peer%v become candidate, start request vote!\n", rf.me)
	rf.mu.Lock()
	// 开始新的term进行拉选票
	rf.currentTerm++
	// 改为Candidate状态
	rf.state = Candidate
	// 先将票投给自己
	rf.votedFor = rf.me
	// 重置定时器
	rf.resetElectTimer()
	_ = rf.persistState()
	// 构造RequestVote请求
	index := rf.lastIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: index,                 //自己日志的最新index
		LastLogTerm:  rf.getLog(index).Term, //最新日志的Term
	}
	rf.mu.Unlock()
	// 用于接收来自每一个Peer对该RPC请求的回应
	replyChan := make(chan RequestVoteReply, len(rf.peers))
	var wg sync.WaitGroup
	// 开使发起选举
	for i := range rf.peers {
		// 选举跳过自己，因为已经投票给自己了
		if int32(i) == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			var reply RequestVoteReply
			// 用于监听RPC请求完成
			respChan := make(chan struct{})
			// 启动一个goroutine发送RequestVote RPC
			go func() {
				// 请求成功时会返回true，出现error会返回false
				//if rf.sendRequestVote(peer, &args, &reply) {
				if rf.sendRPC(peer, &args, &reply) {
					// 请求成功发出通知
					respChan <- struct{}{}
				}
			}()
			// 等待RPC的结果，如果超时了就会返回
			select {
			case <-time.After(RpcCallTimeout):
				return
			case <-respChan:
				// 通知reply
				replyChan <- reply
			}
		}(i)
	}
	// 等待所有RPC完成
	go func() {
		wg.Wait()
		// 关闭channel停止接收数据
		close(replyChan)
	}()
	// 统计replyChan中的reply，收集选票，如果得票超过半数则成为leader，votes初始化为1是因为自己投给自己的一票
	votes, majority := int32(1), int32(len(rf.peers)/2+1)
	for rep := range replyChan { // 当上面goroutine完成后会关闭channel，从而退出循环
		rf.mu.Lock()
		// 如果对方term更大则自己应转为follower
		if rep.Term > rf.currentTerm {
			rf.convertToFollower(rep.Term, VoteNil)
			rf.mu.Unlock()
			return
		}
		// 检查是否仍处于Candidate状态，或者当前 term是否已经更新
		if rf.state != Candidate || rep.Term < rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		// 收集选票，注意原子性
		if rep.VoteGranted {
			atomic.AddInt32(&votes, 1)
		}
		// 如果选票超过半数，转为leader，不用再等待其他选票了
		if atomic.LoadInt32(&votes) >= majority {
			rf.state = Leader
			rf.mu.Unlock()
			log.Printf("[Raft] raft peer%v become Leader successfully！\n", rf.me)
			// 成为leader后开始给其他Peer追加日志并发出心跳，结束该选举goroutine
			go rf.startAppendEntries()
			go rf.startHeartbeat()
			return
		}
		rf.mu.Unlock()
	}
	// 如果选举失败，转为follower，等待下一次超时
	rf.mu.Lock()
	rf.convertToFollower(StayTerm, VoteNil)
	rf.mu.Unlock()
}

// start to send heartbeat when a candidate become leader
func (rf *Raft) startHeartbeat() {
	//心跳定时器
	ch := time.Tick(HeartbeatTerm)
	for {
		// 确保只有leader才有资格发心跳，否则退出
		if !rf.isRunningLeader() {
			return
		}
		// 向其他Peer发送心跳
		for i := range rf.peers {
			if int32(i) == rf.me {
				// 注意发出心跳的同时应重置定时器，否则会导致新一轮的选举
				rf.mu.Lock()
				rf.resetElectTimer()
				rf.mu.Unlock()
				continue
			}
			// 通知其他Peer 心跳到达
			rf.conditions[i].Broadcast()
		}
		// 等待下一次心跳时间到来
		<-ch
	}
}

// 开始向follower追加日志
func (rf *Raft) startAppendEntries() {
	for i := range rf.peers {
		// 跳过自己
		if int32(i) == rf.me {
			continue
		}
		// 开启新的goroutine来发送AppendEntries RPC
		go func(peer int) {
			for {
				// 只有leader才有资格发送该RPC
				if !rf.isRunningLeader() {
					return
				}
				rf.mu.Lock()
				// 等待心跳或新的日志到来，心跳和日志共用该RPC
				rf.conditions[peer].Wait()

				// 同步到最新的日志，如果是心跳的话就没有日志可发了
				next := rf.nextIndex[peer]

				// 如果下一个需发送的log落后于上一次快照的位置，说明落后太多，可能断开重连了，此时直接发送快照，加快同步
				if next <= rf.lastIncludedIndex {
					log.Printf("next: %v, lastIncludedIndex: %v", next, rf.lastIncludedIndex)
					rf.startInstallSnapshot(peer)
					continue
				}
				// 构造AppendEntries RPC 参数
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				// 检查是否有日志未发送
				if next < rf.logLength() {
					// 上次同步的日志的位置
					args.PrevLogIndex = next - 1
					args.PrevLogTerm = rf.getLog(next - 1).Term
					// 加入需同步的日志
					args.Entries = append(args.Entries, rf.logs[rf.subIndex(next):]...)
				}
				rf.mu.Unlock()

				var reply AppendEntriesReply
				respCh := make(chan struct{})
				// 开启一个新的goroutine发送RPC
				go func() {
					//if rf.sendAppendEntries(peer, &args, &reply) {
					if rf.sendRPC(peer, &args, &reply) {
						respCh <- struct{}{}
					}
				}()
				// 等待RPC发送结果
				select {
				case <-time.After(RpcCallTimeout):
					// 超时会返回重新发送
					continue
				case <-respCh:
					close(respCh)
				}

				// 如果日志位置匹配失败，也就是出现了冲突
				if !reply.Success {
					rf.mu.Lock()
					// 检查term
					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term, VoteNil)
						rf.mu.Unlock()
						return
					}
					//检查自己的状态
					if rf.state != Leader || reply.Term < rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					// 根据冲突的index，回退，发送最后一条匹配的日志位置后的全部日志
					if reply.ConflictIndex > 0 {
						rf.mu.Lock()
						firstConflict := reply.ConflictIndex
						if reply.ConflictTerm != NilTerm {
							lastIdx := rf.lastIndex()
							// 找到匹配的term中的最后一条日志，之后发送后面的全部term的日志
							for i := rf.addIndex(0); i <= lastIdx; i++ {
								if rf.getLog(i).Term != reply.ConflictTerm {
									continue
								}
								for i <= lastIdx && rf.getLog(i).Term == reply.ConflictTerm {
									i++
								}
								// 匹配的term中的最后一条日志
								firstConflict = i
								break
							}
						}
						// 更新下一次待发送的位置
						rf.nextIndex[peer] = firstConflict
						rf.mu.Unlock()
					}
					// 跳过，开始重新发送
					continue
				}

				// 如果追加成功，更新index
				if len(args.Entries) > 0 {
					rf.mu.Lock()
					// 更新index
					rf.matchIndex[peer] = args.PrevLogIndex + int64(len(args.Entries))
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					rf.updateCommitIndex()
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

// 开始发送 InstallSnapshot RPC
func (rf *Raft) startInstallSnapshot(peer int) {
	// 确保只有leader才可以发送snapshot
	if rf.state != Leader || rf.stopped {
		rf.mu.Unlock()
		return
	}

	// 读取快找，构造发送请求
	data, _ := rf.persist.ReadSnapshot()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              data,
	}
	rf.mu.Unlock()
	log.Printf("[Raft] raft peer%v start to send InstallSnapshot RPC\n", rf.me)

	// 发送RPC并等待返回结果
	var reply InstallSnapshotReply
	respCh := make(chan struct{})
	go func() {
		//if rf.sendInstallSnapshot(peer, &args, &reply){
		if rf.sendRPC(peer, &args, &reply) {
			respCh <- struct{}{}
		}
	}()
	select {
	case <-time.After(RpcCallTimeout):
		return
	case <-respCh:
		close(respCh)
	}

	// 处理返回结果
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 检查term
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term, VoteNil)
		return
	}
	// 检查自己的状态
	if rf.state != Leader || reply.Term < rf.currentTerm { // curTerm changed already
		return
	}

	// 更新index
	rf.matchIndex[peer] = args.LastIncludedIndex
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
}

// 上层应用会调用该方法主动触发快照
func (rf *Raft) TakeSnapshot(appliedId int64, rawSnapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查是否已经拍过快照
	if appliedId <= rf.lastIncludedIndex {
		return
	}

	// 丢掉之前的日志，拍摄快照
	rf.logs = rf.logs[rf.subIndex(appliedId):]
	rf.lastIncludedIndex = appliedId
	rf.lastIncludedTerm = rf.logs[0].Term
	_ = rf.persistStatesAndSnapshot(rawSnapshot)
}

// 当半数Peer追加了日志，则可以commit
func (rf *Raft) updateCommitIndex() {
	n := len(rf.peers)
	matched := make([]int64, n)
	for i := range matched {
		matched[i] = rf.matchIndex[i]
	}
	sort.Slice(matched, func(i, j int) bool {
		return matched[i] > matched[j]
	})
	// 半数以上已经append，则更新commitIndex
	if N := matched[n/2]; N > rf.commitIndex && rf.getLog(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.apply()
	}
}

// 更新lastApplied并发送通知给调用者
func (rf *Raft) apply() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      rf.getLog(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		//log.Printf("[Raft] raft peer%v successfully APPLY one command, index is: %v!\n", rf.me, rf.lastApplied)
	}
}

// 读取当前Peer的状态，返回当前term以及是否为leader
func (rf *Raft) GetState() (int64, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

// 关闭
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.stopped = true
	rf.mu.Unlock()
}

// 检查raft是否启动以及是否是leader
func (rf *Raft) isRunningLeader() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && !rf.stopped
}

// 转为follower状态
func (rf *Raft) convertToFollower(higherTerm int64, peer int32) {
	rf.state = Follower
	if higherTerm != StayTerm {
		rf.currentTerm = higherTerm
	}
	rf.votedFor = peer
	_ = rf.persistState()
}

// read raft peer's state from persisted file
func (rf *Raft) readPersistedState() error {
	var pb State
	// read state from persisted file
	data, err := rf.persist.ReadRaftState()
	if err != nil {
		return err
	}
	// proto unmarshal data
	if err = proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	// set raft peer's state
	rf.currentTerm = pb.CurrentTerm
	rf.votedFor = pb.VotedFor
	rf.lastIncludedIndex = pb.LastIncludedIndex
	rf.lastIncludedTerm = pb.LastIncludedTerm
	rf.logs = pb.Logs
	return nil
}

// persist raft peer's state
func (rf *Raft) persistState() error {
	pb := &State{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Logs:              rf.logs,
	}
	// serialize peer's state by proto
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	// persist data to file
	return rf.persist.SaveRaftState(data)
}

func (rf *Raft) persistStatesAndSnapshot(snapshot []byte) error {
	pb := &State{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Logs:              rf.logs,
	}
	// serialize peer's state by proto
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	return rf.persist.SaveStateAndSnapshot(data, snapshot)
}

// 重设选举定时器
func (rf *Raft) resetElectTimer() {
	rf.timer.d = randElectTime()
	rf.timer.t.Reset(rf.timer.d)
}

// index of logs may be changed for snapshot
func (rf *Raft) getLog(i int64) LogEntry {
	return *rf.logs[i-rf.lastIncludedIndex]
}

func (rf *Raft) addIndex(i int64) int64 {
	return rf.lastIncludedIndex + i
}

func (rf *Raft) subIndex(i int64) int64 {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) lastIndex() int64 {
	return rf.lastIncludedIndex + int64(len(rf.logs)) - 1
}

func (rf *Raft) lastTerm() int64 {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) logLength() int64 {
	return rf.lastIndex() + 1
}

// init rand seed
func init() {
	rand.Seed(time.Now().UnixNano())
}

// generate random election timeout
func randElectTime() time.Duration {
	extra := time.Duration(rand.Intn(ElectionTimeoutExtra)*5) * time.Millisecond
	return ElectionTimeoutBase + extra
}
