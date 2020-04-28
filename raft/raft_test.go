package raft

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"rache/utils"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 设置用于完成选举的时间为1秒
const RaftElectionTimeout = time.Second

// 测试选举
func TestInitialElection(t *testing.T) {
	addrList := []string{"127.0.0.1:12301", "127.0.0.1:12302", "127.0.0.1:12303"}
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(len(addrList))
	cfg.begin("Test: initial election")

	// 检查是否选举出了一个leader
	cfg.checkOneLeader()

	// 等待一会，检查所有的Peer的Term是否正常
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()

	// 检查在没有网络失效的情况下，是否term发生改变
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term hanged even though there were no failures")
	}
	// 检查是否仍然只有一个leader
	cfg.checkOneLeader()
	// 停止
	cfg.end()
}

// 测试网络断开后的选举和重新连接后的选举
func TestReElection(t *testing.T) {
	addrList := []string{"127.0.0.1:12304", "127.0.0.1:12305", "127.0.0.1:12306"}
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(len(addrList))
	cfg.begin("Test: election after network failure")

	// 网络断开前的leader
	leader1 := cfg.checkOneLeader()

	// 测试该leader断开连接后是否新的leader被选举出来
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	//原来的leader重新加入，，不应该影响当前的leader
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()
	fmt.Println(leader2)

	// 断开两个server后，不应该有leader被选举出来
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % len(addrList))
	time.Sleep(2 * RaftElectionTimeout)
	cfg.checkNoLeader()

	//如果重新加入了一个server，应该选举出一个leader
	cfg.connect((leader2 + 1) % len(addrList))
	cfg.checkOneLeader()

	// 将另一个server恢复，不会影响leader
	cfg.connect(leader2)
	cfg.checkOneLeader()

	cfg.end()
}

// 测试基本的日志是否能达成一致，追加到每一个Peer上
func TestBasicAgree(t *testing.T) {
	addrList := []string{"127.0.0.1:12307", "127.0.0.1:12308", "127.0.0.1:12309", "127.0.0.1:12310", "127.0.0.1:12311"}
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(len(addrList))
	cfg.begin("Test: basic agreement")
	iter := 3
	for index := 1; index < iter+1; index++ {
		n, _ := cfg.nCommitted(index)
		if n > 0 {
			t.Fatalf("some have committed before Start()")
		}
		rIndex := cfg.one(index*100, len(addrList), false)
		if int(rIndex) != index {
			t.Fatalf("got index %v but expected %v", rIndex, index)
		}
	}
	cfg.end()
}

// 测试follower断开连接后是否还能就日志追加达成一致
func TestFailAgree(t *testing.T) {
	addrList := []string{"127.0.0.1:12312", "127.0.0.1:12313", "127.0.0.1:12314"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	cfg.begin("Test: agreement despite follower disconnection")

	cfg.one(101, nums, false)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % nums)

	// follower断开后是否还能追加日志达成一致
	cfg.one(102, nums-1, false)
	cfg.one(103, nums-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(104, nums-1, false)
	cfg.one(105, nums-1, false)

	//重新连接
	cfg.connect((leader + 1) % nums)

	// 继续追加日志
	cfg.one(106, nums, true)
	time.Sleep(RaftElectionTimeout)
	cfg.one(107, nums, true)

	cfg.end()
}

// 测试fail掉大多数节点后无法追加日志达成一致
func TestFailNoAgree(t *testing.T) {
	addrList := []string{"127.0.0.1:12315", "127.0.0.1:12316", "127.0.0.1:12317", "127.0.0.1:12318", "127.0.0.1:12319"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	cfg.begin("Test: no agreement if too many followers disconnect")

	cfg.one(10, nums, false)

	// 断开5个Peer中的3个follower
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % nums)
	cfg.disconnect((leader + 2) % nums)
	cfg.disconnect((leader + 3) % nums)

	index, _, ok := cfg.rafts[leader].Submit([]byte(strconv.Itoa(20)))
	if !ok {
		t.Fatalf("leader rejected Submit()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := cfg.nCommitted(int(index))
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// 重连
	cfg.connect((leader + 1) % nums)
	cfg.connect((leader + 2) % nums)
	cfg.connect((leader + 3) % nums)

	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Submit([]byte(strconv.Itoa(30)))
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	cfg.one(1000, nums, true)

	cfg.end()
}

func TestRejoin(t *testing.T) {
	addrList := []string{"127.0.0.1:12320", "127.0.0.1:12321", "127.0.0.1:12322"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	cfg.begin("Test: rejoin of partitioned leader")

	cfg.one(101, nums, true)

	// 断开leader
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// 让该leader继续接受一些日志
	cfg.rafts[leader1].Submit([]byte(strconv.Itoa(102)))
	cfg.rafts[leader1].Submit([]byte(strconv.Itoa(103)))
	cfg.rafts[leader1].Submit([]byte(strconv.Itoa(104)))

	// 提交给新leader
	cfg.one(103, nums-1, true)
	// 让新leader再断开
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// 重连leader
	cfg.connect(leader1)
	cfg.one(104, nums-1, true)
	cfg.connect(leader2)
	cfg.one(105, nums, true)
	cfg.end()
}

func TestBackup(t *testing.T) {
	addrList := []string{"127.0.0.1:12323", "127.0.0.1:12324", "127.0.0.1:12325", "127.0.0.1:12326", "127.0.0.1:12327"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	cfg.begin("Test: leader backs up quickly over incorrect follower logs")

	cfg.one(rand.Int(), nums, true)

	// 让leader和一个follower在一个分区中
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % nums)
	cfg.disconnect((leader1 + 3) % nums)
	cfg.disconnect((leader1 + 4) % nums)

	// 向leader1提交一些日志，不会被commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Submit([]byte(strconv.Itoa(rand.Int())))
	}

	time.Sleep(RaftElectionTimeout / 2)

	// 断开这两个
	cfg.disconnect((leader1) % nums)
	cfg.disconnect((leader1 + 1) % nums)

	// 让其他三个follower恢复
	cfg.connect((leader1 + 2) % nums)
	cfg.connect((leader1 + 3) % nums)
	cfg.connect((leader1 + 4) % nums)

	// 提交一些新的日志给这三个
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// 恢复所有的
	for i := 0; i < nums; i++ {
		cfg.connect(i)
	}

	cfg.one(rand.Int(), nums, true)

	cfg.end()
}

// 测试并发情况下的工作
func TestConcurrentSubmit(t *testing.T) {
	addrList := []string{"127.0.0.1:12328", "127.0.0.1:12329", "127.0.0.1:12330"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	cfg.begin("Test: concurrent Submit()")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		_, term, ok := cfg.rafts[leader].Submit([]byte(strconv.Itoa(1)))
		if !ok {
			// 突然换了leader
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				index, term1, ok := cfg.rafts[leader].Submit([]byte(strconv.Itoa(100 + i)))
				if term1 != term {
					return
				}
				if !ok {
					return
				}
				is <- int(index)
			}(ii)
		}
		wg.Wait()
		close(is)

		for i := 0; i < nums; i++ {
			if t, _ := cfg.rafts[i].GetState(); t != term {
				continue loop
			}
		}

		failed := false
		var cmds []int
		for index := range is {
			cmd := cfg.wait(index, nums, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			go func() {
				for range is {

				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}
	if !success {
		t.Fatalf("term changed too often")
	}

	cfg.end()
}

// 测试持久化
func TestPersist1(t *testing.T) {
	addrList := []string{"127.0.0.1:12331", "127.0.0.1:12332", "127.0.0.1:12333"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	cfg.begin("Test: basic persistence")

	cfg.one(11, nums, true)

	// crash and re-start
	for i := 0; i < nums; i++ {
		cfg.start(i)
	}
	for i := 0; i < nums; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(12, nums, true)

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.start(leader1)
	cfg.connect(leader1)

	cfg.end()
}

// 测试状态持久化
func TestPersist2(t *testing.T) {
	addrList := []string{"127.0.0.1:12334", "127.0.0.1:12335", "127.0.0.1:12336", "127.0.0.1:12337", "127.0.0.1:12338"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	cfg.begin("Test: more persistence")

	index := 1
	for iter := 0; iter < 2; iter++ {
		cfg.one(10+index, nums, true)
		index++
		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % nums)
		cfg.disconnect((leader1 + 2) % nums)

		cfg.one(10+index, nums-2, true)
		index++

		cfg.disconnect((leader1 + 0) % nums)
		cfg.disconnect((leader1 + 3) % nums)
		cfg.disconnect((leader1 + 4) % nums)

		cfg.start((leader1 + 1) % nums)
		cfg.start((leader1 + 2) % nums)
		cfg.connect((leader1 + 1) % nums)
		cfg.connect((leader1 + 2) % nums)

		time.Sleep(RaftElectionTimeout)

		cfg.start((leader1 + 3) % nums)
		cfg.connect((leader1 + 3) % nums)

		cfg.one(10+index, nums-2, true)
		index++

		cfg.connect((leader1 + 4) % nums)
		cfg.connect((leader1 + 0) % nums)
	}
	cfg.one(1000, nums, true)

	cfg.end()
}

func TestPersist3(t *testing.T) {
	addrList := []string{"127.0.0.1:12331", "127.0.0.1:12332", "127.0.0.1:12333"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)

	cfg.begin("Test (2C): partitioned leader and one follower crash, leader restarts")

	cfg.one(101, 3, true)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 2) % nums)

	cfg.one(102, 2, true)

	cfg.crash((leader + 0) % nums)
	cfg.crash((leader + 1) % nums)
	cfg.connect((leader + 2) % nums)
	cfg.start((leader + 0) % nums)
	cfg.connect((leader + 0) % nums)

	cfg.one(103, 2, true)

	cfg.start((leader + 1) % nums)
	cfg.connect((leader + 1) % nums)

	cfg.one(104, nums, true)

	cfg.end()
}

// 测试Raft论文中出现的Figure8的场景
func TestFigure8(t *testing.T) {
	addrList := []string{"127.0.0.1:12334", "127.0.0.1:12335", "127.0.0.1:12336", "127.0.0.1:12337", "127.0.0.1:12338"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	cfg.begin("Test: Figure 8")

	cfg.one(rand.Int(), 1, true)

	nup := nums
	for iter := 0; iter < 1000; iter++ {
		leader1 := -1
		for i := 0; i < nums; i++ {
			if cfg.rafts[i] != nil {
				_, _, ok := cfg.rafts[i].Submit([]byte(strconv.Itoa(rand.Int())))
				if ok {
					leader1 = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63() % 13
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader1 != -1 {
			cfg.crash(leader1)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % nums
			if cfg.rafts[s] == nil {
				cfg.start(s)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < nums; i++ {
		if cfg.rafts[i] == nil {
			cfg.start(i)
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int(), nums, true)

	cfg.end()
}

// 测试不可靠网络下是否能就追加日志达成一致
func TestUnreliableAgree(t *testing.T) {
	addrList := []string{"127.0.0.1:12334", "127.0.0.1:12335", "127.0.0.1:12336", "127.0.0.1:12337", "127.0.0.1:12338"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	for i := 0; i < nums; i++ {
		cfg.unreliableConnect(i)
	}
	cfg.begin("Test: unreliable agreement")

	var wg sync.WaitGroup
	for iter := 1; iter < 50; iter++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iter, j int) {
				defer wg.Done()
				cfg.one((100*iter)+j, 1, true)
			}(iter, j)
		}
		cfg.one(iter, 1, true)
	}

	for i := 0; i < nums; i++ {
		cfg.reliableConnect(i)
	}

	wg.Wait()
	cfg.one(100, nums, true)
	cfg.end()
}

// 测试在不可靠网络下Figure8的场景
func TestFigure8Unreliable(t *testing.T) {
	addrList := []string{"127.0.0.1:12334", "127.0.0.1:12335", "127.0.0.1:12336", "127.0.0.1:12337", "127.0.0.1:12338"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	for i := 0; i < nums; i++ {
		cfg.unreliableConnect(i)
	}
	cfg.begin("Test: Figure 8 (Unreliable)")
	cfg.one(rand.Int(), 1, true)

	nup := nums
	for iter := 0; iter < 1000; iter++ {
		leader1 := -1
		for i := 0; i < nums; i++ {
			if cfg.rafts[i] != nil {
				_, _, ok := cfg.rafts[i].Submit([]byte(strconv.Itoa(rand.Int())))
				if ok {
					leader1 = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63() % 13
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader1 != -1 {
			cfg.crash(leader1)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % nums
			if cfg.rafts[s] == nil {
				cfg.start(s)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < nums; i++ {
		if cfg.rafts[i] == nil {
			cfg.start(i)
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int(), nums, true)

	cfg.end()
}

func TestInternalChurn(t *testing.T) {
	addrList := []string{"127.0.0.1:12334", "127.0.0.1:12335", "127.0.0.1:12336", "127.0.0.1:12337", "127.0.0.1:12338"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	cfg.begin("Test: churn")

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		var values []int
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < nums; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Submit([]byte(strconv.Itoa(x)))
					if ok1 {
						ok = ok1
						index = int(index1)
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := make([]chan []int, 0)
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % nums
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % nums
			if cfg.rafts[i] == nil {
				cfg.start(i)
			}
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % nums
			if cfg.rafts[i] != nil {
				cfg.crash(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	for i := 0; i < nums; i++ {
		if cfg.rafts[i] == nil {
			cfg.start(i)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(rand.Int(), nums, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= int(lastIndex); index++ {
		v := cfg.wait(index, nums, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	cfg.end()
}

func TestInternalChurnUnreliable(t *testing.T) {
	addrList := []string{"127.0.0.1:12334", "127.0.0.1:12335", "127.0.0.1:12336", "127.0.0.1:12337", "127.0.0.1:12338"}
	nums := len(addrList)
	cfg := makeConfig(t, addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	for i := 0; i < nums; i++ {
		cfg.unreliableConnect(i)
	}
	cfg.begin("Test: churn (Unreliable)")

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		var values []int
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < nums; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Submit([]byte(strconv.Itoa(x)))
					if ok1 {
						ok = ok1
						index = int(index1)
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := make([]chan []int, 0)
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % nums
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % nums
			if cfg.rafts[i] == nil {
				cfg.start(i)
			}
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % nums
			if cfg.rafts[i] != nil {
				cfg.crash(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	for i := 0; i < nums; i++ {
		cfg.reliableConnect(i)
	}
	for i := 0; i < nums; i++ {
		if cfg.rafts[i] == nil {
			cfg.start(i)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(rand.Int(), nums, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= int(lastIndex); index++ {
		v := cfg.wait(index, nums, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	cfg.end()
}

func cleanStateFile(n int) {
	for i := 0; i < n; i++ {
		_ = os.Remove("state" + strconv.Itoa(i))
		_ = os.Remove("state" + strconv.Itoa(i) + "_copy")
	}
}

// 用于测试
type config struct {
	mu         sync.Mutex
	t          *testing.T
	n          int
	rafts      []*Raft
	applyErr   []string
	connected  []bool
	addrList   []string
	saved      []*Persist
	logs       []map[int]int //server已经提交的日志的copy
	startTime  time.Time
	t0         time.Time // 初始时间
	cmds0      int       //初始日志数
	maxIndex   int       // 最大的entry的index
	maxIndex0  int       // 初始的最大entry的index
	longDelays bool
	clients    []RaftClient
	listens    []net.Listener
	servers    []*grpc.Server
}

var ncpu_once sync.Once

func makeConfig(t *testing.T, addrList []string) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(utils.MakeSeed())
	})
	// 设置可并发使用的最大CPU数目
	runtime.GOMAXPROCS(4)
	n := len(addrList)
	// 初始化config
	cfg := &config{
		mu:         sync.Mutex{},
		t:          t,
		n:          n,
		rafts:      make([]*Raft, n),
		applyErr:   make([]string, n),
		connected:  make([]bool, n),
		saved:      make([]*Persist, n),
		logs:       make([]map[int]int, n),
		startTime:  time.Now(),
		longDelays: true,
		addrList:   make([]string, n),
		listens:    make([]net.Listener, n),
		servers:    make([]*grpc.Server, n),
	}
	copy(cfg.addrList, addrList)
	clients := make([]RaftClient, len(cfg.addrList))
	for i := range cfg.addrList {
		conn, err := grpc.Dial(cfg.addrList[i], grpc.WithInsecure())
		if err != nil {
			log.Fatal("start conn error")
		}
		clients[i] = NewRaftClient(conn)
	}
	cfg.clients = clients

	// 创建raft servers
	for i := 0; i < n; i++ {
		cfg.logs[i] = make(map[int]int)
		cfg.start(i)
	}

	// 连接server
	for i := 0; i < n; i++ {
		cfg.connect(i)
	}

	return cfg
}

// 创建一个raft server
func (c *config) start(i int) {
	// 如果已经有了server，先关闭它
	c.crash(i)
	// 创建persist
	c.mu.Lock()
	if c.saved[i] != nil {
		persist, _ := c.saved[i].Copy(i)
		c.saved[i] = persist
	} else {
		c.saved[i] = NewPersist(i)
	}
	c.mu.Unlock()
	// 监听来自raft的消息，代表已经committed过的消息
	applyChan := make(chan ApplyMsg)
	go func() {
		for m := range applyChan {
			errMsg := ""
			if !m.CommandValid {
				// 忽略该类型的消息
			} else if v, err := strconv.Atoi(string(m.Command)); err == nil {
				c.mu.Lock()
				for j := 0; j < len(c.logs); j++ {
					// 如果日志内容对不上
					if old, oldOk := c.logs[j][int(m.CommandIndex)]; oldOk && old != v {
						errMsg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v", m.CommandIndex, i, v, j, old)
					}
				}
				_, prevOk := c.logs[i][int(m.CommandIndex)-1]
				c.logs[i][int(m.CommandIndex)] = v
				if int(m.CommandIndex) > c.maxIndex {
					c.maxIndex = int(m.CommandIndex)
				}
				c.mu.Unlock()

				// 如果顺序对不上
				if m.CommandIndex > 1 && !prevOk {
					errMsg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
				}
			} else {
				// 类型错误
				errMsg = fmt.Sprintf("committed command %v is not an int", m.Command)
			}

			if errMsg != "" {
				log.Fatalf("apply error: %v\n", errMsg)
				c.applyErr[i] = errMsg
			}
		}
	}()

	// 创建raft Peer
	rf := NewRaftPeer(int32(i), c.saved[i], applyChan, c.clients)
	c.mu.Lock()
	c.rafts[i] = rf
	c.mu.Unlock()

	// 注册grpc server
	// 注册gRPC server，创建raft
	addr := c.addrList[i]
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen err: %s\n", err)
	}
	c.listens[i] = listen
	log.Printf("listen %s successfully!", addr)
	s := grpc.NewServer()
	RegisterRaftServer(s, rf)
	c.servers[i] = s
	go func() {
		if err = s.Serve(listen); err != nil {
			log.Fatal("serve error")
		}
	}()
}

// 关闭一个raft server，并保存它的状态
func (c *config) crash(i int) {
	// 终止连接
	c.disconnect(i)
	if c.servers[i] != nil {
		c.servers[i].Stop()
		c.listens[i].Close()
	}

	//复制一个persist，不影响原来的persist
	if c.saved[i] != nil {
		cc, _ := c.saved[i].Copy(i)
		c.mu.Lock()
		c.saved[i] = cc
		c.mu.Unlock()
	}
	// 终止raft
	rf := c.rafts[i]
	if rf != nil {
		rf.Kill()
		c.mu.Lock()
		c.rafts[i] = nil
		c.mu.Unlock()
	}
	//保存原来的state
	if c.saved[i] != nil {
		raftLog, _ := c.saved[i].ReadSnapshot()
		c.mu.Lock()
		c.saved[i] = NewPersist(i)
		c.mu.Unlock()
		c.saved[i].SaveRaftState(raftLog)
	}
}

// 建立连接
func (c *config) connect(i int) {
	c.connected[i] = true
	// 建立向内和向外的连接
	c.rafts[i].networkDrop = false
}

// 断开连接
func (c *config) disconnect(i int) {
	c.connected[i] = false
	// 断开向内和向外的连接，true表示网络不可用
	if c.rafts[i] != nil {
		c.rafts[i].networkDrop = true
	}
}

// 启用不可靠网络
func (c *config) unreliableConnect(i int) {
	if c.rafts[i] != nil {
		c.rafts[i].networkUnreliable = true
	}
}

func (c *config) reliableConnect(i int) {
	if c.rafts[i] != nil {
		c.rafts[i].networkUnreliable = false
	}
}

// 执行清理工作
func (c *config) cleanup() {
	// 终止每一个raft server
	for i := 0; i < len(c.rafts); i++ {
		if c.rafts[i] != nil {
			c.rafts[i].Kill()
		}
	}
	// 检查是否超时
	c.checkTimeout()
}

// 检查是否超时，限制超时时间为2分钟
func (c *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !c.t.Failed() && time.Since(c.startTime) > 120*time.Second {
		c.t.Fatal("test took longer than 120 seconds")
	}
}

// 检查是否只有一个leader，re-election的情况下重试多次
func (c *config) checkOneLeader() int {
	for iter := 0; iter < 10; iter++ {
		ms := 450 + rand.Int63()%100
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// 遍历每一个server，记录其term和是否为leader
		leaders := make(map[int64][]int)
		for i := 0; i < c.n; i++ {
			if c.connected[i] {
				if term, leader := c.rafts[i].GetState(); leader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		lastTermWithLeader := int64(-1)
		for term, leaderList := range leaders {
			// 如果某个term出现了不止一个leader，报错
			if len(leaderList) > 1 {
				c.t.Fatalf("term %d has %d (>1) leaders", term, len(leaderList))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		// 返回最后一个term的leader
		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	// 如果没有选出leader，报错
	c.t.Fatalf("expected one leader, get none")
	return -1
}

// 检查是否没有leader
func (c *config) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			_, isLeader := c.rafts[i].GetState()
			if isLeader {
				c.t.Fatalf("expected no leader, but %v claims to be leader", i)
			}
		}
	}
}

// 检查是否每一个raft节点都agree on相同的term
func (c *config) checkTerms() int64 {
	term := int64(-1)
	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			xterm, _ := c.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				c.t.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}

//检查多少个server认为一个日志committed
func (c *config) nCommitted(index int) (int, interface{}) {
	count := 0
	cmd := -1
	for i := 0; i < len(c.rafts); i++ {
		if c.applyErr[i] != "" {
			c.t.Fatal(c.applyErr[i])
		}
		// 查看对应的Peer的日志，如果存在指定index，则计数++
		c.mu.Lock()
		cmd1, ok := c.logs[i][index]
		c.mu.Unlock()
		if ok {
			// 如果日志内容不一致，报错
			if count > 0 && cmd != cmd1 {
				c.t.Fatalf("committed value do not match: index %v, %v, %v\n", index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

// 等待至少n个server commit，但不会一直等待
func (c *config) wait(index int, n int, startTerm int64) interface{} {
	to := 10 * time.Millisecond
	for iter := 0; iter < 30; iter++ {
		// 检查已经commit的server的数目
		nd, _ := c.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		// 每次倍增休眠时间
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, r := range c.rafts {
				// 检查是否到了下一个term
				if t, _ := r.GetState(); t > startTerm {
					return -1
				}
			}
		}
	}
	// 返回对应的cmd
	nd, cmd := c.nCommitted(index)
	if nd < n {
		c.t.Fatalf("only %d decided for index %d; wanted %d\n", nd, index, n)
	}
	return cmd
}

// 执行一次完整的agreement。一开始可能会选错leader，提交失败后会进行重试，失败超过10s会结束
//
func (c *config) one(cmd int, expectedServers int, retry bool) int64 {
	t0 := time.Now()
	starts := 0
	// 执行时间超所10秒就返回
	for time.Since(t0).Seconds() < 10 {
		// 尝试所有的server，可能其中之一是leader
		index := int64(-1)
		for si := 0; si < c.n; si++ {
			starts = (starts + 1) % c.n
			var rf *Raft
			c.mu.Lock()
			if c.connected[starts] {
				rf = c.rafts[starts]
			}
			c.mu.Unlock()
			if rf != nil {
				// 如果是leader且提交成功了
				index1, _, ok := rf.Submit([]byte(strconv.Itoa(cmd)))
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// 如果已经有leader并提交了该cmd，等待agreement
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				// 寻找提交记录
				nd, cmd1 := c.nCommitted(int(index))
				if nd > 0 && nd >= expectedServers {
					if cmd2, ok := cmd1.(int); ok && cmd2 == cmd {
						// 是我们提交的command
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			// 是否重试
			if retry == false {
				log.Println(index)
				c.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}

//开始一个test
func (c *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	c.t0 = time.Now()
	c.cmds0 = 0
	c.maxIndex0 = c.maxIndex
}

//  end a test -- 能够到达这里说明通过了测试，打印相关信息
func (c *config) end() {
	c.checkTimeout()
	if c.t.Failed() == false {
		c.mu.Lock()
		t := time.Since(c.t0).Seconds()    // 测试所用的时间
		peerNum := c.n                     // peer数
		cmdNum := c.maxIndex - c.maxIndex0 // 执行的cmd 日志数目
		c.mu.Unlock()

		fmt.Printf("--- Passed ---")
		fmt.Printf("  %4.1f  %d %4d\n", t, peerNum, cmdNum)
	}
}

func TestEncrypt(t *testing.T)  {
	input := "Something to encrypt"
	m := md5.New()
	io.WriteString(m, input+"42b91e")
	inputMd5 := hex.EncodeToString(m.Sum(nil))
	h := sha1.New()
	io.WriteString(h, "08a441")
	io.WriteString(h, inputMd5)
	fmt.Println(hex.EncodeToString(h.Sum(nil)))
}