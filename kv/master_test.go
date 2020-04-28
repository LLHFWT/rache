package kv

import (
	"fmt"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"os"
	"rache/raft"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// 测试基本功能
func TestBasicJoinAndLeave(t *testing.T) {
	addrList := []string{"127.0.0.1:12301", "127.0.0.1:12302", "127.0.0.1:12303"}
	cfg := makeMasterConfig(t, addrList)
	nums := len(addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	mc := cfg.makeMClient()

	fmt.Println("Test: Basic leave/join...")
	confList := make([]*Config, 6)
	// 检查初始状态
	confList[0] = mc.Query(-1)
	check(t, []int32{}, mc)

	// 检查加入第一个group
	var gid1 int32 = 1
	mc.Join(map[int32]*Servers{gid1: {
		ServerList: []string{"x", "y", "z"},
	}})
	check(t, []int32{gid1}, mc)
	confList[1] = mc.Query(-1)

	//检查加入第二个group
	var gid2 int32 = 2
	mc.Join(map[int32]*Servers{gid2: {
		ServerList: []string{"a", "b", "c"},
	}})
	check(t, []int32{gid1, gid2}, mc)
	confList[2] = mc.Query(-1)
	confList[3] = &Config{
		Num:    0,
		Shards: make([]int32, NShards),
	}

	// 检查group内容是否正确
	cfx := mc.Query(-1)
	sa1 := cfx.Groups[gid1].ServerList
	if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
	}
	sa2 := cfx.Groups[gid2].ServerList
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}

	// 移除group1
	mc.Leave([]int32{gid1})
	check(t, []int32{gid2}, mc)
	confList[4] = mc.Query(-1)

	// 移除group2
	mc.Leave([]int32{gid2})
	check(t, []int32{}, mc)
	confList[5] = mc.Query(-1)
	fmt.Printf("  ... Passed\n")
	time.Sleep(time.Second)

	fmt.Println("Test: Historical queries...")
	for s := 0; s < nums; s++ {
		for i := 0; i < len(confList); i++ {
			c := mc.Query(confList[i].Num)
			checkSameConfig(t, c, confList[i])
		}
	}
	fmt.Println(" ... Passed")
}

func TestBasicMove(t *testing.T) {
	addrList := []string{"127.0.0.1:12301", "127.0.0.1:12302", "127.0.0.1:12303"}
	cfg := makeMasterConfig(t, addrList)
	nums := len(addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	mc := cfg.makeMClient()

	fmt.Println("Test: Move...")
	var gid3 int32 = 503
	mc.Join(map[int32]*Servers{gid3: {
		ServerList: []string{"3a", "3b", "3c"},
	}})
	var gid4 int32 = 504
	mc.Join(map[int32]*Servers{gid4: {
		ServerList: []string{"4a", "4b", "4c"},
	}})

	for i := 0; i < NShards; i++ {
		cf := mc.Query(-1)
		// 将前一半shards移动给gid3,后一半移动给gid4
		if i < NShards/2 {
			mc.Move(int32(i), gid3)
			if cf.Shards[i] != gid3 {
				// 检查是否更新了Num
				cf1 := mc.Query(-1)
				if cf1.Num <= cf.Num {
					t.Fatalf("Move should increase Config.Num")
				}
			}
		} else {
			mc.Move(int32(i), gid4)
			if cf.Shards[i] != gid4 {
				cf1 := mc.Query(-1)
				if cf1.Num <= cf.Num {
					t.Fatalf("Move should increase Config.Num")
				}
			}
		}
	}
	//检查移动结果，是否是前一半分配给gid3，后一半分配给gid4
	cf2 := mc.Query(-1)
	for i := 0; i < NShards; i++ {
		if i < NShards/2 {
			if cf2.Shards[i] != gid3 {
				t.Fatalf("expected shard %v on gid %v actually %v", i, gid3, cf2.Shards[i])
			}
		} else {
			if cf2.Shards[i] != gid4 {
				t.Fatalf("expected shard %v on gid %v actually %v", i, gid4, cf2.Shards[i])
			}
		}
	}
	mc.Leave([]int32{gid3})
	mc.Leave([]int32{gid4})

	fmt.Println(" ... Passed ")
}

func TestConcurrentLeaveAndJoin(t *testing.T) {
	addrList := []string{"127.0.0.1:12301", "127.0.0.1:12302", "127.0.0.1:12303"}
	cfg := makeMasterConfig(t, addrList)
	nums := len(addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	mc := cfg.makeMClient()

	fmt.Println("Test: Concurrent leave and join...")
	// 创建10个客户端
	const clientNums = 10
	var cka [clientNums]*MClient
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeMClient()
	}
	gids := make([]int32, clientNums)
	ch := make(chan bool)
	for xi := 0; xi < clientNums; xi++ {
		gids[xi] = int32(xi*10 + 100)
		go func(i int) {
			defer func() { ch <- true }()
			var gid int32 = gids[i]
			sid1, sid2 := fmt.Sprintf("s%da", gid), fmt.Sprintf("s%db", gid)
			cka[i].Join(map[int32]*Servers{gid + 1000: {
				ServerList: []string{sid1},
			}})
			cka[i].Join(map[int32]*Servers{gid: {
				ServerList: []string{sid2},
			}})
			cka[i].Leave([]int32{gid + 1000})
		}(xi)
	}
	for i := 0; i < clientNums; i++ {
		<-ch
	}
	check(t, gids, mc)
}

func TestMinimalTransfers(t *testing.T) {
	addrList := []string{"127.0.0.1:12301", "127.0.0.1:12302", "127.0.0.1:12303"}
	cfg := makeMasterConfig(t, addrList)
	nums := len(addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	mc := cfg.makeMClient()

	fmt.Println("Test: Minimal transfers after joins ...")
	const npara = 10
	c1 := mc.Query(-1)
	for i := 0; i < 5; i++ {
		gid := int32(npara + 1 + i)
		mc.Join(map[int32]*Servers{gid: {
			ServerList: []string{fmt.Sprintf("%da", gid), fmt.Sprintf("%db", gid), fmt.Sprintf("%db", gid)},
		}})
	}
	c2 := mc.Query(-1)
	for i := 1; i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == int32(i) {
				if c1.Shards[j] != int32(i) {
					t.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}
	fmt.Println("... Passed")

	fmt.Printf("Test: Minimal transfers after leaves ...\n")
	for i := 0; i < 5; i++ {
		mc.Leave([]int32{int32(npara + 1 + i)})
	}
	c3 := mc.Query(-1)
	for i := 1; i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == int32(i) {
				if c3.Shards[j] != int32(i) {
					t.Fatalf("non-minimal transfer after Leave()s")
				}
			}
		}
	}
	fmt.Println("... Passed")
}

func TestMultiGroupJoinAndLeave(t *testing.T) {
	addrList := []string{"127.0.0.1:12301", "127.0.0.1:12302", "127.0.0.1:12303"}
	cfg := makeMasterConfig(t, addrList)
	nums := len(addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	mc := cfg.makeMClient()

	fmt.Printf("Test: Multi-group join/leave ...\n")
	cfa := make([]*Config, 6)
	cfa[0] = mc.Query(-1)
	check(t, []int32{}, mc)

	gid1, gid2 := int32(1), int32(2)
	mc.Join(map[int32]*Servers{
		gid1: {ServerList: []string{"x", "y", "z"}},
		gid2: {ServerList: []string{"a", "b", "c"}},
	})
	check(t, []int32{gid1, gid2}, mc)
	cfa[1] = mc.Query(-1)

	gid3 := int32(3)
	mc.Join(map[int32]*Servers{
		gid3: {ServerList: []string{"j", "k", "l"}},
	})
	check(t, []int32{gid1, gid2, gid3}, mc)
	cfa[2] = mc.Query(-1)

	cfx := mc.Query(-1)
	sa1 := cfx.Groups[gid1].ServerList
	if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
	}
	sa2 := cfx.Groups[gid2].ServerList
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}
	sa3 := cfx.Groups[gid3].ServerList
	if len(sa3) != 3 || sa3[0] != "j" || sa3[1] != "k" || sa3[2] != "l" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid3, sa3)
	}

	mc.Leave([]int32{gid1, gid3})
	check(t, []int32{gid2}, mc)
	cfa[3] = mc.Query(-1)

	cfx = mc.Query(-1)
	sa2 = cfx.Groups[gid2].ServerList
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}
	mc.Leave([]int32{gid2})

	fmt.Printf("  ... Passed\n")
}

func TestConcurrentMultiLeaveAndJoin(t *testing.T) {
	addrList := []string{"127.0.0.1:12301", "127.0.0.1:12302", "127.0.0.1:12303"}
	cfg := makeMasterConfig(t, addrList)
	nums := len(addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	mc := cfg.makeMClient()

	fmt.Printf("Test: Concurrent multi leave/join ...\n")
	const clientNums = 10
	var cka [clientNums]*MClient
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeMClient()
	}
	gids := make([]int32, clientNums)
	var wg sync.WaitGroup
	for i := 0; i < clientNums; i++ {
		wg.Add(1)
		gids[i] = int32(i + 1000)
		go func(i int) {
			defer wg.Done()
			gid := gids[i]
			cka[i].Join(map[int32]*Servers{
				gid:        {ServerList: []string{fmt.Sprintf("%da", gid), fmt.Sprintf("%db", gid), fmt.Sprintf("%dc", gid)}},
				gid + 1000: {ServerList: []string{fmt.Sprintf("%da", gid+1000)}},
				gid + 2000: {ServerList: []string{fmt.Sprintf("%da", gid+2000)}},
			})
			cka[i].Leave([]int32{gid + 1000, gid + 2000})
		}(i)
	}
	wg.Wait()
	check(t, gids, mc)
	fmt.Println("....Passed. ")
}

func TestMultiMinimalTransfers(t *testing.T) {
	addrList := []string{"127.0.0.1:12301", "127.0.0.1:12302", "127.0.0.1:12303"}
	cfg := makeMasterConfig(t, addrList)
	nums := len(addrList)
	defer cfg.cleanup()
	defer cleanStateFile(nums)
	mc := cfg.makeMClient()

	fmt.Printf("Test: Minimal transfers after multijoins ...\n")
	const n = 10
	c1 := mc.Query(-1)
	m := make(map[int32]*Servers)
	for i := 0; i < 5; i++ {
		gid := int32(n + 1 + i)
		m[gid] = &Servers{ServerList: []string{fmt.Sprintf("%da", gid), fmt.Sprintf("%db", gid)}}
	}
	mc.Join(m)
	c2 := mc.Query(-1)
	for i := 1; i <= n; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == int32(i) {
				if c1.Shards[j] != int32(i) {
					t.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}
	fmt.Println("....Passed ")

	fmt.Printf("Test: Minimal transfers after multileaves ...\n")
	var l []int32
	for i := 0; i < 5; i++ {
		l = append(l, int32(n+1+i))
	}
	mc.Leave(l)
	c3 := mc.Query(-1)
	for i := 1; i <= n; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == int32(i) {
				if c3.Shards[j] == int32(i) {
					t.Fatalf("non-minimal transfer after leave()s")
				}
			}
		}
	}
	fmt.Println("...Passed ")
}

// 检查给定的group和最新的config是否匹配
func check(t *testing.T, groups []int32, c *MClient) {
	conf := c.Query(-1)
	if len(conf.Groups) != len(groups) {
		t.Fatalf("wanted %v groups, got %v", len(groups), len(conf.Groups))
	}

	// 检查groups是否一致
	for _, g := range groups {
		if _, ok := conf.Groups[g]; !ok {
			t.Fatalf("missing group %v", g)
		}
	}

	// 检查是否有未分配的shards,分片按顺序标记的
	if len(groups) > 0 {
		for s, g := range conf.Shards {
			if _, ok := conf.Groups[g]; !ok {
				t.Fatalf("shard %v -> invalid group %v", s, g)
			}
		}
	}

	// 检查是否分片均衡
	counts := make(map[int32]int)
	for _, g := range conf.Shards {
		counts[g] += 1
	}
	min, max := 257, 0
	for g, _ := range conf.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		t.Fatalf("max %v too much larger than min %v", max, min)
	}
}

// 检查给定的两个config是否一致
func checkSameConfig(t *testing.T, c1, c2 *Config) {
	// 检查Num是否一致
	if c1.Num != c2.Num {
		t.Fatalf("Num wrong")
	}
	//检查Shards是否一致
	if len(c1.Shards) != len(c2.Shards) {
		t.Fatalf("number of Shards is wrong")
	}
	for i := 0; i < len(c1.Shards); i++ {
		if c1.Shards[i] != c2.Shards[i] {
			t.Fatalf("Shards wrong")
		}
	}
	// 检查groups是否一致
	if len(c1.Groups) != len(c2.Groups) {
		t.Fatalf("number of groups is wrong")
	}
	for g, s := range c1.Groups {
		s1, ok := c2.Groups[g]
		if !ok || len(s.ServerList) != len(s1.ServerList) {
			t.Fatalf("len(Groups) wrong")
		}
		for j := 0; j < len(s.ServerList); j++ {
			if s.ServerList[j] != s1.ServerList[j] {
				t.Fatalf("Groups wrong")
			}
		}
	}
}

func cleanStateFile(n int) {
	for i := 0; i < n; i++ {
		_ = os.Remove("state" + strconv.Itoa(i))
		_ = os.Remove("state" + strconv.Itoa(i) + "_copy")
	}
}

type masterConfig struct {
	mu           sync.Mutex
	t            *testing.T
	n            int
	addrList     []string
	servers      []*Master
	saved        []*raft.Persist
	nextClientId int
	start        time.Time // 开始时间
	raftClients  []raft.RaftClient
	clients      map[*MClient]bool
}

// 创建config
func makeMasterConfig(t *testing.T, addrList []string) *masterConfig {
	runtime.GOMAXPROCS(4)
	n := len(addrList)
	cfg := &masterConfig{
		mu:           sync.Mutex{},
		t:            t,
		n:            n,
		addrList:     make([]string, n),
		servers:      make([]*Master, n),
		saved:        make([]*raft.Persist, n),
		nextClientId: n + 1000,
		start:        time.Now(),
		raftClients:  make([]raft.RaftClient, n),
		clients:      make(map[*MClient]bool),
	}
	// 创建raft master的rpc客户端用于创建raft server和master client
	copy(cfg.addrList, addrList)
	raftClients := make([]raft.RaftClient, n)
	for i := range cfg.addrList {
		conn, err := grpc.Dial(cfg.addrList[i], grpc.WithInsecure())
		if err != nil {
			log.Fatal("start conn error")
		}
		raftClients[i] = raft.NewRaftClient(conn)
	}
	cfg.raftClients = raftClients

	// 启动server
	for i := 0; i < cfg.n; i++ {
		cfg.startMasterServer(i)
	}

	return cfg
}

// 启动server
func (cfg *masterConfig) startMasterServer(i int) {
	cfg.mu.Lock()
	if cfg.saved[i] != nil {
		persist, _ := cfg.saved[i].Copy(i)
		cfg.saved[i] = persist
	} else {
		cfg.saved[i] = raft.NewPersist(i)
	}
	cfg.mu.Unlock()

	// 创建master server
	cfg.servers[i] = StartServer(int32(i), cfg.saved[i], cfg.raftClients)

	// 注册gRPC server，创建raft
	addr := cfg.addrList[i]
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen err: %s\n", err)
	}
	log.Printf("listen %s successfully!", addr)
	s := grpc.NewServer()
	raft.RegisterRaftServer(s, cfg.servers[i].rf)
	RegisterMasterServer(s, cfg.servers[i])
	go func() {
		if err = s.Serve(listen); err != nil {
			log.Fatal("serve error")
		}
	}()
}

// 关闭server
func (cfg *masterConfig) shutdownMasterServer(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	if cfg.saved[i] != nil {
		cfg.saved[i], _ = cfg.saved[i].Copy(i)
	}

	s := cfg.servers[i]
	if s != nil {
		cfg.mu.Unlock()
		s.Kill()
		cfg.mu.Lock()
		cfg.servers[i] = nil
	}
}

// 创建master的客户端，只建立对to中server的连接
func (cfg *masterConfig) makeMClient() *MClient {
	// 随机化打乱客户端的顺序
	masterClients := make([]MasterClient, cfg.n)
	for i := range cfg.addrList {
		conn, err := grpc.Dial(cfg.addrList[i], grpc.WithInsecure())
		if err != nil {
			log.Fatal("start conn error")
		}
		masterClients[i] = NewMasterClient(conn)
	}
	for i := range masterClients {
		// j位于[0，i]之间
		j := rand.Intn(i + 1)
		masterClients[i], masterClients[j] = masterClients[j], masterClients[i]
	}
	mc := NewMClient(masterClients)
	cfg.clients[mc] = true
	cfg.nextClientId++
	return mc
}

// 检查超时，限制执行时间2分钟
func (cfg *masterConfig) checkTimeout() {
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took loner than 120 seconds")
	}
}

// 清理工作
func (cfg *masterConfig) cleanup() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	for i := 0; i < len(cfg.servers); i++ {
		if cfg.servers[i] != nil {
			cfg.servers[i].Kill()
		}
	}
	cfg.checkTimeout()
}
