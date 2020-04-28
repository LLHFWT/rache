package kv

import (
	"fmt"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"os"
	"rache/raft"
	"rache/utils"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const CheckTimeout = 1 * time.Second

func TestStaticShards(t *testing.T) {
	fmt.Println("Test: static shards...")
	cfg := makeKVConfig(t, 3, -1)
	defer cfg.cleanup()
	cli := cfg.makeKVClient()

	cfg.join(0)
	cfg.join(1)
	// 加入key/value
	n := 10
	ka, va := make([]string, n), make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = utils.RandString(20)
		cli.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
	}
	cfg.checkLogs()

	ch := make(chan bool)
	for xi := 0; xi < n; xi++ {
		ck1 := cfg.makeKVClient() // only one call allowed per client
		go func(i int) {
			defer func() { ch <- true }()
			checkKV(t, ck1, ka[i], va[i])
		}(xi)
	}
	ndone := 0
	done := false
	for !done {
		select {
		case <-ch:
			ndone += 1
		case <-time.After(2 * time.Second):
			done = true
			break
		}
	}
	if ndone != 10 {
		t.Fatalf("expected 10 completions, get %v", ndone)
	}

	fmt.Printf("  ... Passed\n")
	clearStateFile(cfg.persistId)
}

func TestJoinLeave(t *testing.T) {
	fmt.Println("Test: join then leave...")
	cfg := makeKVConfig(t, 3, -1)
	defer cfg.cleanup()
	cli := cfg.makeKVClient()

	cfg.join(0)
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = utils.RandString(5)
		cli.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
	}

	cfg.join(1)
	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
		x := utils.RandString(5)
		cli.Append(ka[i], x)
		va[i] += x
	}

	cfg.leave(0)
	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
		x := utils.RandString(5)
		cli.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(time.Second)
	cfg.checkLogs()
	cfg.ShutdownGroup(0)
	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
	}
	fmt.Println("  ... Passed")
	clearStateFile(cfg.persistId)
}

func TestSnapshot(t *testing.T) {
	fmt.Println("Test: snapshots, join, and leave ...")
	cfg := makeKVConfig(t, 3, 1000)
	defer cfg.cleanup()
	cli := cfg.makeKVClient()

	cfg.join(0)
	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = utils.RandString(20)
		cli.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
	}
	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
		x := utils.RandString(20)
		cli.Append(ka[i], x)
		va[i] += x
	}
	cfg.leave(1)
	cfg.join(0)
	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
		x := utils.RandString(20)
		cli.Append(ka[i], x)
		va[i] += x
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
	}
	time.Sleep(time.Second)
	cfg.checkLogs()
	fmt.Println("... Passed")
	clearStateFile(cfg.persistId)
}

func TestConcurrent(t *testing.T) {
	fmt.Println("Test: servers miss configuration changes...")
	cfg := makeKVConfig(t, 3, 100)
	defer cfg.cleanup()
	cli := cfg.makeKVClient()
	cfg.join(0)
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = utils.RandString(5)
		cli.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)
	ff := func(i int) {
		defer func() { ch <- true }()
		cli1 := cfg.makeKVClient()
		for atomic.LoadInt32(&done) == 0 {
			x := utils.RandString(5)
			cli1.Append(ka[i], x)
			va[i] += x
			time.Sleep(10 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}
	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(100 * time.Millisecond)
	cfg.join(0)
	time.Sleep(100 * time.Millisecond)
	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
	}
	fmt.Println("...Passed")
	clearStateFile(cfg.persistId)
}

func TestConcurrentMore(t *testing.T) {
	fmt.Println("Test: more corrent puts and configuration changes...")
	cfg := makeKVConfig(t, 3, -1)
	defer cfg.cleanup()
	cli := cfg.makeKVClient()
	cfg.join(1)
	cfg.join(0)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = utils.RandString(1)
		cli.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)
	ff := func(i int, cli1 *KVClient) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := utils.RandString(1)
			cli1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		cli1 := cfg.makeKVClient()
		go ff(i, cli1)
	}

	time.Sleep(3000 * time.Millisecond)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
	}

	fmt.Println(" ...Passed")
	clearStateFile(cfg.persistId)
}

func TestDelete(t *testing.T) {
	fmt.Printf("Test: shard deletion (challenge 1) ...\n")
	cfg := makeKVConfig(t, 3, 1)
	defer cfg.cleanup()
	cli := cfg.makeKVClient()
	cfg.join(0)
	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = utils.RandString(1000)
		cli.Put(ka[i], va[i])
	}
	for i := 0; i < 3; i++ {
		checkKV(t, cli, ka[i], va[i])
	}
	for iter := 0; iter < 2; iter++ {
		cfg.join(1)
		cfg.leave(0)
		cfg.join(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			checkKV(t, cli, ka[i], va[i])
		}
		cfg.leave(1)
		cfg.join(0)
		cfg.leave(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			checkKV(t, cli, ka[i], va[i])
		}
	}

	cfg.join(1)
	cfg.join(2)
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		checkKV(t, cli, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		checkKV(t, cli, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		checkKV(t, cli, ka[i], va[i])
	}

	total := 0
	for gi := 0; gi < cfg.groupNum; gi++ {
		for i := 0; i < cfg.serverNumPerGroup; i++ {
			if cfg.groups[gi].saved[i] != nil {

				raft, _ := cfg.groups[gi].saved[i].ReadStateSize()
				snap, _ := cfg.groups[gi].saved[i].ReadSnapshot()
				total += raft + len(snap)
			}
		}
	}
	expected := 10 * (((n - 3) * 1000) + 2*3*1000 + 6000)
	if total > expected {
		t.Fatalf("snapshot + persisted Raft state are too big: %v > %v\n", total, expected)
	}

	for i := 0; i < n; i++ {
		checkKV(t, cli, ka[i], va[i])
	}
	fmt.Println("...Passed")
	clearStateFile(cfg.persistId)
}

func checkKV(t *testing.T, cli *KVClient, key string, value string) {
	v := cli.Get(key)
	if v != value {
		t.Fatalf("Get(%s): expected \n%v\nreceived:\n%v", key, value, v)
	}
}

func clearStateFile(n int) {
	for i := 0; i < n; i++ {
		os.Remove("state" + strconv.Itoa(i))
		os.Remove("state" + strconv.Itoa(i) + "_copy")
		os.Remove("snapshot" + strconv.Itoa(i))
		os.Remove("snapshot" + strconv.Itoa(i) + "_copy")
	}
}

// 一组shard server
type group struct {
	gid          int32
	addrList     []string
	raftClients  []raft.RaftClient
	servers      []*ShardKV
	saved        []*raft.Persist
	groupClients []GroupClient
}

var cpu_once sync.Once

type kvConfig struct {
	mu        sync.Mutex
	t         *testing.T
	start     time.Time
	persistId int
	kvPortId  int

	// master
	masterAddrList []string //master server地址
	masterNums     int
	masterPbCli    []MasterClient
	masterRaftCli  []raft.RaftClient
	masterServers  []*Master //master servers列表
	masterCli      *MClient  //访问master获取config的client

	// shard kv server groups
	groupNum          int      //一共有多少个group
	serverNumPerGroup int      //每个group中server的数目
	groups            []*group //group列表

	// shard kv clients
	clients      map[*KVClient]bool // 访问shard kv的clients
	nextClientId int                // 下一个client的ID
	maxRaftSize  int                //raft日志大小超过该值时需要take snapshot
}

// 创建一个config
func makeKVConfig(t *testing.T, serverNumPerGroup int, maxRaftSize int) *kvConfig {
	cpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(utils.MakeSeed())
	})
	runtime.GOMAXPROCS(4)

	// 初始化基本参数
	cfg := &kvConfig{
		t:           t,
		maxRaftSize: maxRaftSize,
		start:       time.Now(),
		persistId:   0,
		kvPortId:    12306,
		// 设定一共有三个master节点
		masterAddrList: []string{"127.0.0.1:12301", "127.0.0.1:12302", "127.0.0.1:12303"},
		masterNums:     3,
		masterServers:  make([]*Master, 3),
		masterPbCli:    make([]MasterClient, 3),
		masterRaftCli:  make([]raft.RaftClient, 3),
		// 设定一共有三个shard kv group
		groupNum:          3,
		groups:            make([]*group, 3),
		serverNumPerGroup: serverNumPerGroup, // 每个group的节点数
		clients:           make(map[*KVClient]bool),
		nextClientId:      serverNumPerGroup + 1000,
	}

	// 初始化master客户端
	for i := range cfg.masterAddrList {
		conn, err := grpc.Dial(cfg.masterAddrList[i], grpc.WithInsecure())
		if err != nil {
			log.Fatal("start conn error")
		}
		cfg.masterPbCli[i] = NewMasterClient(conn)
		cfg.masterRaftCli[i] = raft.NewRaftClient(conn)
	}
	cfg.masterCli = NewMClient(cfg.masterPbCli)

	// 初始化master servers
	for i := 0; i < cfg.masterNums; i++ {
		cfg.startMasterServer(i)
	}

	// 初始化groups
	for i := 0; i < cfg.groupNum; i++ {
		addrList := make([]string, cfg.serverNumPerGroup)
		for j := range addrList {
			addrList[j] = "127.0.0.1:" + strconv.Itoa(cfg.kvPortId)
			cfg.kvPortId++
		}
		g := &group{
			gid:          int32(100 + i),
			servers:      make([]*ShardKV, cfg.serverNumPerGroup),
			saved:        make([]*raft.Persist, cfg.serverNumPerGroup),
			addrList:     addrList,
			raftClients:  make([]raft.RaftClient, cfg.serverNumPerGroup),
			groupClients: make([]GroupClient, cfg.serverNumPerGroup),
		}
		// 初始化raft clients
		for j := range addrList {
			conn, err := grpc.Dial(addrList[j], grpc.WithInsecure())
			if err != nil {
				log.Fatal("start conn error")
			}
			g.raftClients[j] = raft.NewRaftClient(conn)
			g.groupClients[j] = NewGroupClient(conn)
			// 添加到全局变量中
			GroupClientMap[addrList[j]] = g.groupClients[j]
		}
		cfg.groups[i] = g
		// 启动group中的每一个server
		for j := 0; j < cfg.serverNumPerGroup; j++ {
			cfg.startKVServer(i, j)
		}
	}

	return cfg
}

// 启动kv server, i代表group索引， j代表group内server的索引
func (cfg *kvConfig) startKVServer(i, j int) {
	cfg.mu.Lock()
	group := cfg.groups[i]
	if group.saved[j] != nil {
		group.saved[j], _ = group.saved[j].Copy(cfg.persistId)
	} else {
		group.saved[j] = raft.NewPersist(cfg.persistId)
	}
	cfg.persistId++
	cfg.mu.Unlock()

	group.servers[j] = NewKVServer(group.raftClients, int32(j), group.saved[j], cfg.maxRaftSize, group.gid, cfg.masterPbCli)
	addr := group.addrList[j]
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen err: %s\n", err)
	}
	log.Printf("listen %s successfully!", addr)
	s := grpc.NewServer()
	raft.RegisterRaftServer(s, group.servers[j].rf)
	RegisterGroupServer(s, group.servers[j])
	go func() {
		if err = s.Serve(listen); err != nil {
			log.Fatal("serve error")
		}
	}()
}

// 创建master Server
func (cfg *kvConfig) startMasterServer(i int) {
	cfg.mu.Lock()
	p := raft.NewPersist(cfg.persistId)
	cfg.persistId++
	cfg.mu.Unlock()

	cfg.masterServers[i] = StartServer(int32(i), p, cfg.masterRaftCli)
	addr := cfg.masterAddrList[i]
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen err: %s\n", err)
	}
	log.Printf("Master server %d listen %s successfully!", i, addr)
	s := grpc.NewServer()
	raft.RegisterRaftServer(s, cfg.masterServers[i].rf)
	RegisterMasterServer(s, cfg.masterServers[i])
	go func() {
		if err = s.Serve(listen); err != nil {
			log.Fatal("serve error")
		}
	}()
}

// 执行完毕后检查是否超时
func (cfg *kvConfig) checkTimeout() {
	if !cfg.t.Failed() && time.Since(cfg.start) > 2*time.Minute {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

// 检查snapshot是否合法
func (cfg *kvConfig) checkLogs() {
	for i := 0; i < cfg.groupNum; i++ {
		for j := 0; j < cfg.serverNumPerGroup; j++ {
			if cfg.groups[i].saved[j] == nil {
				continue
			}
			stateSize, _ := cfg.groups[i].saved[j].ReadStateSize()
			snapshot, _ := cfg.groups[i].saved[j].ReadSnapshot()
			// 检查raft log的大小是否合适
			if cfg.maxRaftSize >= 0 && stateSize > 2*cfg.maxRaftSize {
				cfg.t.Fatalf("raft state size is %v, but maxRaftSize is %v", stateSize, cfg.maxRaftSize)
			}
			// 检查未启用snapshot的情况下，是否执行snapshot
			if cfg.maxRaftSize < 0 && len(snapshot) > 0 {
				cfg.t.Fatalf("maxRaftSize is -1, but snapshot is non-empty")
			}
		}
	}
}

// 启动一个组中所有server
func (cfg *kvConfig) startGroup(i int) {
	for j := 0; j < cfg.serverNumPerGroup; j++ {
		cfg.startKVServer(i, j)
	}
}

func (cfg *kvConfig) ShutdownKVServer(i, j int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	group := cfg.groups[i]
	if group.saved[j] != nil {
		group.saved[j], _ = group.saved[j].Copy(cfg.persistId)
		cfg.persistId++
	}
	kv := group.servers[j]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		group.servers[j] = nil
	}
}

func (cfg *kvConfig) ShutdownGroup(i int) {
	for j := 0; j < cfg.serverNumPerGroup; j++ {
		cfg.ShutdownKVServer(i, j)
	}
}

// 创建访问shard kv的客户端
func (cfg *kvConfig) makeKVClient() *KVClient {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	cli := NewKVClient(cfg.masterPbCli)
	cfg.nextClientId++
	cfg.clients[cli] = true
	return cli
}

// 删除kv客户端
func (cfg *kvConfig) deleteCli(c *KVClient) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	delete(cfg.clients, c)
}

func (cfg *kvConfig) join(g int32) {
	cfg.joinm([]int32{g})
}

// 加入一些组
func (cfg *kvConfig) joinm(gs []int32) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	groups := make(map[int32]*Servers)
	for _, g := range gs {
		gid := cfg.groups[g].gid
		servers := &Servers{
			ServerList: make([]string, cfg.serverNumPerGroup),
		}
		copy(servers.ServerList, cfg.groups[g].addrList)
		groups[gid] = servers
	}
	log.Println("Join groups:", groups)
	cfg.masterCli.Join(groups)
}

// 撤出一个组
func (cfg *kvConfig) leave(g int32) {
	cfg.leavem([]int32{g})
}

// 将一些组撤出
func (cfg *kvConfig) leavem(gs []int32) {
	gids := make([]int32, 0, len(gs))
	for _, g := range gs {
		gids = append(gids, cfg.groups[g].gid)
	}
	log.Println("Leave groups:", gids)
	cfg.masterCli.Leave(gids)
}

func (cfg *kvConfig) cleanup() {
	for g := 0; g < cfg.groupNum; g++ {
		cfg.ShutdownGroup(g)
	}
	cfg.checkTimeout()
}
