package main

import (
	"flag"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"rache/conf"
	"rache/http"
	"rache/kv"
	"rache/raft"
	"time"
)

var me = flag.Int("me", 0, "当前机器在集群中的编号，值从0开始")

func main() {
	flag.Parse()
	masterAddr := conf.GlobalConfigObj.MasterAddr
	kvAddr := conf.GlobalConfigObj.GroupAddr[*me]
	masterNum, kvServerNum := len(masterAddr), len(kvAddr)
	persistSeq := 0

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Kill, os.Interrupt)

	// 初始化master客户端
	masterRaftPbCli, masterPbCli := make([]raft.RaftClient, masterNum), make([]kv.MasterClient, kvServerNum)
	var masterCli *kv.MClient
	for i := range masterAddr {
		conn, err := grpc.Dial(masterAddr[i], grpc.WithInsecure())
		if err != nil {
			log.Fatal("start conn error")
		}
		masterPbCli[i] = kv.NewMasterClient(conn)
		masterRaftPbCli[i] = raft.NewRaftClient(conn)
	}
	masterCli = kv.NewMClient(masterPbCli)

	// 初始化master server
	masterServers := make([]*kv.Master, masterNum)
	for i := 0; i < masterNum; i++ {
		masterServers[i] = startMasterServer(i, persistSeq, masterRaftPbCli, masterAddr[i])
		persistSeq++
	}

	// 启动group clients
	groupRaftPbClients := make([]raft.RaftClient, kvServerNum)
	for i := 0; i < len(conf.GlobalConfigObj.GroupAddr); i++ {
		for j := 0; j < kvServerNum; j++ {
			conn, err := grpc.Dial(conf.GlobalConfigObj.GroupAddr[i][j], grpc.WithInsecure())
			if err != nil {
				log.Fatal("start conn error")
			}
			kv.GroupClientMap[conf.GlobalConfigObj.GroupAddr[i][j]] = kv.NewGroupClient(conn)
			if i == *me {
				groupRaftPbClients[j] = raft.NewRaftClient(conn)
			}
		}
	}
	kv.RacheClient = kv.NewKVClient(masterPbCli)

	// 启动group server
	groupServers := make([]*kv.ShardKV, kvServerNum)
	for i := 0; i < kvServerNum; i++ {
		groupServers[i] = startGroupServer(i, persistSeq, *me, kvAddr[i], groupRaftPbClients, masterPbCli)
		persistSeq++
	}

	http.Init()
	masterCli.Join(map[int32]*kv.Servers{0: {ServerList: kvAddr}})
	time.Sleep(2 * time.Second)
	<-c
}

// 启动master server
func startMasterServer(i, seq int, raftClients []raft.RaftClient, addr string) *kv.Master {
	p := raft.NewPersist(seq)
	server := kv.StartServer(int32(i), p, raftClients)
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen err: %s\n", err)
	}
	log.Printf("Master server %d listen %s successfully!", i, addr)
	s := grpc.NewServer()
	raft.RegisterRaftServer(s, server.Raft())
	kv.RegisterMasterServer(s, server)
	go func() {
		if err = s.Serve(listen); err != nil {
			log.Fatal("serve error")
		}
	}()
	return server
}

// 启动group server
func startGroupServer(i, seq, gid int, addr string, raftClients []raft.RaftClient, masterClients []kv.MasterClient) *kv.ShardKV {
	p := raft.NewPersist(seq)
	server := kv.NewKVServer(raftClients, int32(i), p, conf.GlobalConfigObj.MaxRaftSize, int32(gid), masterClients)
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen err: %s\n", err)
	}
	log.Printf("listen %s successfully!", addr)
	s := grpc.NewServer()
	raft.RegisterRaftServer(s, server.Raft())
	kv.RegisterGroupServer(s, server)
	go func() {
		if err = s.Serve(listen); err != nil {
			log.Fatal("serve error")
		}
	}()
	return server
}
