# rache
 Rache is a distributed key-value in-memory cache based on Raft consensus algorithm in Golang and gRPC provided gRPC provides the underlying RPC communication service between various servers.

Rache supports concurrent secure access by multiple clients, and achieves data consistency among various servers according to the raft protocol. Raft is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance. If you want to learn more about the principles of the raft algorithm, I think this website (https://raft.github.io/) can definitely help you and the best way is to read the raft paper.

In a distributed raft cluster, the servers communicate through remote procedure calls. I chose gRPC as the underlying RPC communication framework. It is based on Google's protocol buffer, has high performance, and supports streaming RPC communication. Details Information can be found here: https://grpc.io/docs/reference/.

If you want to learn the raft algorithm, or build a simple distributed application for yourself, I believe that it will be very helpful to read this project. There are detailed comments in the code. I will keep updating this project and keep improving it, hoping that the performance and functions will become more and more powerful!
