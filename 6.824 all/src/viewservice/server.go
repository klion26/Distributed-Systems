package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import (
	"sync/atomic"
	"errors"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	lastPing map[string]time.Time
	curView  View
	hasView bool
	hasAcked bool
	secondBackup string
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	if args.Viewnum == 0 {
		fmt.Println("viewnum0[[[[", vs.hasView, vs.hasAcked, "p", vs.curView.Primary, "me", args.Me, "b", vs.curView.Backup)
	}
	vs.lastPing[args.Me] = time.Now()

	vs.mu.Lock()
	if !vs.hasView {
		fmt.Println("Init view")
		vs.hasView = true
		vs.curView.Viewnum += 1
		vs.curView.Primary = args.Me
		vs.curView.Backup = ""
		vs.hasAcked = false
		fmt.Println("Init view", args.Viewnum, vs.curView.Primary, vs.curView.Backup)
	} else {
		if vs.curView.Primary == args.Me {
			if args.Viewnum == 0 {
				//这里是用 0 判断,还是用 vs.curView.Viewnum 判断?
				//viewnum will change

				if "" != vs.curView.Backup {
					//swap primary and backup
					tmp := vs.curView.Primary
					vs.curView.Primary = vs.curView.Backup
					vs.secondBackup = tmp
					vs.curView.Backup = ""
				}

				vs.curView.Viewnum += 1
				vs.hasAcked = false
				fmt.Println("Primary with0", args.Viewnum, vs.curView.Primary, vs.curView.Backup)
			} else {
				if args.Viewnum == vs.curView.Viewnum {
					//为什么要加这个 args.View == vs.curView.Viewnum 的逻辑???
					if vs.secondBackup != "" {
						vs.curView.Backup = vs.secondBackup
						vs.secondBackup = ""
						vs.curView.Viewnum += 1
						vs.hasAcked = false
					} else {
						vs.hasAcked = true
					}
					fmt.Println("Primary *", args.Viewnum, args.Me, vs.curView.Primary, vs.curView.Backup)
				}
			}
		} else {
			if vs.curView.Backup == args.Me {
				if 0 == args.Viewnum {
					//backup restart 的这种情况, 是直接 viewnum 加1
					//还是先将 backup 放到 secondbackup
					//等下次 primary 来的时候再进行提升???
					fmt.Println("Backup with0", vs.hasAcked, args.Viewnum, vs.curView.Primary, vs.curView.Backup)
					if vs.hasAcked {
						//vs.curView.Viewnum += 1
						vs.secondBackup = args.Me
						fmt.Println("change view by adding a backup", args.Me, vs.curView.Primary)
						vs.hasAcked =false
					}
				} else {
					//do nothing
					fmt.Println("Backup ack")
				}
			} else {
				if "" == vs.curView.Backup {
					fmt.Println("add backup")
					if vs.hasAcked {
						//应该再加一个 second backup, 预备 backup
						//等 primary ack 的时候,将 second backup 提升为 backup
						vs.secondBackup = args.Me
						//vs.hasAcked = false
						fmt.Println("change view by adding a backup", args.Me, vs.curView.Primary, vs.curView.Backup)
						fmt.Println(vs.hasAcked)
					}
				} else {
					//do nothing
					//这里是否需要将第三方的增加到 secondBackup???
					fmt.Println("DDDDDDD")
				}
			}
		}
	}
	vs.mu.Unlock()

	reply.View = vs.curView
	//end of my code
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	if !vs.hasView {
		return errors.New("current view is nil")
	}

	//这里需要加锁吗???
	//需要加锁的原因: 可能获取到不对的 view
	//不需要加锁的原因: 这个操作算原子操作???
	reply.View = vs.curView
	//end of my code
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	//if vs didn't receive primary's ping
	if vs.hasAcked {
		eclipsTime := time.Now().Sub(vs.lastPing[vs.curView.Primary])
		if eclipsTime > DeadPings * PingInterval {
			vs.mu.Lock()
				/**
			  只有 primary 已经 ack 了当前的 view,
			  才能进行 view 的更改
			 */
				if vs.curView.Backup == "" {
					vs.hasView = false
					fmt.Println("view destory")
				} else {
					fmt.Println("tick[[[[", vs.hasAcked)
					vs.curView.Primary = vs.curView.Backup
					delete(vs.lastPing, vs.curView.Backup)
					vs.curView.Backup = ""
					vs.curView.Viewnum += 1
					vs.hasAcked = false
					fmt.Println("in tick", vs.hasAcked, "p", vs.curView.Primary, "b", vs.curView.Backup)
				}
			vs.mu.Unlock()
		}

		if "" != vs.curView.Backup {
			eclipsBackup := time.Now().Sub(vs.lastPing[vs.curView.Backup])
			if eclipsBackup > DeadPings * PingInterval{
				vs.mu.Lock()
				fmt.Println("dead backup?")
				vs.curView.Backup = ""
				//这里的 Viewnum 是否需要+1???
//				vs.curView.Viewnum += 1
				vs.hasAcked = false
				vs.mu.Unlock()
			}
		}
	}
	// Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	//my initializations begin
	vs.mu = sync.Mutex{}
	vs.curView = View{0, "", ""}
	vs.hasView = false
	vs.lastPing = make(map[string]time.Time)
	vs.secondBackup = ""
	//my initializations end
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
