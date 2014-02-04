package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"

const (
	ROLEP = "PRIMARY"
	ROLEB = "BACKUP"
	ROLEI = "IDLE"

	SERVER_DEBUG = false
	GET_DEBUG    = true && SERVER_DEBUG
	PUT_DEBUG    = true && SERVER_DEBUG
	SYNC_DEBUG   = true && SERVER_DEBUG
	FWD_DEBUG    = true && SERVER_DEBUG
	TICK_DEBUG   = false && SERVER_DEBUG
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	cview  viewservice.View
	db     map[string]string
	role   string
	synced bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	if GET_DEBUG {
		fmt.Printf("GET(%s) me:%s", args.Key, pb.me)
	}
	// Your code here.
	var ok bool
	reply.Value, ok = pb.db[args.Key]
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	if GET_DEBUG {
		fmt.Printf("---- %s => %s, Err:%s\n", args.Key, reply.Value, reply.Err)
	}

	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {

	// Your code here.
	if PUT_DEBUG {
		fmt.Printf("\nPut(%s => %s) [%s]\n", args.Key, args.Value, pb.me)
	}

	// Put re quests come from clients, so I should be P
	if pb.updateView(); pb.role != ROLEP {
		reply.Err = ErrWrongServer
		return nil
	}

	// first forward to B
	ok := pb.forward(args, reply, pb.cview.Backup)

	if ok && reply.Err == ErrFwdToPrimary {
		// Backup exists but return ErrFwdToPrimary
		// TODO: Get the latest View from viewservice
		reply.Err = ErrWrongServer
		return nil
	}

	// it is ok to save the value now
	pb.db[args.Key] = args.Value
	reply.Err = OK

	return nil
}

func (pb *PBServer) forward(args *PutArgs, reply *PutReply, bkp string) bool {
	if FWD_DEBUG {
		fmt.Printf("fwd [%s]\n", pb.me)
		fmt.Printf("---- to %s\n", pb.cview.Backup)
		fmt.Printf("---- args %v\n", args)
	}

	ok := call(bkp, "PBServer.Forward", args, reply)

	if FWD_DEBUG {
		fmt.Printf("---- fwd ok:%t reply:%v\n", ok, reply)
	}

	return ok
}

// Handle forward requests from primary, so I should be Backup
func (pb *PBServer) Forward(args *PutArgs, reply *PutReply) error {

	// fmt.Printf("Fwd [%s]\n", pb.me)
	// fmt.Printf("---- %s => %s\n", args.Key, args.Value)

	// Err if I am P
	if pb.role == ROLEP {
		reply.Err = ErrFwdToPrimary
		return nil
	}

	// Err if I am I
	if pb.role == ROLEI {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.db[args.Key] = args.Value
	reply.Err = OK

	return nil
}

// handle sync requests from primary, so I should be Backup
func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
	// update current view to see if I am B
	pb.updateView()

	if SYNC_DEBUG {
		fmt.Printf("Sync\n ")
		fmt.Printf("---- I'm %s\n, role:%s\n", pb.me, pb.role)
		fmt.Printf("---- db:%v\n", args.db)
	}

	if pb.role == ROLEP || pb.role == ROLEI {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.db = args.db
	reply.Err = OK

	return nil
}

func (pb *PBServer) updateView() {

	// fetch next view
	next_view, _ := pb.vs.Ping(pb.cview.Viewnum)

	// change nothing if view has not changed
	if pb.cview.Viewnum == next_view.Viewnum {
		return
	}

	// update my role
	if next_view.Primary == pb.me {
		pb.role = ROLEP
	} else if next_view.Backup == pb.me {
		pb.role = ROLEB
	} else {
		pb.role = ROLEI
	}

	// update sync when
	// 1. I am P; 2. B is not null
	if TICK_DEBUG {
		fmt.Printf("\nNew View! I'am %s\n", pb.me)
		fmt.Printf("---- New Role:%s, new Backup:%s\n", pb.role, next_view.Backup)
		fmt.Printf("---- Need sync:%t\n", pb.role == ROLEP && next_view.Backup != "")
	}

	if pb.role == ROLEP && next_view.Backup != "" {
		// args := &SyncArgs{}
		// reply := &SyncReply{}

		// args.db = map[string]string{}

		for k, v := range pb.db {
			// fmt.Printf("---- syncing %s, %s\n", k, v)
			args := &PutArgs{k, v}
			reply := &PutReply{}

			pb.forward(args, reply, next_view.Backup)
			// args.db[k] = v
		}

		// if TICK_DEBUG {
		// 	fmt.Printf("---- Sync(%v)\n", args)
		// }

		// ok := call(next_view.Backup, "PBServer.Sync", args, reply)

		// if TICK_DEBUG {
		// 	fmt.Printf("---- Sync ok=%t reply=%v\n", ok, reply)
		// }

		// if ok == false || reply.Err != OK {
		// 	fmt.Errorf("!!!! Sync to Backup %s return ok:%t err %s\n", next_view.Backup, ok, reply.Err)
		// }
	}

	// update current view
	pb.cview = next_view
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.updateView()

	if TICK_DEBUG {
		fmt.Printf("\nTick() [%s]\n", pb.me)
		fmt.Printf("----role:%s, view:%s\n\n", pb.role, pb.cview)
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.cview = viewservice.View{}
	pb.db = map[string]string{}
	pb.synced = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
