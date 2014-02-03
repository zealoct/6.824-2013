package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

const (
	TICK_DEBUG = false
	PING_DEBUG = false
	GET_DEBUG  = false
)

type ClientInfo struct {
	name         string    // client's name
	last_ping    time.Time // Time of last Ping
	last_viewnum uint      // Viewnum of last Ping
	idle         bool      // whether this client is idle
	dead         bool      // whether this client is dead
}

func (ci ClientInfo) String() string {
	return fmt.Sprintf("[N:%s, I:%t, D:%t]", ci.name, ci.idle, ci.dead)
}

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	current_view      View
	acked             bool // current view is acked by P
	primary_restarted bool // P has restarted within DeadPings * PingInterval

	clients map[string]*ClientInfo // list of k/v servers
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	if PING_DEBUG {
		fmt.Printf("\nPing: %v\n", args)
		fmt.Printf("----cur:%s, new:%t\n", vs.current_view, vs.clients[args.Me] == nil)
	}

	// Your code here.
	if args.Viewnum == 0 && vs.clients[args.Me] == nil {
		// a new server ping!
		vs.clients[args.Me] = new(ClientInfo)
		vs.clients[args.Me].name = args.Me
		vs.clients[args.Me].idle = true
	}

	if args.Me == vs.current_view.Primary &&
		args.Viewnum == 0 && vs.current_view.Viewnum != 0 && vs.acked {
		// check primary failed and restarted within an Interval
		if PING_DEBUG {
			fmt.Printf("----primary restart detected\n")
		}
		vs.primary_restarted = true
	}

	if args.Me == vs.current_view.Primary &&
		args.Viewnum == vs.current_view.Viewnum {
		// current view is acked by primary server
		if PING_DEBUG {
			fmt.Printf("----primary acked\n")
		}
		vs.acked = true
	}

	vs.clients[args.Me].last_ping = time.Now()
	vs.clients[args.Me].last_viewnum = args.Viewnum
	vs.clients[args.Me].dead = false

	reply.View = vs.current_view

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.current_view

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	if TICK_DEBUG {
		fmt.Printf("\nTick() VS\n")
		fmt.Printf("----current view:%v acked:%t\n", vs.current_view, vs.acked)
		fmt.Printf("----clients:%s\n", vs.clients)
	}

	// Your code here.
	var idle_client, inited_idle_client *ClientInfo
	currentTime := time.Now()

	// update the state of all clients
	for _, v := range vs.clients {
		since_last_ping := currentTime.Sub(v.last_ping)
		if since_last_ping > PingInterval && vs.acked {
			if TICK_DEBUG {
				fmt.Printf("----Ping time out, mark idle & dead\n")
			}
			v.dead = true
			v.idle = true
		}

		// fetch the first idle client, could be useful
		if v.idle && !v.dead && idle_client == nil {
			idle_client = v
			if TICK_DEBUG {
				fmt.Printf("----Got idle client:%s\n", idle_client.name)
			}
		}

		if v.idle && !v.dead && v.last_viewnum == vs.current_view.Viewnum && inited_idle_client == nil {
			inited_idle_client = v
			if TICK_DEBUG {
				fmt.Printf("----Got inited idle client:%s\n", inited_idle_client.name)
			}
		}
	}

	// proceed to next view if needed
	if vs.acked {
		cv := &vs.current_view

		if cv.Primary != "" && vs.clients[cv.Primary].dead {
			cv.Primary = ""
		}

		if vs.primary_restarted {
			if TICK_DEBUG {
				fmt.Printf("----primary_restarted, mark idle\n")
			}
			vs.clients[cv.Primary].idle = true
			cv.Primary = ""
		}

		if cv.Backup != "" && vs.clients[cv.Backup].dead {
			cv.Backup = ""
		}

		if cv.Primary == "" && cv.Backup == "" {
			if inited_idle_client != nil {
				cv.Primary = inited_idle_client.name
				cv.Viewnum++

				vs.acked = false
				vs.primary_restarted = false

				inited_idle_client.idle = false
			}
		} else if cv.Primary == "" {
			if vs.clients[cv.Backup].last_viewnum == cv.Viewnum {
				cv.Primary = cv.Backup
				cv.Backup = ""
				cv.Viewnum++

				vs.acked = false
				vs.primary_restarted = false
			}
		} else if cv.Backup == "" {
			if idle_client != nil {
				cv.Backup = idle_client.name
				cv.Viewnum++

				vs.acked = false

				idle_client.idle = false
			}
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.current_view = View{}
	vs.acked = true
	vs.primary_restarted = false
	vs.clients = map[string]*ClientInfo{}

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
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
