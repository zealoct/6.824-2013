package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

type OpType string
type OpState int

const (
	OPTYPE_PUT      = "Put"
	OPTYPE_GET      = "GET"
	OPSTATE_PENDING = 10010
	OPSTATE_DONE    = 10011
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    OpType
	Version int64
	Key     string
	Value   string
	State   OpState
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	log     []Op
	pending *Queue
}

func (kv *KVPaxos) syncLog() int {
	i = len(log)
	for {
		ok, value = kv.px.Status(i)

		// this log slot is already decided
		if ok {
			log[i] = value
			i++
			continue
		}

		// this log slot is empty
		return i
	}
}

func (kv *KVPaxos) worker() {
	for { // iteration on each pending request

		// fetch next pending request
		op := kv.pending.Poll()

		// if the queue is currently empty, sleep
		if fetch == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		for { // keep trying on the same request with increasing req number

			// 1. sync with Paxos
			seq := kv.syncLog()

			// 2. if req is redundent
			for _, opslot := range kv.log {
				if op.Version == opslot.Version {
					// return origin value directly
					// if Put request, this is useless
					// if Get request, set Value to the origin return value
					op.Value = opslot.Value
					op.State = OPSTATE_DONE
					break
				}
			}
			if op.State == OPSTATE_DONE {
				break
			}

			// 3. call Paxos.Start
			// create a new Op with state DONE for broadcasting
			req_op = Op{op.Type, op.Value, op.Key, op.Value, OPSTATE_DONE}
			kv.px.Start(seq, req_op)

			// 4. keep checking the status of the request

		} // keep trying on the same request with increasing req number
	} // iteration on each pending request
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	op := &Op{OPTYPE_GET, args.Version, args.Key, "", OPSTATE_PENDING}

	// pending the request into the pending list
	// do not need sync cause the queue has done it
	kv.pending.Push(op)

	for op.State == OPSTATE_PENDING {
		time.Sleep(100 * time.Millisecond)
	}

	// Get request is handled
	reply.Err = OK
	reply.Value = op.Value

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.

	op := &Op{OPTYPE_PUT, args.Version, args.Key, "", OPSTATE_PENDING}

	kv.pending.Push(op)

	for op.State == OPSTATE_PENDING {
		time.Sleep(100 * time.Millisecond)
	}

	// request is handled
	reply.Err = OK

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// this call is all that's needed to persuade
	// Go's RPC library to marshall/unmarshall
	// struct Op.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.mu = &sync.Mutex{}
	kv.pending = NewQueue()
	log = make([]int)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
