package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

const (
	DBG_PREPARE  = false
	DBG_ACCEPT   = false
	DBG_PROPOSER = true
	DBG_DECIDED  = true
)

type Slot_t struct {
	Decided bool
	V       interface{}
}

type Proposal struct {
	// should not use PNum 0
	PNum  int
	Value interface{}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	peers_count int
	majority    int
	max_seq     int
	// highest number ever passed to Done
	z int

	// [A] highest accept seen
	APa map[int]Proposal
	// [A] highest prepare seen
	APp map[int]int

	Lslots map[int]Slot_t
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	fmt.Printf("Call srv:%s name:%s\n", srv, name)
	err = c.Call(name, args, reply)
	fmt.Printf("After Call %s, err:%v, rpl:%v\n", srv, err, reply)

	if err == nil {
		return true
	}
	return false
}

func (px *Paxos) mylog(dbg bool, funcname, msg string) {
	if dbg {
		fmt.Printf("[%s] me:%s\n", funcname, px.me)
		fmt.Printf("...%s\n", msg)
	}
}

/* Proposer
 * send prepare request for pNum in slot seq
 * return
 * - if OK to send accept request
 * - highest-numbered proposal
 */
func (px *Paxos) send_prepare(seq int, pNum int) (bool, Proposal) {
	ok_count := 0
	p := Proposal{}

	for _, peer := range px.peers {
		args := &PrepareArgs{}
		reply := &PrepareReply{}

		args.Seq = seq
		args.PNum = pNum

		ok := call(peer, "Paxos.Prepare", args, reply)

		// TODO: what if I got only one Reject?

		if ok && reply.Err == OK {
			ok_count++
			if reply.Proposal.PNum > p.PNum {
				p = reply.Proposal
			}
		}
	}

	return (ok_count > px.majority), p
}

/* Acceptor
 * handler for prepare request
 */
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	if args.PNum > px.APp[args.Seq] {
		// prepare request with higher Proposal Number
		px.APp[args.Seq] = args.PNum
		reply.Err = OK
		reply.Proposal = px.APa[args.Seq]
	} else {
		// Already promised to Proposal with a higher Proposal Number
		reply.Err = Reject
		reply.Proposal = Proposal{}
	}
	return nil
}

/* Proposer
 * send accept requests
 * return true if success
 */
func (px *Paxos) send_accept(seq int, p Proposal) bool {
	ok_count := 0

	for _, peer := range px.peers {
		args := &AcceptArgs{}
		reply := &AcceptReply{}

		args.Seq = seq
		args.Proposal = p

		ok := call(peer, "Paxos.Accept", args, reply)

		if ok && reply.Err == OK {
			ok_count++
		}
	}

	return (ok_count > px.majority)
}

/* Acceptor
 * handler for Accept request
 */
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	if args.Proposal.PNum >= px.APp[args.Seq] {
		px.APp[args.Seq] = args.Proposal.PNum
		px.APa[args.Seq] = args.Proposal
		reply.Err = OK
	} else {
		reply.Err = Reject
	}
	return nil
}

/* Proposer
 * send decided value to all
 */
func (px *Paxos) send_decided(seq int, v interface{}) {
	for _, peer := range px.peers {
		args := &DecdidedArgs{}
		reply := &DecidedReply{}

		args.Seq = seq
		args.V = v

		call(peer, "Paxos.Decided", args, reply)
	}
}

/* Learner
 * handler for decide notification
 */
func (px *Paxos) Decided(args *DecdidedArgs, reply *DecidedReply) error {
	if DBG_DECIDED {
		fmt.Printf("[Decided] me:%d\n....Seq=%d V=%v\n", px.me, args.Seq, args.V)
	}
	px.Lslots[args.Seq] = Slot_t{true, args.V}
	if args.Seq > px.max_seq {
		px.max_seq = args.Seq
	}
	reply.Err = OK
	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// run Paxos algorithm in a new thread(run the Paxos protocol concurrently)
	// play the role of proposer

	// Your code here.

	if DBG_PROPOSER {
		fmt.Printf("[Start] me:%d\n....Start seq=%d v=%v\n",
			px.me, seq, v)
	}

	// I'm Proposer
	go func() {
		for {
			n := px.APp[seq] + 1
			prepare_ok, p := px.send_prepare(seq, n)
			if !prepare_ok {
				continue
			}

			new_p := Proposal{}

			// no proposal yet, use v
			if p.PNum == 0 {
				new_p.Value = v
			} else {
				new_p.Value = p.Value
			}

			new_p.PNum = n

			if DBG_PROPOSER {
				fmt.Printf("[Start] me:%d\n....prepare OK, proposal=%v\n",
					px.me, new_p)
			}

			accept_ok := px.send_accept(seq, new_p)
			if !accept_ok {
				continue
			}

			if DBG_PROPOSER {
				fmt.Printf("[Start] me:%d\n....accept OK\n", px.me)
			}

			px.send_decided(seq, new_p.Value)

			if DBG_PROPOSER {
				fmt.Printf("[Start] me:%d\n....decided\n", px.me)
			}
			break
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.z = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.max_seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	return px.Lslots[seq].Decided, px.Lslots[seq].V
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	fmt.Printf("#### Make %d/%d ####\n", me, len(peers))

	// Your initialization code here.
	px.peers_count = len(peers)
	px.majority = (px.peers_count + 1) / 2
	px.max_seq = -1
	px.z = -1

	px.APp = map[int]int{}
	px.APa = map[int]Proposal{}
	px.Lslots = map[int]Slot_t{}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
