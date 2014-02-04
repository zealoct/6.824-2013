package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need to uncomment this:
// import "time"

const (
	GET_CLIENT_DEBUG = false
	PUT_CLIENT_DEBUG = false
)

type Clerk struct {
	vs    *viewservice.Clerk
	cview viewservice.View
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}

func (ck *Clerk) updateView() {
	ck.cview, _ = ck.vs.Get()
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	if GET_CLIENT_DEBUG {
		fmt.Printf("\n[Client] Get(%s)\n", key)
	}

	// make sure we'v got a primary
	for ck.cview.Primary == "" {
		ck.updateView()
	}

	args := &GetArgs{}
	var reply GetReply
	args.Key = key

	if GET_CLIENT_DEBUG {
		fmt.Printf("[Client] ---- Primary:%s\n", ck.cview.Primary)
	}

	ok := call(ck.cview.Primary, "PBServer.Get", args, &reply)

	if GET_CLIENT_DEBUG {
		fmt.Printf("[Client] ---- ok:%t, reply:%s\n", ok, reply)
	}

	for ok == false || reply.Err == ErrWrongServer {
		ck.updateView()

		if GET_CLIENT_DEBUG {
			fmt.Printf("[Client] ---- retry Primary:%s\n", ck.cview.Primary)
		}

		ok = call(ck.cview.Primary, "PBServer.Get", args, &reply)

		if GET_CLIENT_DEBUG {
			fmt.Printf("[Client] ---- ok:%t, reply:%s\n", ok, reply)
		}
	}

	if reply.Err == OK {
		return reply.Value
	}

	return fmt.Sprintf("%s", reply.Err)
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {

	if PUT_CLIENT_DEBUG {
		fmt.Printf("\nClient Put (%s => %s)\n", key, value)
	}
	// Your code here.
	// make sure we'v got a primary
	for ck.cview.Primary == "" {
		ck.updateView()
	}

	if PUT_CLIENT_DEBUG {
		fmt.Printf("---Primary: %s\n", ck.cview.Primary)
	}

	args := &PutArgs{key, value}
	reply := PutReply{}

	ok := call(ck.cview.Primary, "PBServer.Put", args, &reply)

	for ok == false {
		ck.updateView()
		ok = call(ck.cview.Primary, "PBServer.Put", args, &reply)
	}

	if PUT_CLIENT_DEBUG {
		fmt.Printf("Err:%s\n", reply.Err)
	}

	if reply.Err == OK {
		return
	}

}
