package paxos

const (
	OK     = "OK"
	Error  = "Error"
	Reject = "Reject"
)

type Err string

type PrepareArgs struct {
	Seq  int
	PNum int
}

type PrepareReply struct {
	Err      Err
	Proposal Proposal
}

type AcceptArgs struct {
	Seq      int
	Proposal Proposal
}

type AcceptReply struct {
	Err Err
}

type DecdidedArgs struct {
	Seq int
	V   interface{}
}

type DecidedReply struct {
	Err Err
}
