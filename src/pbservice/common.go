package pbservice

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongServer  = "ErrWrongServer"
	ErrFwdToPrimary = "ErrFwdToPrimary"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

type SyncArgs struct {
	db map[string]string
}

type SyncReply struct {
	Err Err
}
