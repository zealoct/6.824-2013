package lockservice

import "net"
import "net/rpc"
import "log"
import "sync"
import "fmt"
import "os"
import "io"
import "time"

type LockServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool  // for test_test.go
  dying bool // for test_test.go

  am_primary bool // am I the primary?
  backup string   // backup's port

  // for each lock name, is it locked?
  locks map[string]bool

  // last lock request version for a lock
  lockver map[string]int64

  // last return value for a lock, corresponding to the lock request version
  lockret map[string]bool

  verexists map[int64]bool
  prevret map[int64]bool
}

type dyingError struct {
  err string
}

func (e *dyingError) Error() string {
  return e.err
}

//
// server Lock RPC handler.
//
// you will have to modify this function
//
func (ls *LockServer) Lock(args *LockArgs, reply *LockReply) error {
  ls.mu.Lock()
  defer ls.mu.Unlock()

  locked, _ := ls.locks[args.Lockname]
  // lastver, _ := ls.lockver[args.Lockname]
  // lastret, _ := ls.lockret[args.Lockname]

  exists, _ := ls.verexists[args.Version]
  prevret, _ := ls.prevret[args.Version]

  // if ls.dying {
  //   reply.OK = false
  //   fmt.Printf("%s Lock(%s:%t) dying return \n", 
  //     logHeader(ls.am_primary),
  //     args.Lockname, 
  //     locked)
  //   return &dyingError{"dying"}
  // }

  // check if i am primary
  if ls.am_primary {
    // need not to handle inconsistant! oh yeah~
    var backup_reply LockReply
    defer call(ls.backup, "LockServer.Lock", args, backup_reply)
  }

  if exists {
    reply.OK = prevret
  } else {
    if locked {
      reply.OK = false
    } else {
      reply.OK = true
      ls.locks[args.Lockname] = true
    }
    ls.verexists[args.Version] = true
    ls.prevret[args.Version] = reply.OK
  }

  // if exists == args.Version {
  //   // this could only happen in backup, need not handle 
  //   reply.OK = lastret
  // } else {
  //   // this is a new request
  //   if locked {
  //     reply.OK = false
  //     ls.lockver[args.Lockname] = args.Version
  //     ls.lockret[args.Lockname] = false
  //   } else {
  //     reply.OK = true
  //     ls.locks[args.Lockname] = true
  //     ls.lockver[args.Lockname] = args.Version
  //     ls.lockret[args.Lockname] = true
  //   }
  // }
  
  // fmt.Printf("%s Lock(%s:%t)=%t ver=%d lver=%d lret=%t dying=%t\n", 
  //   logHeader(ls.am_primary),
  //   args.Lockname, 
  //   locked, 
  //   reply.OK,
  //   args.Version,
  //   lastver,
  //   lastret,
  //   ls.dying)


  return nil
}

//
// server Unlock RPC handler.
//
func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {

  // Your code here.
  ls.mu.Lock()
  defer ls.mu.Unlock()

  locked, _ := ls.locks[args.Lockname]
  // lastver, _ := ls.lockver[args.Lockname]
  // lastret, _ := ls.lockret[args.Lockname]

  exists, _ := ls.verexists[args.Version]
  prevret, _ := ls.prevret[args.Version]

  // if ls.dying {
  //   reply.OK = false
  //   fmt.Printf("%s Unlock(%s:%t) dying return \n", 
  //     logHeader(ls.am_primary),
  //     args.Lockname, 
  //     locked)
  //   return &dyingError{"dying"}
  // }

  // check if i am primary
  if ls.am_primary {
    // need not to handle inconsistant! oh yeah~
    var backup_reply UnlockReply
    defer call(ls.backup, "LockServer.Unlock", args, backup_reply)
  }

  if exists {
    reply.OK = prevret
  } else {
    if locked {
      reply.OK = true
      ls.locks[args.Lockname] = false
    } else {
      reply.OK = false
    }
    ls.verexists[args.Version] = true
    ls.prevret[args.Version] = reply.OK
  }

  // if lastver == args.Version {
  //   reply.OK = lastret
  // } else {
  //   if locked {
  //     reply.OK = true
  //     ls.locks[args.Lockname] = false
  //     ls.lockver[args.Lockname] = args.Version
  //     ls.lockret[args.Lockname] = true
  //   } else {
  //     reply.OK = false
  //     ls.lockver[args.Lockname] = args.Version
  //     ls.lockret[args.Lockname] = false
  //   }
  // }

  

  // fmt.Printf("%s Unlock(%s:%t)=%t ver=%d lver=%d lret=%t dying=%t\n", 
  //   logHeader(ls.am_primary),
  //   args.Lockname, 
  //   locked, 
  //   reply.OK,
  //   args.Version,
  //   lastver,
  //   lastret,
  //   ls.dying)

  return nil
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this.
//
func (ls *LockServer) kill() {
  ls.dead = true
  ls.l.Close()
}

//
// hack to allow test_test.go to have primary process
// an RPC but not send a reply. can't use the shutdown()
// trick b/c that causes client to immediately get an
// error and send to backup before primary does.
// please don't change anything to do with DeafConn.
//
type DeafConn struct {
  c io.ReadWriteCloser
}
func (dc DeafConn) Write(p []byte) (n int, err error) {
  return len(p), nil
}
func (dc DeafConn) Close() error {
  return dc.c.Close()
}
func (dc DeafConn) Read(p []byte) (n int, err error) {
  return dc.c.Read(p)
}

func StartServer(primary string, backup string, am_primary bool) *LockServer {
  ls := new(LockServer)
  ls.backup = backup
  ls.am_primary = am_primary
  ls.locks = map[string]bool{}
  ls.lockver = map[string]int64{}
  ls.lockret = map[string]bool{}

  ls.verexists = map[int64]bool{}
  ls.prevret = map[int64]bool{}

  // Your initialization code here.


  me := ""
  if am_primary {
    me = primary
  } else {
    me = backup
  }

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(ls)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(me) // only needed for "unix"
  l, e := net.Listen("unix", me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  ls.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for ls.dead == false {
      conn, err := ls.l.Accept()
      if err == nil && ls.dead == false {
        if ls.dying {
          // process the request but force discard of reply.

          // without this the connection is never closed,
          // b/c ServeConn() is waiting for more requests.
          // test_test.go depends on this two seconds.
          go func() {
            time.Sleep(2 * time.Second)
            conn.Close()
          }()
          ls.l.Close()

          // this object has the type ServeConn expects,
          // but discards writes (i.e. discards the RPC reply).
          deaf_conn := DeafConn{c : conn}

          rpcs.ServeConn(deaf_conn)

          ls.dead = true
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && ls.dead == false {
        fmt.Printf("LockServer(%v) accept: %v\n", me, err.Error())
        ls.kill()
      }
    }
  }()

  return ls
}
