package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type DBEntry struct {
	Value   string
	Version rpc.Tversion
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	db map[string]DBEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		db: make(map[string]DBEntry),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if entry, found := kv.db[args.Key]; found {
		reply.Value = entry.Value
		reply.Version = entry.Version
		reply.Err = rpc.OK
		return
	}

	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if entry, found := kv.db[args.Key]; found {
		if args.Version != entry.Version {
			reply.Err = rpc.ErrVersion
			return
		}

		kv.db[args.Key] = DBEntry{
			Value:   args.Value,
			Version: rpc.Tversion(entry.Version + 1),
		}
		reply.Err = rpc.OK
		return
	}

	if args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		return
	}

	kv.db[args.Key] = DBEntry{
		Value:   args.Value,
		Version: rpc.Tversion(1),
	}
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
