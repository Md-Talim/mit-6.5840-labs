package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		clientID: kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, _ := lk.ck.Get(lk.lockname)
		if value != "" { // lock is not free
			continue
		}

		err := lk.ck.Put(lk.lockname, lk.clientID, version)
		if err == rpc.OK {
			return
		}
	}
}

func (lk *Lock) Release() {
	value, version, _ := lk.ck.Get(lk.lockname)
	if value == lk.clientID {
		lk.ck.Put(lk.lockname, "", rpc.Tversion(version))
	}
}
