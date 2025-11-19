package lock

import (
	"fmt"
	"strings"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck            kvtest.IKVClerk
	state         bool
	id            string
	versionNumber rpc.Tversion
	key           string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:            ck,
		state:         false,
		id:            kvtest.RandValue(8),
		versionNumber: 0,
		key:           l,
	}
	return lk
}

func getStatusAndId(value string) (bool, string) {
	left, right, _ := strings.Cut(value, "$")
	if left == "1" {
		return true, right
	}
	return false, right
}

func makeValueFromStatusAndId(id string, status bool) (value string) {
	if status {
		value += "1"
	} else {
		value += "0"
	}

	value += "$" + id
	return
}

func (lk *Lock) tryAcquire() bool {

	value, version, err := lk.ck.Get(lk.key)

	if err == rpc.ErrNoKey {
		version = 0
	} else {
		acquired, id := getStatusAndId(value)
		if acquired {
			return id == lk.id
		}
	}

	lockVal := makeValueFromStatusAndId(lk.id, true)

	err = lk.ck.Put(lk.key, lockVal, version)
	if err == rpc.ErrMaybe {
		curVal, curVersion, _ := lk.ck.Get(lk.key)
		if curVal == lockVal && curVersion == version+1 {
			err = rpc.OK
		} else {
			err = rpc.ErrVersion
		}
	}

	if err == rpc.ErrVersion {
		return false
	}

	lk.state = true
	lk.versionNumber = version + 1
	return true
}

func (lk *Lock) Acquire() {
	if lk.state {
		return
	}
	for {
		if lk.tryAcquire() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	if !lk.state {
		return
	}
	lockVal := makeValueFromStatusAndId(lk.id, false)
	err := lk.ck.Put(lk.key, lockVal, lk.versionNumber)

	if err != rpc.OK {
		curVal, curVersion, _ := lk.ck.Get(lk.key)
		if curVersion <= lk.versionNumber {
			fmt.Printf("Error in releasing lock, key: %s, currval: %s, currVersion: %v", lk.key, curVal, curVersion)
		}
	}
	lk.state = false
}
