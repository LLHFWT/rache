package raft

import (
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

const (
	StateFile    = "state"
	SnapshotFile = "snapshot"
)

// used to persis raft peer's state and snapshot
type Persist struct {
	mu                      sync.RWMutex
	peerId                  int
	stateFile, snapshotFile string
}

func NewPersist(peerId int) *Persist {
	return &Persist{
		mu:           sync.RWMutex{},
		peerId:       peerId,
		stateFile:    StateFile + strconv.Itoa(peerId),
		snapshotFile: SnapshotFile + strconv.Itoa(peerId),
	}
}

// persis raft peer's state to file
func (ps *Persist) SaveRaftState(state []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ioutil.WriteFile(ps.stateFile, state, 0700)
}

// persist raft snapshot to file
func (ps *Persist) SaveStateAndSnapshot(state, snapshot []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	err := ioutil.WriteFile(ps.stateFile, state, 0700)
	err = ioutil.WriteFile(ps.snapshotFile, snapshot, 0700)
	return err
}

// read raft state from file
func (ps *Persist) ReadRaftState() ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return ioutil.ReadFile(ps.stateFile)
}

// read raft snapshot from file
func (ps *Persist) ReadSnapshot() ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return ioutil.ReadFile(ps.snapshotFile)
}

// state file size
func (ps *Persist) ReadStateSize() (int, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	data, err := ioutil.ReadFile(ps.stateFile)
	if err != nil {
		return -1, err
	}
	return len(data), nil
}

// snapshot file size
func (ps *Persist) ReadSnapshotSize() (int, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	data, err := ioutil.ReadFile(ps.snapshotFile)
	if err != nil {
		return -1, err
	}
	return len(data), nil
}

func (ps *Persist) Copy(i int) (*Persist, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	stateFileCopy, snapshotFileCopy := ps.stateFile+"_copy", ps.snapshotFile+"_copy"
	err := copyFile(ps.stateFile, stateFileCopy)
	err = copyFile(ps.snapshotFile, snapshotFileCopy)
	if err != nil {
		return nil, err
	}

	p := &Persist{
		mu:           sync.RWMutex{},
		peerId:       i,
		stateFile:    StateFile + strconv.Itoa(i),
		snapshotFile: SnapshotFile + strconv.Itoa(i),
	}
	return p, nil
}

func copyFile(src, dst string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()
	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}
