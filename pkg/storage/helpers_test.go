// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// This file includes test-only helper methods added to types in
// package storage. These methods are only linked in to tests in this
// directory (but may be used from tests in both package storage and
// package storage_test).

package storage

import (
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// AddReplica adds the replica to the store's replica map and to the sorted
// replicasByKey slice. To be used only by unittests.
func (s *Store) AddReplica(repl *Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.addReplicaInternalLocked(repl); err != nil {
		return err
	}
	s.metrics.ReplicaCount.Inc(1)
	return nil
}

// ComputeMVCCStats immediately computes correct total MVCC usage statistics
// for the store, returning the computed values (but without modifying the
// store).
func (s *Store) ComputeMVCCStats() (enginepb.MVCCStats, error) {
	var totalStats enginepb.MVCCStats
	var err error

	now := s.Clock().PhysicalNow()
	newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
		var stats enginepb.MVCCStats
		stats, err = ComputeStatsForRange(r.Desc(), s.Engine(), now)
		if err != nil {
			return false
		}
		totalStats.Add(stats)
		return true
	})
	return totalStats, err
}

func forceScanAndProcess(s *Store, q *baseQueue) {
	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		q.MaybeAdd(repl, s.cfg.Clock.Now())
		return true
	})

	q.DrainQueue(s.cfg.Clock)
}

// ForceReplicationScanAndProcess iterates over all ranges and
// enqueues any that need to be replicated.
func (s *Store) ForceReplicationScanAndProcess() {
	forceScanAndProcess(s, s.replicateQueue.baseQueue)
}

// ForceReplicaGCScanAndProcess iterates over all ranges and enqueues any that
// may need to be GC'd.
func (s *Store) ForceReplicaGCScanAndProcess() {
	forceScanAndProcess(s, s.replicaGCQueue.baseQueue)
}

// ForceSplitScanAndProcess iterates over all ranges and enqueues any that
// may need to be split.
func (s *Store) ForceSplitScanAndProcess() {
	forceScanAndProcess(s, s.splitQueue.baseQueue)
}

// ForceRaftLogScanAndProcess iterates over all ranges and enqueues any that
// need their raft logs truncated and then process each of them.
func (s *Store) ForceRaftLogScanAndProcess() {
	forceScanAndProcess(s, s.raftLogQueue.baseQueue)
}

// ForceTimeSeriesMaintenanceQueueProcess iterates over all ranges, enqueuing
// any that need time series maintenance, then processes the time series
// maintenance queue.
func (s *Store) ForceTimeSeriesMaintenanceQueueProcess() {
	forceScanAndProcess(s, s.tsMaintenanceQueue.baseQueue)
}

// ForceRaftSnapshotQueueProcess iterates over all ranges, enqueuing
// any that need raft snapshots, then processes the raft snapshot
// queue.
func (s *Store) ForceRaftSnapshotQueueProcess() {
	forceScanAndProcess(s, s.raftSnapshotQueue.baseQueue)
}

// ConsistencyQueueShouldQueue invokes the shouldQueue method on the
// store's consistency queue.
func (s *Store) ConsistencyQueueShouldQueue(
	ctx context.Context, now hlc.Timestamp, r *Replica, cfg config.SystemConfig,
) (bool, float64) {
	return s.consistencyQueue.shouldQueue(ctx, now, r, cfg)
}

// GetDeadReplicas exports s.deadReplicas for tests.
func (s *Store) GetDeadReplicas() roachpb.StoreDeadReplicas {
	return s.deadReplicas()
}

// LogReplicaChangeTest adds a fake replica change event to the log for the
// range which contains the given key.
func (s *Store) LogReplicaChangeTest(
	ctx context.Context,
	txn *client.Txn,
	changeType roachpb.ReplicaChangeType,
	replica roachpb.ReplicaDescriptor,
	desc roachpb.RangeDescriptor,
	reason RangeLogEventReason,
	details string,
) error {
	return s.logChange(ctx, txn, changeType, replica, desc, reason, details)
}

// ReplicateQueuePurgatoryLength returns the number of replicas in replicate
// queue purgatory.
func (s *Store) ReplicateQueuePurgatoryLength() int {
	return s.replicateQueue.PurgatoryLength()
}

// SetRaftLogQueueActive enables or disables the raft log queue.
func (s *Store) SetRaftLogQueueActive(active bool) {
	s.setRaftLogQueueActive(active)
}

// SetReplicaGCQueueActive enables or disables the replica GC queue.
func (s *Store) SetReplicaGCQueueActive(active bool) {
	s.setReplicaGCQueueActive(active)
}

// SetSplitQueueActive enables or disables the split queue.
func (s *Store) SetSplitQueueActive(active bool) {
	s.setSplitQueueActive(active)
}

// SetRaftSnapshotQueueActive enables or disables the raft snapshot queue.
func (s *Store) SetRaftSnapshotQueueActive(active bool) {
	s.setRaftSnapshotQueueActive(active)
}

// SetReplicaScannerActive enables or disables the scanner. Note that while
// inactive, removals are still processed.
func (s *Store) SetReplicaScannerActive(active bool) {
	s.setScannerActive(active)
}

// SetSplitQueueProcessTimeout sets the timeout for processing a replica in the
// split queue.
func (s *Store) SetSplitQueueProcessTimeout(dur time.Duration) {
	s.splitQueue.SetProcessTimeout(dur)
}

// GetOrCreateReplica passes through to its lowercase sibling.
func (s *Store) GetOrCreateReplica(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	creatingReplica *roachpb.ReplicaDescriptor,
) (*Replica, bool, error) {
	return s.getOrCreateReplica(ctx, rangeID, replicaID, creatingReplica)
}

func (s *Store) SetRebalancesDisabled(v bool) {
	var i int32
	if v {
		i = 1
	}
	atomic.StoreInt32(&s.rebalancesDisabled, i)
}

// EnqueueRaftUpdateCheck enqueues the replica for a Raft update check, forcing
// the replica's Raft group into existence.
func (s *Store) EnqueueRaftUpdateCheck(rangeID roachpb.RangeID) {
	s.enqueueRaftUpdateCheck(rangeID)
}

func manualQueue(s *Store, q queueImpl, repl *Replica) error {
	cfg, ok := s.Gossip().GetSystemConfig()
	if !ok {
		return fmt.Errorf("%s: system config not yet available", s)
	}
	ctx := repl.AnnotateCtx(context.TODO())
	return q.process(ctx, repl, cfg)
}

// ManualGC processes the specified replica using the store's GC queue.
func (s *Store) ManualGC(repl *Replica) error {
	return manualQueue(s, s.gcQueue, repl)
}

// ManualReplicaGC processes the specified replica using the store's replica
// GC queue.
func (s *Store) ManualReplicaGC(repl *Replica) error {
	return manualQueue(s, s.replicaGCQueue, repl)
}

func (s *Store) ReservationCount() int {
	return len(s.snapshotApplySem)
}

func NewTestStorePool(cfg StoreConfig) *StorePool {
	TimeUntilStoreDead.Override(&cfg.Settings.SV, TestTimeUntilStoreDeadOff)
	return NewStorePool(
		cfg.AmbientCtx,
		cfg.Settings,
		cfg.Gossip,
		cfg.Clock,
		func(roachpb.NodeID, time.Time, time.Duration) nodeStatus {
			return nodeStatusLive
		},
		/* deterministic */ false,
	)
}

func (r *Replica) ReplicaIDLocked() roachpb.ReplicaID {
	return r.mu.replicaID
}

func (r *Replica) DescLocked() *roachpb.RangeDescriptor {
	return r.mu.state.Desc
}

// GetGCThreshold returns the range's GCThreshold, acquiring a replica lock in
// the process.
func (r *Replica) GetGCThreshold() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.GCThreshold
}

// GetTxnSpanGCThreshold returns the range's TxnSpanGCThreshold, acquiring a replica lock in
// the process.
func (r *Replica) GetTxnSpanGCThreshold() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.TxnSpanGCThreshold
}

func (r *Replica) AssertState(ctx context.Context, reader engine.Reader) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.assertStateLocked(ctx, reader)
}

func (r *Replica) RaftLock() {
	r.raftMu.Lock()
}

func (r *Replica) RaftUnlock() {
	r.raftMu.Unlock()
}

// GetLastIndex is the same function as LastIndex but it does not require
// that the replica lock is held.
func (r *Replica) GetLastIndex() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.raftLastIndexLocked()
}

// GetLease exposes replica.getLease for tests.
// If you just need information about the lease holder, consider issuing a
// LeaseInfoRequest instead of using this internal method.
func (r *Replica) GetLease() (roachpb.Lease, *roachpb.Lease) {
	return r.getLease()
}

// SetQuotaPool allows the caller to set a replica's quota pool initialized to
// a given quota. Additionally it initializes the replica's quota release queue
// and its command sizes map. Only safe to call on the replica that is both
// lease holder and raft leader.
func (r *Replica) InitQuotaPool(quota int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.mu.proposalQuotaBaseIndex = r.mu.lastIndex
	if r.mu.proposalQuota != nil {
		r.mu.proposalQuota.close()
	}
	r.mu.proposalQuota = newQuotaPool(quota)
	r.mu.quotaReleaseQueue = nil
	r.mu.commandSizes = make(map[storagebase.CmdIDKey]int)
}

// QuotaAvailable returns the quota available in the replica's quota pool. Only
// safe to call on the replica that is both lease holder and raft leader.
func (r *Replica) QuotaAvailable() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.proposalQuota.approximateQuota()
}

func (r *Replica) QuotaReleaseQueueLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.quotaReleaseQueue)
}

func (r *Replica) CommandSizesLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.commandSizes)
}

// GetTimestampCacheLowWater returns the timestamp cache low water mark.
func (r *Replica) GetTimestampCacheLowWater() hlc.Timestamp {
	r.store.tsCacheMu.Lock()
	defer r.store.tsCacheMu.Unlock()
	t := r.store.tsCacheMu.cache.lowWater
	// Bump the per-Store low-water mark using the per-range read and write info.
	start := roachpb.Key(r.Desc().StartKey)
	end := roachpb.Key(r.Desc().EndKey)
	if r, _, ok := r.store.tsCacheMu.cache.GetMaxRead(start, end); !ok && t.Less(r) {
		t = r
	}
	if w, _, ok := r.store.tsCacheMu.cache.GetMaxWrite(start, end); !ok && t.Less(w) {
		t = w
	}
	return t
}

// PermittingLargeSnapshots returns whether the replica is permitting large
// snapshots.
func (r *Replica) PermittingLargeSnapshots() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.permitLargeSnapshots
}

// GetRaftLogSize returns the raft log size.
func (r *Replica) GetRaftLogSize() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.raftLogSize
}

func (r *Replica) IsRaftGroupInitialized() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.internalRaftGroup != nil
}

// HasQuorum returns true iff the range that this replica is part of
// can achieve quorum.
func (r *Replica) HasQuorum() bool {
	desc := r.Desc()
	liveReplicas, _ := r.store.allocator.storePool.liveAndDeadReplicas(desc.RangeID, desc.Replicas)
	quorum := computeQuorum(len(desc.Replicas))
	return len(liveReplicas) >= quorum
}

// GetStoreList exposes getStoreList for testing only, but with a hardcoded
// storeFilter of storeFilterNone.
func (sp *StorePool) GetStoreList(rangeID roachpb.RangeID) (StoreList, int, int) {
	return sp.getStoreList(rangeID, storeFilterNone)
}

// Stores returns a copy of sl.stores.
func (sl *StoreList) Stores() []roachpb.StoreDescriptor {
	stores := make([]roachpb.StoreDescriptor, len(sl.stores))
	copy(stores, sl.stores)
	return stores
}

const (
	sideloadBogusIndex = 12345
	sideloadBogusTerm  = 67890
)

func (r *Replica) PutBogusSideloadedData() {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	if err := r.raftMu.sideloaded.PutIfNotExists(context.Background(), sideloadBogusIndex, sideloadBogusTerm, []byte("bogus")); err != nil {
		panic(err)
	}
}

func (r *Replica) HasBogusSideloadedData() bool {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	if _, err := r.raftMu.sideloaded.Get(context.Background(), sideloadBogusIndex, sideloadBogusTerm); err == errSideloadedFileNotFound {
		return false
	} else if err != nil {
		panic(err)
	}
	return true
}

func MakeSSTable(key, value string, ts hlc.Timestamp) ([]byte, engine.MVCCKeyValue) {
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		panic(err)
	}
	defer sst.Close()

	v := roachpb.MakeValueFromBytes([]byte(value))
	v.InitChecksum([]byte(key))

	kv := engine.MVCCKeyValue{
		Key: engine.MVCCKey{
			Key:       []byte(key),
			Timestamp: ts,
		},
		Value: v.RawBytes,
	}

	if err := sst.Add(kv); err != nil {
		panic(errors.Wrap(err, "while finishing SSTable"))
	}
	b, err := sst.Finish()
	if err != nil {
		panic(errors.Wrap(err, "while finishing SSTable"))
	}
	return b, kv
}

func ProposeAddSSTable(ctx context.Context, key, val string, ts hlc.Timestamp, store *Store) error {
	var ba roachpb.BatchRequest
	ba.RangeID = store.LookupReplica(roachpb.RKey(key), nil).RangeID

	var addReq roachpb.AddSSTableRequest
	addReq.Data, _ = MakeSSTable(key, val, ts)
	addReq.Key = roachpb.Key(key)
	addReq.EndKey = addReq.Key.Next()
	ba.Add(&addReq)

	_, pErr := store.Send(ctx, ba)
	if pErr != nil {
		return pErr.GoError()
	}
	return nil
}

func SetMockAddSSTable() (undo func()) {
	prev := commands[roachpb.AddSSTable]

	// TODO(tschottdorf): this already does nontrivial work. Worth open-sourcing the relevant
	// subparts of the real evalAddSSTable to make this test less likely to rot.
	evalAddSSTable := func(
		ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, _ roachpb.Response,
	) (EvalResult, error) {
		log.Event(ctx, "evaluated testing-only AddSSTable mock")
		args := cArgs.Args.(*roachpb.AddSSTableRequest)

		return EvalResult{
			Replicated: storagebase.ReplicatedEvalResult{
				AddSSTable: &storagebase.ReplicatedEvalResult_AddSSTable{
					Data:  args.Data,
					CRC32: util.CRC32(args.Data),
				},
			},
		}, nil
	}

	SetAddSSTableCmd(Command{
		DeclareKeys: DefaultDeclareKeys,
		Eval:        evalAddSSTable,
	})
	return func() {
		SetAddSSTableCmd(prev)
	}
}

// IsQuiescent returns whether the replica is quiescent or not.
func (r *Replica) IsQuiescent() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.quiescent
}

func (r *Replica) IsPushTxnQueueEnabled() bool {
	return r.pushTxnQueue.isEnabled()
}

// GetQueueLastProcessed returns the last processed timestamp for the
// specified queue, or the zero timestamp if not available.
func (r *Replica) GetQueueLastProcessed(ctx context.Context, queue string) (hlc.Timestamp, error) {
	return r.getQueueLastProcessed(ctx, queue)
}

func (r *Replica) RaftTransferLeader(ctx context.Context, target roachpb.ReplicaID) {
	r.maybeTransferRaftLeadership(ctx, target)
}

func GetGCQueueTxnCleanupThreshold() time.Duration {
	return txnCleanupThreshold
}

func (nl *NodeLiveness) SetDrainingInternal(
	ctx context.Context, liveness *Liveness, drain bool,
) error {
	return nl.setDrainingInternal(ctx, liveness, drain)
}

func (nl *NodeLiveness) SetDecommissioningInternal(
	ctx context.Context, nodeID roachpb.NodeID, liveness *Liveness, decommission bool,
) error {
	return nl.setDecommissioningInternal(ctx, nodeID, liveness, decommission)
}
