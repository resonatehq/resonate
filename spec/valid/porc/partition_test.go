package model

import "testing"

// A heartbeat carries no top-level id — its subject is the ref list — so
// keying the partition on `Op.ID` filed every one of them under `""`,
// away from the tasks whose leases they extend. The extension is then
// lost in the owning partition, taskLeaseTimeout becomes enabled earlier than the
// specification allows, and since firing an internal step is a CHOICE that only
// adds candidates, the partitioned replay accepts a superset. A false
// accept is the one direction a checker must never fail in.
func TestHeartbeatIsPartitionedWithItsTasks(t *testing.T) {
	hb := Op{Kind: "task.heartbeat", PID: "w1",
		Refs: []TaskRef{{ID: "o1:x", Version: 1}, {ID: "o1:y", Version: 1}}}
	if got := partitionKey(hb); got != "o1" {
		t.Errorf("heartbeat filed under %q, want %q — its tasks live there", got, "o1")
	}
	if err := CheckPartitionable([]Op{hb}); err != nil {
		t.Errorf("single-origin heartbeat rejected: %v", err)
	}
}

// A heartbeat spanning two origins belongs to both partitions at once,
// which is not representable. Guessing one would silently drop the
// extension for the tasks in the other, so the split must be refused.
func TestCrossOriginHeartbeatIsNotPartitionable(t *testing.T) {
	hb := Op{Kind: "task.heartbeat", PID: "w1",
		Refs: []TaskRef{{ID: "o1:x", Version: 1}, {ID: "o2:y", Version: 1}}}
	if err := CheckPartitionable([]Op{hb}); err == nil {
		t.Fatal("cross-origin heartbeat accepted; partitioning would be unsound")
	}
}
