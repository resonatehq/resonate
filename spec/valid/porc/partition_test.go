package model

import "testing"

func TestHeartbeatIsPartitionedWithItsTasks(t *testing.T) {
	hb := Op{Kind: "task.heartbeat", PID: "w1",
		Refs: []TaskRef{{ID: "o1.x", Version: 1}, {ID: "o1.y", Version: 1}}}
	if got := partitionKey(hb); got != "o1" {
		t.Errorf("heartbeat filed under %q, want %q — its tasks live there", got, "o1")
	}
	if err := CheckPartitionable([]Op{hb}); err != nil {
		t.Errorf("single-origin heartbeat rejected: %v", err)
	}
}

func TestCrossOriginHeartbeatIsNotPartitionable(t *testing.T) {
	hb := Op{Kind: "task.heartbeat", PID: "w1",
		Refs: []TaskRef{{ID: "o1.x", Version: 1}, {ID: "o2.y", Version: 1}}}
	if err := CheckPartitionable([]Op{hb}); err == nil {
		t.Fatal("cross-origin heartbeat accepted; partitioning would be unsound")
	}
}
