package model

import "testing"

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

func TestCrossOriginHeartbeatIsRejectedAndSoInert(t *testing.T) {
	hb := Op{Kind: "task.heartbeat", PID: "w1",
		Refs: []TaskRef{{ID: "o1:x", Version: 1}, {ID: "o2:y", Version: 1}}}

	s := &ServerState{}
	if got := s.TaskHeartbeat(Projected, hb.PID, hb.Refs, 1000).Status; got != 400 {
		t.Fatalf("cross-origin heartbeat answered %d, want 400", got)
	}
	if err := CheckPartitionable([]Op{hb}); err != nil {
		t.Fatalf("rejected heartbeat treated as a partitioning hazard: %v", err)
	}
}

func TestCrossOriginHeartbeatWouldBeUnsoundIfAccepted(t *testing.T) {
	hb := Op{Kind: "task.heartbeat", PID: "w1",
		Refs: []TaskRef{{ID: "o1:x", Version: 1}, {ID: "o2:y", Version: 1}}}
	if rejectedBySchema(hb) != true {
		t.Fatal("the guard now relies on this op being rejected; it is not")
	}
}
