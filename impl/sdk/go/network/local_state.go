package network

import (
	"encoding/json"
	"fmt"
	"strconv"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

// pendingRetryTTL is how long a pending task waits before being re-dispatched
// by the ticker.
const pendingRetryTTL int64 = 30_000

// outgoingMsg is a server-state-machine push that LocalNetwork delivers to
// subscribers after a request is processed. Address is informational —
// LocalNetwork fans out to all subscribers regardless.
type outgoingMsg struct {
	address string
	message map[string]any
}

// durablePromise is the in-memory promise record. Param and Value are stored
// as json.RawMessage so they're round-tripped without decoding.
type durablePromise struct {
	ID          string
	State       resonate.PromiseState
	Param       json.RawMessage
	Value       json.RawMessage
	Tags        map[string]string
	TimeoutAt   int64
	CreatedAt   int64
	SettledAt   *int64
	Awaiters    map[string]struct{}
	Subscribers map[string]struct{}
}

func (p *durablePromise) toRecord() map[string]any {
	rec := map[string]any{
		"id":        p.ID,
		"state":     string(p.State),
		"param":     rawOrNull(p.Param),
		"value":     rawOrNull(p.Value),
		"tags":      tagsOrEmpty(p.Tags),
		"timeoutAt": p.TimeoutAt,
		"createdAt": p.CreatedAt,
	}
	if p.SettledAt != nil {
		rec["settledAt"] = *p.SettledAt
	}
	return rec
}

// task is the in-memory task record.
type task struct {
	ID      string
	State   resonate.TaskState
	Version int64
	PID     *string
	TTL     *int64
	Resumes map[string]struct{}
}

func (t *task) toRecord() map[string]any {
	rec := map[string]any{
		"id":        t.ID,
		"state":     string(t.State),
		"version":   t.Version,
		"promiseId": t.ID,
	}
	if t.PID != nil {
		rec["pid"] = *t.PID
	}
	if t.TTL != nil {
		rec["ttl"] = *t.TTL
	}
	return rec
}

// pTimeout / tTimeout track promise/task timeout entries scheduled for the
// ticker. tType 0 = pending-retry, 1 = lease.
type pTimeout struct {
	id      string
	timeout int64
}

type tTimeout struct {
	id      string
	tType   uint8
	timeout int64
}

// scheduleStub is the in-memory schedule record. Schedules are not actually
// triggered in local mode — they only support CRUD.
type scheduleStub struct {
	ID             string
	Cron           string
	PromiseID      string
	PromiseTimeout int64
	PromiseParam   json.RawMessage
	PromiseTags    json.RawMessage
	CreatedAt      int64
}

func (s *scheduleStub) toRecord() map[string]any {
	return map[string]any{
		"id":             s.ID,
		"cron":           s.Cron,
		"promiseId":      s.PromiseID,
		"promiseTimeout": s.PromiseTimeout,
		"promiseParam":   rawOrNull(s.PromiseParam),
		"promiseTags":    rawOrObject(s.PromiseTags),
		"createdAt":      s.CreatedAt,
		"nextRunAt":      int64(0),
		"lastRunAt":      nil,
	}
}

// serverState holds the entire in-memory state of LocalNetwork. It is owned
// by a single goroutine (the actor) and has no internal locking.
type serverState struct {
	promises   map[string]*durablePromise
	tasks      map[string]*task
	schedules  map[string]*scheduleStub
	pTimeouts  []pTimeout
	tTimeouts  []tTimeout
	outgoing   []outgoingMsg
}

func newServerState() *serverState {
	return &serverState{
		promises:  map[string]*durablePromise{},
		tasks:     map[string]*task{},
		schedules: map[string]*scheduleStub{},
	}
}

// apply dispatches a single (flat) request to the matching handler. The
// returned outgoing slice is non-empty when the handler queued push messages
// that should be fanned out to subscribers after the reply is sent.
func (s *serverState) apply(now int64, req map[string]any) (map[string]any, []outgoingMsg, error) {
	s.outgoing = s.outgoing[:0]

	kind, _ := req["kind"].(string)
	corrID := req["corrId"]

	// Auto-timeout pass before processing.
	switch kind {
	case "promise.get", "promise.create", "promise.settle":
		if id, ok := req["id"].(string); ok {
			s.tryAutoTimeout(now, id)
		}
	case "promise.register_listener":
		if id, ok := req["awaited"].(string); ok {
			s.tryAutoTimeout(now, id)
		}
	case "task.create":
		if action, ok := req["action"]; ok {
			pd, _ := extractActionData(action).(map[string]any)
			if id, ok := pd["id"].(string); ok {
				s.tryAutoTimeout(now, id)
			}
		}
	case "task.acquire", "task.release", "task.fulfill":
		if id, ok := req["id"].(string); ok {
			s.tryAutoTimeout(now, id)
		}
	case "task.fence":
		if id, ok := req["id"].(string); ok {
			s.tryAutoTimeout(now, id)
		}
		if action, ok := req["action"]; ok {
			ad, _ := extractActionData(action).(map[string]any)
			if id, ok := ad["id"].(string); ok {
				s.tryAutoTimeout(now, id)
			}
		}
	case "task.suspend":
		if id, ok := req["id"].(string); ok {
			s.tryAutoTimeout(now, id)
		}
		if actions, ok := req["actions"].([]any); ok {
			for _, a := range actions {
				ad, _ := extractActionData(a).(map[string]any)
				if awaited, ok := ad["awaited"].(string); ok && awaited != "" {
					s.tryAutoTimeout(now, awaited)
				}
			}
		}
	}

	var (
		resp map[string]any
		err  error
	)
	switch kind {
	case "promise.get":
		resp, err = s.promiseGet(corrID, req)
	case "promise.create":
		resp, err = s.promiseCreate(now, corrID, req)
	case "promise.settle":
		resp, err = s.promiseSettle(now, corrID, req)
	case "promise.register_listener":
		resp, err = s.promiseRegisterListener(corrID, req)
	case "task.create":
		resp, err = s.taskCreate(now, corrID, req)
	case "task.acquire":
		resp, err = s.taskAcquire(now, corrID, req)
	case "task.release":
		resp, err = s.taskRelease(now, corrID, req)
	case "task.fulfill":
		resp, err = s.taskFulfill(now, corrID, req)
	case "task.fence":
		resp, err = s.taskFence(now, corrID, req)
	case "task.suspend":
		resp, err = s.taskSuspend(corrID, req)
	case "task.heartbeat":
		resp, err = s.taskHeartbeat(now, corrID, req)
	case "schedule.create":
		resp, err = s.scheduleCreate(now, corrID, req)
	case "schedule.get":
		resp, err = s.scheduleGet(corrID, req)
	case "schedule.delete":
		resp, err = s.scheduleDelete(corrID, req)
	case "schedule.search":
		resp, err = s.scheduleSearch(corrID, req)
	default:
		err = &resonate.ServerError{Code: 400, Message: "unknown request kind: " + kind}
	}
	if err != nil {
		return nil, nil, err
	}
	var out []outgoingMsg
	if len(s.outgoing) > 0 {
		out = append([]outgoingMsg(nil), s.outgoing...)
		s.outgoing = s.outgoing[:0]
	}
	return resp, out, nil
}

// tick runs scheduled timeouts: promise expiries, lease expiries, pending
// retries. Settling, fulfilling, awaiter resume, and lease retry are ordered
// in separate passes so that side-effects observe consistent state.
func (s *serverState) tick(now int64) []outgoingMsg {
	s.outgoing = s.outgoing[:0]

	var promiseSettles []string
	var taskReleases []struct {
		id      string
		version int64
	}
	var taskRetries []struct {
		id      string
		version int64
	}

	for _, pt := range s.pTimeouts {
		if now < pt.timeout {
			continue
		}
		if p, ok := s.promises[pt.id]; ok && p.State == resonate.PromiseStatePending {
			promiseSettles = append(promiseSettles, pt.id)
		}
	}
	for _, tt := range s.tTimeouts {
		if now < tt.timeout {
			continue
		}
		t, ok := s.tasks[tt.id]
		if !ok {
			continue
		}
		if tt.tType == 1 && t.State == resonate.TaskStateAcquired {
			taskReleases = append(taskReleases, struct {
				id      string
				version int64
			}{tt.id, t.Version})
		} else if tt.tType == 0 && t.State == resonate.TaskStatePending {
			taskRetries = append(taskRetries, struct {
				id      string
				version int64
			}{tt.id, t.Version})
		}
	}

	for _, id := range promiseSettles {
		p, ok := s.promises[id]
		if !ok || p.State != resonate.PromiseStatePending {
			continue
		}
		st := s.timeoutState(p.Tags)
		p.State = st
		p.Value = nil
		t := p.TimeoutAt
		p.SettledAt = &t
		s.delPTimeout(id)
	}
	for _, id := range promiseSettles {
		s.enqueueSettle(id)
	}
	for _, id := range promiseSettles {
		s.resumeAwaiters(id, now)
		s.notifySubscribers(id)
	}

	for _, r := range taskReleases {
		t, ok := s.tasks[r.id]
		if !ok || t.State != resonate.TaskStateAcquired || t.Version != r.version {
			continue
		}
		newVersion := t.Version + 1
		t.State = resonate.TaskStatePending
		t.Version = newVersion
		t.PID = nil
		t.TTL = nil
		s.setTTimeout(r.id, 0, now+pendingRetryTTL)
		if p, ok := s.promises[r.id]; ok {
			if addr, ok := p.Tags["resonate:target"]; ok {
				s.sendExecuteMessage(addr, r.id, newVersion)
			}
		}
	}

	for _, r := range taskRetries {
		t, ok := s.tasks[r.id]
		if !ok || t.State != resonate.TaskStatePending {
			continue
		}
		s.setTTimeout(r.id, 0, now+pendingRetryTTL)
		if p, ok := s.promises[r.id]; ok {
			if addr, ok := p.Tags["resonate:target"]; ok {
				s.sendExecuteMessage(addr, r.id, t.Version)
			}
		}
	}

	if len(s.outgoing) == 0 {
		return nil
	}
	out := append([]outgoingMsg(nil), s.outgoing...)
	s.outgoing = s.outgoing[:0]
	return out
}

// ──────────────────────────────────────────────────────────────────────────
// Promise operations
// ──────────────────────────────────────────────────────────────────────────

func (s *serverState) promiseGet(corrID any, req map[string]any) (map[string]any, error) {
	id, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	p, ok := s.promises[id]
	if !ok {
		return map[string]any{
			"kind":   "promise.get",
			"corrId": corrID,
			"status": 404,
			"error":  fmt.Sprintf("promise %s not found", id),
		}, nil
	}
	return map[string]any{
		"kind":    "promise.get",
		"corrId":  corrID,
		"status":  200,
		"promise": p.toRecord(),
	}, nil
}

func (s *serverState) promiseCreate(now int64, corrID any, req map[string]any) (map[string]any, error) {
	id, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	if existing, ok := s.promises[id]; ok {
		return map[string]any{
			"kind":    "promise.create",
			"corrId":  corrID,
			"status":  200,
			"promise": existing.toRecord(),
		}, nil
	}
	timeoutAt := getInt64(req, "timeoutAt", int64(1)<<62)
	paramRaw := rawFromAny(req["param"])
	tags := extractTags(req)

	if now >= timeoutAt {
		state := s.timeoutState(tags)
		p := &durablePromise{
			ID:          id,
			State:       state,
			Param:       paramRaw,
			Tags:        tags,
			TimeoutAt:   timeoutAt,
			CreatedAt:   timeoutAt,
			SettledAt:   &timeoutAt,
			Awaiters:    map[string]struct{}{},
			Subscribers: map[string]struct{}{},
		}
		rec := p.toRecord()
		s.promises[id] = p
		s.enqueueSettle(id)
		s.resumeAwaiters(id, now)
		s.notifySubscribers(id)
		return map[string]any{
			"kind":    "promise.create",
			"corrId":  corrID,
			"status":  200,
			"promise": rec,
		}, nil
	}

	p := &durablePromise{
		ID:          id,
		State:       resonate.PromiseStatePending,
		Param:       paramRaw,
		Tags:        tags,
		TimeoutAt:   timeoutAt,
		CreatedAt:   now,
		Awaiters:    map[string]struct{}{},
		Subscribers: map[string]struct{}{},
	}
	rec := p.toRecord()
	s.promises[id] = p
	s.setPTimeout(id, timeoutAt)

	if addr, ok := tags["resonate:target"]; ok {
		var delay int64
		hasDelay := false
		if d, ok := tags["resonate:delay"]; ok {
			if parsed, ok := parseInt64(d); ok {
				delay = parsed
				hasDelay = true
			}
		}
		deferred := hasDelay && now < delay

		t := &task{
			ID:      id,
			State:   resonate.TaskStatePending,
			Version: 0,
			Resumes: map[string]struct{}{},
		}
		s.tasks[id] = t
		nextTimeout := now + pendingRetryTTL
		if deferred {
			nextTimeout = delay
		}
		s.setTTimeout(id, 0, nextTimeout)
		if !deferred {
			s.sendExecuteMessage(addr, id, 0)
		}
	}

	return map[string]any{
		"kind":    "promise.create",
		"corrId":  corrID,
		"status":  201,
		"promise": rec,
	}, nil
}

func (s *serverState) promiseSettle(now int64, corrID any, req map[string]any) (map[string]any, error) {
	id, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	settleState := resonate.PromiseStateResolved
	if v, ok := req["state"].(string); ok {
		settleState = resonate.PromiseState(v)
	}
	valueRaw := rawFromAny(req["value"])

	p, ok := s.promises[id]
	if !ok {
		return map[string]any{
			"kind":   "promise.settle",
			"corrId": corrID,
			"status": 404,
			"error":  fmt.Sprintf("promise %s not found", id),
		}, nil
	}
	if p.State != resonate.PromiseStatePending {
		return map[string]any{
			"kind":    "promise.settle",
			"corrId":  corrID,
			"status":  200,
			"promise": p.toRecord(),
		}, nil
	}
	p.State = settleState
	p.Value = valueRaw
	nowCopy := now
	p.SettledAt = &nowCopy
	rec := p.toRecord()
	s.delPTimeout(id)
	s.enqueueSettle(id)
	s.resumeAwaiters(id, now)
	s.notifySubscribers(id)

	return map[string]any{
		"kind":    "promise.settle",
		"corrId":  corrID,
		"status":  200,
		"promise": rec,
	}, nil
}

func (s *serverState) promiseRegisterListener(corrID any, req map[string]any) (map[string]any, error) {
	awaited, err := requireStr(req, "awaited")
	if err != nil {
		return nil, err
	}
	address, err := requireStr(req, "address")
	if err != nil {
		return nil, err
	}
	p, ok := s.promises[awaited]
	if !ok {
		return map[string]any{
			"kind":   "promise.register_listener",
			"corrId": corrID,
			"status": 404,
			"error":  fmt.Sprintf("promise %s not found", awaited),
		}, nil
	}
	if p.State == resonate.PromiseStatePending {
		p.Subscribers[address] = struct{}{}
	}
	return map[string]any{
		"kind":    "promise.register_listener",
		"corrId":  corrID,
		"status":  200,
		"promise": p.toRecord(),
	}, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Task operations
// ──────────────────────────────────────────────────────────────────────────

func (s *serverState) taskCreate(now int64, corrID any, req map[string]any) (map[string]any, error) {
	pid, err := requireStr(req, "pid")
	if err != nil {
		return nil, err
	}
	ttl := getInt64(req, "ttl", 60_000)
	actionRaw := req["action"]
	promiseReq, _ := extractActionData(actionRaw).(map[string]any)
	promiseID, err := requireStr(promiseReq, "id")
	if err != nil {
		return nil, err
	}

	if existing, ok := s.tasks[promiseID]; ok {
		state := existing.State
		var pRecord any
		if p, ok := s.promises[promiseID]; ok {
			pRecord = p.toRecord()
		}
		switch state {
		case resonate.TaskStatePending:
			preload := s.preload(promiseID)
			existing.State = resonate.TaskStateAcquired
			pidCopy := pid
			ttlCopy := ttl
			existing.PID = &pidCopy
			existing.TTL = &ttlCopy
			existing.Resumes = map[string]struct{}{}
			s.setTTimeout(promiseID, 1, now+ttl)
			return map[string]any{
				"kind":    "task.create",
				"corrId":  corrID,
				"status":  200,
				"task":    existing.toRecord(),
				"promise": pRecord,
				"preload": preload,
			}, nil
		case resonate.TaskStateFulfilled:
			preload := s.preload(promiseID)
			return map[string]any{
				"kind":    "task.create",
				"corrId":  corrID,
				"status":  200,
				"task":    existing.toRecord(),
				"promise": pRecord,
				"preload": preload,
			}, nil
		default:
			return map[string]any{
				"kind":   "task.create",
				"corrId": corrID,
				"status": 409,
				"error":  "Already exists",
			}, nil
		}
	}

	if _, ok := s.promises[promiseID]; ok {
		return map[string]any{
			"kind":   "task.create",
			"corrId": corrID,
			"status": 409,
			"error":  "Already exists",
		}, nil
	}

	timeoutAt := getInt64(promiseReq, "timeoutAt", int64(1)<<62)
	paramRaw := rawFromAny(promiseReq["param"])
	tags := extractTags(promiseReq)

	if now >= timeoutAt {
		state := s.timeoutState(tags)
		p := &durablePromise{
			ID: promiseID, State: state, Param: paramRaw, Tags: tags,
			TimeoutAt: timeoutAt, CreatedAt: timeoutAt, SettledAt: &timeoutAt,
			Awaiters: map[string]struct{}{}, Subscribers: map[string]struct{}{},
		}
		pr := p.toRecord()
		s.promises[promiseID] = p
		t := &task{ID: promiseID, State: resonate.TaskStateFulfilled, Resumes: map[string]struct{}{}}
		tr := t.toRecord()
		s.tasks[promiseID] = t
		return map[string]any{
			"kind":    "task.create",
			"corrId":  corrID,
			"status":  200,
			"task":    tr,
			"promise": pr,
			"preload": []any{},
		}, nil
	}

	p := &durablePromise{
		ID: promiseID, State: resonate.PromiseStatePending, Param: paramRaw, Tags: tags,
		TimeoutAt: timeoutAt, CreatedAt: now,
		Awaiters: map[string]struct{}{}, Subscribers: map[string]struct{}{},
	}
	pr := p.toRecord()
	s.promises[promiseID] = p
	s.setPTimeout(promiseID, timeoutAt)

	pidCopy := pid
	ttlCopy := ttl
	t := &task{
		ID: promiseID, State: resonate.TaskStateAcquired, Version: 0,
		PID: &pidCopy, TTL: &ttlCopy, Resumes: map[string]struct{}{},
	}
	tr := t.toRecord()
	s.tasks[promiseID] = t
	s.setTTimeout(promiseID, 1, now+ttl)

	preload := s.preload(promiseID)
	return map[string]any{
		"kind":    "task.create",
		"corrId":  corrID,
		"status":  201,
		"task":    tr,
		"promise": pr,
		"preload": preload,
	}, nil
}

func (s *serverState) taskAcquire(now int64, corrID any, req map[string]any) (map[string]any, error) {
	taskID, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	pid, _ := req["pid"].(string)
	ttl := getInt64(req, "ttl", 60_000)
	if ttl < 1 {
		ttl = 1
	}
	t, ok := s.tasks[taskID]
	if !ok {
		return map[string]any{
			"kind":   "task.acquire",
			"corrId": corrID,
			"status": 404,
			"error":  fmt.Sprintf("task %s not found", taskID),
		}, nil
	}
	if t.State != resonate.TaskStatePending {
		return map[string]any{
			"kind":   "task.acquire",
			"corrId": corrID,
			"status": 409,
			"error":  fmt.Sprintf("task not in pending state (state: %s)", t.State),
		}, nil
	}
	preload := s.preload(taskID)
	pidCopy := pid
	ttlCopy := ttl
	t.State = resonate.TaskStateAcquired
	t.PID = &pidCopy
	t.TTL = &ttlCopy
	t.Resumes = map[string]struct{}{}
	s.setTTimeout(taskID, 1, now+ttl)
	var pRecord any
	if p, ok := s.promises[taskID]; ok {
		pRecord = p.toRecord()
	}
	return map[string]any{
		"kind":    "task.acquire",
		"corrId":  corrID,
		"status":  200,
		"task":    t.toRecord(),
		"promise": pRecord,
		"preload": preload,
	}, nil
}

func (s *serverState) taskRelease(now int64, corrID any, req map[string]any) (map[string]any, error) {
	taskID, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	t, ok := s.tasks[taskID]
	if !ok {
		return map[string]any{"kind": "task.release", "corrId": corrID, "status": 404}, nil
	}
	if t.State != resonate.TaskStateAcquired {
		return map[string]any{"kind": "task.release", "corrId": corrID, "status": 409}, nil
	}
	newVersion := t.Version + 1
	t.State = resonate.TaskStatePending
	t.Version = newVersion
	t.PID = nil
	t.TTL = nil
	s.setTTimeout(taskID, 0, now+pendingRetryTTL)
	if p, ok := s.promises[taskID]; ok {
		if addr, ok := p.Tags["resonate:target"]; ok {
			s.sendExecuteMessage(addr, taskID, newVersion)
		}
	}
	return map[string]any{"kind": "task.release", "corrId": corrID, "status": 200}, nil
}

func (s *serverState) taskFulfill(now int64, corrID any, req map[string]any) (map[string]any, error) {
	taskID, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	t, ok := s.tasks[taskID]
	if !ok {
		return map[string]any{"kind": "task.fulfill", "corrId": corrID, "status": 404}, nil
	}
	if t.State != resonate.TaskStateAcquired {
		return map[string]any{"kind": "task.fulfill", "corrId": corrID, "status": 409}, nil
	}

	actionRaw := req["action"]
	settle, _ := extractActionData(actionRaw).(map[string]any)
	promiseID, _ := settle["id"].(string)
	if promiseID == "" {
		promiseID = taskID
	}
	settleState := resonate.PromiseStateResolved
	if v, ok := settle["state"].(string); ok {
		settleState = resonate.PromiseState(v)
	}
	valueRaw := rawFromAny(settle["value"])

	if p, ok := s.promises[promiseID]; ok && p.State == resonate.PromiseStatePending {
		p.State = settleState
		p.Value = valueRaw
		nowCopy := now
		p.SettledAt = &nowCopy
		s.delPTimeout(promiseID)
	}

	var pRecord any
	if p, ok := s.promises[promiseID]; ok {
		pRecord = p.toRecord()
	}
	s.enqueueSettle(taskID)
	s.resumeAwaiters(promiseID, now)
	s.notifySubscribers(promiseID)
	return map[string]any{
		"kind":    "task.fulfill",
		"corrId":  corrID,
		"status":  200,
		"promise": pRecord,
	}, nil
}

// taskFence runs a promise.create or promise.settle action gated on the
// task's lease (id, version). On lease mismatch or missing task, returns
// 404/409 without applying the action.
func (s *serverState) taskFence(now int64, corrID any, req map[string]any) (map[string]any, error) {
	taskID, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	version, _ := toInt64(req["version"])

	t, ok := s.tasks[taskID]
	if !ok {
		return map[string]any{"kind": "task.fence", "corrId": corrID, "status": 404}, nil
	}
	if t.State != resonate.TaskStateAcquired || t.Version != version {
		return map[string]any{"kind": "task.fence", "corrId": corrID, "status": 409}, nil
	}

	actionMap, _ := req["action"].(map[string]any)
	subKind, _ := actionMap["kind"].(string)
	actionData, _ := extractActionData(req["action"]).(map[string]any)

	flat := map[string]any{}
	for k, v := range actionData {
		flat[k] = v
	}

	var inner map[string]any
	switch subKind {
	case "promise.create":
		flat["kind"] = "promise.create"
		inner, err = s.promiseCreate(now, corrID, flat)
	case "promise.settle":
		flat["kind"] = "promise.settle"
		inner, err = s.promiseSettle(now, corrID, flat)
	default:
		return nil, &resonate.ServerError{Code: 400, Message: "unknown fence action kind: " + subKind}
	}
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"kind":   "task.fence",
		"corrId": corrID,
		"status": 200,
		"action": map[string]any{
			"data": map[string]any{"promise": inner["promise"]},
		},
		"preload": s.preload(taskID),
	}, nil
}

func (s *serverState) taskSuspend(corrID any, req map[string]any) (map[string]any, error) {
	taskID, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	t, ok := s.tasks[taskID]
	if !ok {
		return map[string]any{"kind": "task.suspend", "corrId": corrID, "status": 404}, nil
	}
	if t.State != resonate.TaskStateAcquired {
		return map[string]any{"kind": "task.suspend", "corrId": corrID, "status": 409}, nil
	}

	if len(t.Resumes) > 0 {
		t.Resumes = map[string]struct{}{}
		return map[string]any{
			"kind":     "task.suspend",
			"corrId":   corrID,
			"status":   300,
			"redirect": true,
			"preload":  s.preload(taskID),
		}, nil
	}

	var callbacks []string
	if actions, ok := req["actions"].([]any); ok {
		for _, a := range actions {
			ad, _ := extractActionData(a).(map[string]any)
			if awaited, ok := ad["awaited"].(string); ok && awaited != "" {
				callbacks = append(callbacks, awaited)
			}
		}
	}

	anySettled := false
	for _, awaited := range callbacks {
		p, ok := s.promises[awaited]
		if !ok {
			continue
		}
		if p.State == resonate.PromiseStatePending {
			p.Awaiters[taskID] = struct{}{}
		} else {
			anySettled = true
		}
	}

	if anySettled {
		return map[string]any{
			"kind":     "task.suspend",
			"corrId":   corrID,
			"status":   300,
			"redirect": true,
			"preload":  s.preload(taskID),
		}, nil
	}

	t.State = resonate.TaskStateSuspended
	t.PID = nil
	t.TTL = nil
	t.Resumes = map[string]struct{}{}
	s.delTTimeout(taskID)
	return map[string]any{"kind": "task.suspend", "corrId": corrID, "status": 200}, nil
}

func (s *serverState) taskHeartbeat(now int64, corrID any, req map[string]any) (map[string]any, error) {
	pid, err := requireStr(req, "pid")
	if err != nil {
		return nil, err
	}
	tasks, _ := req["tasks"].([]any)
	for _, raw := range tasks {
		ref, _ := raw.(map[string]any)
		id, _ := ref["id"].(string)
		if id == "" {
			return nil, &resonate.ServerError{Code: 400, Message: "missing or empty required field: id"}
		}
		var versionPtr *int64
		if vv, ok := ref["version"]; ok {
			if iv, ok := toInt64(vv); ok {
				versionPtr = &iv
			}
		}
		t, ok := s.tasks[id]
		if !ok {
			continue
		}
		if t.State != resonate.TaskStateAcquired {
			continue
		}
		if t.PID == nil || *t.PID != pid {
			continue
		}
		if versionPtr != nil && *versionPtr != t.Version {
			continue
		}
		ttl := int64(30_000)
		if t.TTL != nil {
			ttl = *t.TTL
		}
		s.setTTimeout(id, 1, now+ttl)
	}
	return map[string]any{"kind": "task.heartbeat", "corrId": corrID, "status": 200}, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Schedule operations
// ──────────────────────────────────────────────────────────────────────────

func (s *serverState) scheduleCreate(now int64, corrID any, req map[string]any) (map[string]any, error) {
	id, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	if existing, ok := s.schedules[id]; ok {
		return map[string]any{
			"kind":     "schedule.create",
			"corrId":   corrID,
			"status":   200,
			"schedule": existing.toRecord(),
		}, nil
	}
	cron, err := requireStr(req, "cron")
	if err != nil {
		return nil, err
	}
	promiseID, err := requireStr(req, "promiseId")
	if err != nil {
		return nil, err
	}
	stub := &scheduleStub{
		ID:             id,
		Cron:           cron,
		PromiseID:      promiseID,
		PromiseTimeout: getInt64(req, "promiseTimeout", 0),
		PromiseParam:   rawFromAny(req["promiseParam"]),
		PromiseTags:    rawFromAny(req["promiseTags"]),
		CreatedAt:      now,
	}
	rec := stub.toRecord()
	s.schedules[id] = stub
	return map[string]any{
		"kind":     "schedule.create",
		"corrId":   corrID,
		"status":   201,
		"schedule": rec,
	}, nil
}

func (s *serverState) scheduleGet(corrID any, req map[string]any) (map[string]any, error) {
	id, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	stub, ok := s.schedules[id]
	if !ok {
		return map[string]any{
			"kind":   "schedule.get",
			"corrId": corrID,
			"status": 404,
			"error":  fmt.Sprintf("schedule %s not found", id),
		}, nil
	}
	return map[string]any{
		"kind":     "schedule.get",
		"corrId":   corrID,
		"status":   200,
		"schedule": stub.toRecord(),
	}, nil
}

func (s *serverState) scheduleDelete(corrID any, req map[string]any) (map[string]any, error) {
	id, err := requireStr(req, "id")
	if err != nil {
		return nil, err
	}
	if _, ok := s.schedules[id]; ok {
		delete(s.schedules, id)
		return map[string]any{"kind": "schedule.delete", "corrId": corrID, "status": 200}, nil
	}
	return map[string]any{
		"kind":   "schedule.delete",
		"corrId": corrID,
		"status": 404,
		"error":  fmt.Sprintf("schedule %s not found", id),
	}, nil
}

func (s *serverState) scheduleSearch(corrID any, _ map[string]any) (map[string]any, error) {
	recs := make([]any, 0, len(s.schedules))
	for _, stub := range s.schedules {
		recs = append(recs, stub.toRecord())
	}
	return map[string]any{
		"kind":      "schedule.search",
		"corrId":    corrID,
		"status":    200,
		"schedules": recs,
		"cursor":    nil,
	}, nil
}

// ──────────────────────────────────────────────────────────────────────────
// State helpers (private, actor-owned)
// ──────────────────────────────────────────────────────────────────────────

func (s *serverState) tryAutoTimeout(now int64, id string) {
	p, ok := s.promises[id]
	if !ok || p.State != resonate.PromiseStatePending || now < p.TimeoutAt {
		return
	}
	p.State = s.timeoutState(p.Tags)
	t := p.TimeoutAt
	p.SettledAt = &t
	s.delPTimeout(id)
	s.enqueueSettle(id)
	s.resumeAwaiters(id, now)
	s.notifySubscribers(id)
}

func (s *serverState) enqueueSettle(promiseID string) {
	t, ok := s.tasks[promiseID]
	if !ok {
		if p, ok := s.promises[promiseID]; ok {
			if _, hasTarget := p.Tags["resonate:target"]; hasTarget {
				s.tasks[promiseID] = &task{
					ID:      promiseID,
					State:   resonate.TaskStateFulfilled,
					Resumes: map[string]struct{}{},
				}
			}
		}
		return
	}
	if t.State == resonate.TaskStateFulfilled {
		return
	}
	t.State = resonate.TaskStateFulfilled
	t.PID = nil
	t.TTL = nil
	t.Resumes = map[string]struct{}{}
	s.delTTimeout(promiseID)
	for _, p := range s.promises {
		delete(p.Awaiters, promiseID)
	}
}

func (s *serverState) resumeAwaiters(promiseID string, now int64) {
	p, ok := s.promises[promiseID]
	if !ok {
		return
	}
	for awaiterID := range p.Awaiters {
		t, ok := s.tasks[awaiterID]
		if !ok {
			continue
		}
		switch t.State {
		case resonate.TaskStateSuspended:
			newVersion := t.Version + 1
			t.State = resonate.TaskStatePending
			t.Version = newVersion
			t.Resumes = map[string]struct{}{promiseID: {}}
			s.setTTimeout(awaiterID, 0, now+pendingRetryTTL)
			if ap, ok := s.promises[awaiterID]; ok {
				if addr, ok := ap.Tags["resonate:target"]; ok {
					s.sendExecuteMessage(addr, awaiterID, newVersion)
				}
			}
		case resonate.TaskStatePending, resonate.TaskStateAcquired, resonate.TaskStateHalted:
			t.Resumes[promiseID] = struct{}{}
		}
	}
	p.Awaiters = map[string]struct{}{}
}

func (s *serverState) notifySubscribers(promiseID string) {
	p, ok := s.promises[promiseID]
	if !ok || len(p.Subscribers) == 0 {
		return
	}
	rec := p.toRecord()
	for addr := range p.Subscribers {
		s.outgoing = append(s.outgoing, outgoingMsg{
			address: addr,
			message: map[string]any{
				"kind": "unblock",
				"data": map[string]any{"promise": rec},
			},
		})
	}
	p.Subscribers = map[string]struct{}{}
}

func (s *serverState) preload(promiseID string) []any {
	p, ok := s.promises[promiseID]
	if !ok {
		return []any{}
	}
	branch, ok := p.Tags["resonate:branch"]
	if !ok {
		return []any{}
	}
	out := []any{}
	for _, other := range s.promises {
		if other.ID == promiseID {
			continue
		}
		if b, ok := other.Tags["resonate:branch"]; ok && b == branch {
			out = append(out, other.toRecord())
		}
	}
	return out
}

func (s *serverState) timeoutState(tags map[string]string) resonate.PromiseState {
	if v, ok := tags["resonate:timer"]; ok && v == "true" {
		return resonate.PromiseStateResolved
	}
	return resonate.PromiseStateRejectedTimedout
}

func (s *serverState) setPTimeout(id string, timeout int64) {
	for i := range s.pTimeouts {
		if s.pTimeouts[i].id == id {
			s.pTimeouts[i].timeout = timeout
			return
		}
	}
	s.pTimeouts = append(s.pTimeouts, pTimeout{id: id, timeout: timeout})
}

func (s *serverState) delPTimeout(id string) {
	out := s.pTimeouts[:0]
	for _, pt := range s.pTimeouts {
		if pt.id != id {
			out = append(out, pt)
		}
	}
	s.pTimeouts = out
}

func (s *serverState) setTTimeout(id string, tType uint8, timeout int64) {
	for i := range s.tTimeouts {
		if s.tTimeouts[i].id == id {
			s.tTimeouts[i].tType = tType
			s.tTimeouts[i].timeout = timeout
			return
		}
	}
	s.tTimeouts = append(s.tTimeouts, tTimeout{id: id, tType: tType, timeout: timeout})
}

func (s *serverState) delTTimeout(id string) {
	out := s.tTimeouts[:0]
	for _, tt := range s.tTimeouts {
		if tt.id != id {
			out = append(out, tt)
		}
	}
	s.tTimeouts = out
}

func (s *serverState) sendExecuteMessage(address, taskID string, version int64) {
	msg := map[string]any{
		"kind": "execute",
		"data": map[string]any{
			"task": map[string]any{"id": taskID, "version": version},
		},
	}
	for i := range s.outgoing {
		om := s.outgoing[i].message
		if k, _ := om["kind"].(string); k != "execute" {
			continue
		}
		data, _ := om["data"].(map[string]any)
		tk, _ := data["task"].(map[string]any)
		if id, _ := tk["id"].(string); id == taskID {
			s.outgoing[i].address = address
			s.outgoing[i].message = msg
			return
		}
	}
	s.outgoing = append(s.outgoing, outgoingMsg{address: address, message: msg})
}

// ──────────────────────────────────────────────────────────────────────────
// Small utility helpers
// ──────────────────────────────────────────────────────────────────────────

func requireStr(obj map[string]any, field string) (string, error) {
	if obj == nil {
		return "", &resonate.ServerError{Code: 400, Message: "missing or empty required field: " + field}
	}
	if s, ok := obj[field].(string); ok && s != "" {
		return s, nil
	}
	return "", &resonate.ServerError{Code: 400, Message: "missing or empty required field: " + field}
}

func extractTags(req map[string]any) map[string]string {
	out := map[string]string{}
	raw, ok := req["tags"]
	if !ok {
		return out
	}
	obj, ok := raw.(map[string]any)
	if !ok {
		return out
	}
	for k, v := range obj {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func getInt64(obj map[string]any, key string, def int64) int64 {
	if v, ok := obj[key]; ok {
		if iv, ok := toInt64(v); ok {
			return iv
		}
	}
	return def
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return int64(x), true
	case int64:
		return x, true
	case int:
		return int64(x), true
	case json.Number:
		i, err := x.Int64()
		if err == nil {
			return i, true
		}
	}
	return 0, false
}

func parseInt64(s string) (int64, bool) {
	x, err := strconv.ParseInt(s, 10, 64)
	return x, err == nil
}

func rawFromAny(v any) json.RawMessage {
	switch x := v.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return x
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

func rawOrNull(r json.RawMessage) any {
	if len(r) == 0 {
		return nil
	}
	var v any
	if json.Unmarshal(r, &v) != nil {
		return nil
	}
	return v
}

func rawOrObject(r json.RawMessage) any {
	if len(r) == 0 {
		return map[string]any{}
	}
	var v any
	if json.Unmarshal(r, &v) != nil {
		return map[string]any{}
	}
	return v
}

func tagsOrEmpty(t map[string]string) map[string]string {
	if t == nil {
		return map[string]string{}
	}
	return t
}
