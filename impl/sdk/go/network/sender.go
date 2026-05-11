package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

// ──────────────────────────────────────────────────────────────────────────
// Request / response types
// ──────────────────────────────────────────────────────────────────────────

// TaskAcquireResult is the parsed outcome of task.acquire and task.create.
type TaskAcquireResult struct {
	Task    resonate.TaskRecord
	Promise resonate.PromiseRecord
	Preload []resonate.PromiseRecord
}

// TaskCreateResult is the result of Sender.TaskCreate. Exactly one of Created
// or Conflict is set: Conflict is true when the server returned 409 because a
// task or promise with the same id already existed.
type TaskCreateResult struct {
	Created  *TaskAcquireResult
	Conflict bool
}

// SuspendResult is the parsed outcome of task.suspend. When Redirected is
// false the task was suspended normally; when true the server short-circuited
// because at least one awaited promise had already settled, in which case
// Preload carries the freshly preloaded promises.
type SuspendResult struct {
	Redirected bool
	Preload    []resonate.PromiseRecord
}

// TaskFenceResult is the parsed outcome of a task.fence call.
type TaskFenceResult struct {
	Promise resonate.PromiseRecord
	Preload []resonate.PromiseRecord
}

// TaskSearchResult is a paginated task list.
type TaskSearchResult struct {
	Tasks  []resonate.TaskRecord
	Cursor *string
}

// PromiseSearchResult is a paginated promise list.
type PromiseSearchResult struct {
	Promises []resonate.PromiseRecord
	Cursor   *string
}

// ScheduleSearchResult is a paginated schedule list.
type ScheduleSearchResult struct {
	Schedules []resonate.ScheduleRecord
	Cursor    *string
}

// ScheduleCreateReq is the data payload for schedule.create.
type ScheduleCreateReq struct {
	ID             string            `json:"id"`
	Cron           string            `json:"cron"`
	PromiseID      string            `json:"promiseId"`
	PromiseTimeout int64             `json:"promiseTimeout"`
	PromiseParam   resonate.Value    `json:"promiseParam"`
	PromiseTags    map[string]string `json:"promiseTags"`
}

// TaskRef is a (id, version) pair used by heartbeat.
type TaskRef struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

// ──────────────────────────────────────────────────────────────────────────
// Sender
// ──────────────────────────────────────────────────────────────────────────

// Sender provides typed RPC methods over a Network. It builds outgoing
// envelopes, validates the corrId/kind on responses, and parses replies into
// typed records.
//
// The push path (server → worker) flows through Sender.Recv, which decodes
// each raw frame into a Message before forwarding to the caller's callback.
type Sender struct {
	net  Network
	auth *string

	log *slog.Logger
}

// NewSender wraps a Network. Pass nil for auth when no Authorization header
// is required (e.g., for LocalNetwork or unauthenticated servers).
func NewSender(n Network, auth *string) *Sender {
	return &Sender{net: n, auth: auth, log: slog.Default()}
}

// Recv registers a callback for push messages. Frames that fail to decode
// are logged at warn and dropped — they do not break the subscription.
func (s *Sender) Recv(cb func(Message)) {
	s.net.Recv(func(raw string) {
		msg, err := DecodeMessage([]byte(raw))
		if err != nil {
			s.log.Warn("sender: dropping malformed push frame", "err", err, "raw", raw)
			return
		}
		cb(msg)
	})
}

// envelope is the outer wire shape: { kind, head, data }.
type envelope struct {
	Kind string `json:"kind"`
	Head head   `json:"head"`
	Data any    `json:"data"`
}

// head carries the per-request correlation id, protocol version, and (optionally) auth.
type head struct {
	CorrID  string  `json:"corrId"`
	Version string  `json:"version"`
	Auth    *string `json:"auth,omitempty"`
}

// subEnvelope is the nested envelope used inside task.create / task.fulfill /
// task.suspend / task.fence for the action payload.
type subEnvelope struct {
	Kind string `json:"kind"`
	Head head   `json:"head"`
	Data any    `json:"data"`
}

func (s *Sender) makeHead() head {
	return head{
		CorrID:  fmt.Sprintf("sr-%d", time.Now().UnixMilli()),
		Version: resonate.ProtocolVersion,
		Auth:    s.auth,
	}
}

// sendEnvelope builds and ships an envelope, validates correlation, and
// returns (status, response data). If allow409 is true, a 409 status is
// returned to the caller without being converted into an error; any other
// 4xx/5xx status returns a *resonate.ServerError.
func (s *Sender) sendEnvelope(ctx context.Context, kind string, data any, allow409 bool) (int, json.RawMessage, error) {
	h := s.makeHead()
	env := envelope{Kind: kind, Head: h, Data: data}
	body, err := json.Marshal(env)
	if err != nil {
		return 0, nil, &resonate.EncodingError{Msg: fmt.Sprintf("marshal envelope: %v", err)}
	}

	respRaw, err := s.net.Send(ctx, string(body))
	if err != nil {
		return 0, nil, err
	}

	var resp struct {
		Kind string          `json:"kind"`
		Head respHead        `json:"head"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(respRaw), &resp); err != nil {
		return 0, nil, &resonate.DecodingError{Msg: fmt.Sprintf("response envelope: %v", err)}
	}
	if resp.Kind != kind {
		return 0, nil, &resonate.ServerError{
			Code:    500,
			Message: fmt.Sprintf("response kind mismatch: expected %q, got %q", kind, resp.Kind),
		}
	}
	if resp.Head.CorrID != h.CorrID {
		return 0, nil, &resonate.ServerError{
			Code:    500,
			Message: fmt.Sprintf("response corrId mismatch: expected %q, got %q", h.CorrID, resp.Head.CorrID),
		}
	}

	status := resp.Head.Status
	if status == 0 {
		status = 200
	}
	if status >= 400 && !(allow409 && status == 409) {
		msg := errorMessageFromData(resp.Data, status)
		return status, resp.Data, &resonate.ServerError{Code: status, Message: msg}
	}
	return status, resp.Data, nil
}

type respHead struct {
	CorrID string `json:"corrId"`
	Status int    `json:"status"`
}

func errorMessageFromData(data json.RawMessage, status int) string {
	var s string
	if err := json.Unmarshal(data, &s); err == nil && s != "" {
		return s
	}
	var wrap struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(data, &wrap); err == nil && wrap.Error != "" {
		return wrap.Error
	}
	return fmt.Sprintf("server error (status %d)", status)
}

// wrapAction builds a nested envelope used by task.create / task.fulfill /
// task.suspend / task.fence for the action payload.
func (s *Sender) wrapAction(kind string, data any) subEnvelope {
	return subEnvelope{Kind: kind, Head: s.makeHead(), Data: data}
}

// ──────────────────────────────────────────────────────────────────────────
// Promises
// ──────────────────────────────────────────────────────────────────────────

// PromiseCreate creates a durable promise.
func (s *Sender) PromiseCreate(ctx context.Context, req resonate.PromiseCreateReq) (resonate.PromiseRecord, error) {
	_, data, err := s.sendEnvelope(ctx, "promise.create", req, false)
	if err != nil {
		return resonate.PromiseRecord{}, err
	}
	return parseRecord[resonate.PromiseRecord](data, "promise")
}

// PromiseSettle resolves, rejects, or cancels a durable promise.
func (s *Sender) PromiseSettle(ctx context.Context, req resonate.PromiseSettleReq) (resonate.PromiseRecord, error) {
	_, data, err := s.sendEnvelope(ctx, "promise.settle", req, false)
	if err != nil {
		return resonate.PromiseRecord{}, err
	}
	return parseRecord[resonate.PromiseRecord](data, "promise")
}

// PromiseGet fetches a promise by ID.
func (s *Sender) PromiseGet(ctx context.Context, id string) (resonate.PromiseRecord, error) {
	_, data, err := s.sendEnvelope(ctx, "promise.get", map[string]any{"id": id}, false)
	if err != nil {
		return resonate.PromiseRecord{}, err
	}
	return parseRecord[resonate.PromiseRecord](data, "promise")
}

// PromiseRegisterCallback links two promises so that settlement of `awaited`
// resumes the task identified by `awaiter`.
func (s *Sender) PromiseRegisterCallback(ctx context.Context, awaited, awaiter string) (resonate.PromiseRecord, error) {
	_, data, err := s.sendEnvelope(ctx, "promise.register_callback", map[string]any{
		"awaited": awaited,
		"awaiter": awaiter,
	}, false)
	if err != nil {
		return resonate.PromiseRecord{}, err
	}
	return parseRecord[resonate.PromiseRecord](data, "promise")
}

// PromiseRegisterListener subscribes a polling address to a promise so the
// server emits an unblock push when it settles.
func (s *Sender) PromiseRegisterListener(ctx context.Context, awaited, address string) (resonate.PromiseRecord, error) {
	_, data, err := s.sendEnvelope(ctx, "promise.register_listener", map[string]any{
		"awaited": awaited,
		"address": address,
	}, false)
	if err != nil {
		return resonate.PromiseRecord{}, err
	}
	return parseRecord[resonate.PromiseRecord](data, "promise")
}

// PromiseSearchOptions controls a PromiseSearch call. Any zero-valued field
// is omitted from the request.
type PromiseSearchOptions struct {
	State  string
	Tags   map[string]string
	Limit  *uint32
	Cursor string
}

// PromiseSearch returns a paginated list of promises matching the filter.
func (s *Sender) PromiseSearch(ctx context.Context, opts PromiseSearchOptions) (PromiseSearchResult, error) {
	payload := map[string]any{}
	if opts.State != "" {
		payload["state"] = opts.State
	}
	if opts.Tags != nil {
		payload["tags"] = opts.Tags
	}
	if opts.Limit != nil {
		payload["limit"] = *opts.Limit
	}
	if opts.Cursor != "" {
		payload["cursor"] = opts.Cursor
	}
	items, cursor, err := searchList[resonate.PromiseRecord](ctx, s, "promise.search", payload, "promises")
	if err != nil {
		return PromiseSearchResult{}, err
	}
	return PromiseSearchResult{Promises: items, Cursor: cursor}, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Tasks
// ──────────────────────────────────────────────────────────────────────────

// TaskAcquire claims a task's lease so this worker can execute it. Returns
// the task record, root promise, and any preloaded branch promises.
func (s *Sender) TaskAcquire(ctx context.Context, id string, version int64, pid string, ttl int64) (TaskAcquireResult, error) {
	_, data, err := s.sendEnvelope(ctx, "task.acquire", map[string]any{
		"id":      id,
		"version": version,
		"pid":     pid,
		"ttl":     ttl,
	}, false)
	if err != nil {
		return TaskAcquireResult{}, err
	}
	return parseTaskAcquireFromData(data)
}

// TaskCreate creates both a root promise and a task in one shot. A 409 from
// the server is treated as an idempotency conflict and surfaced as
// TaskCreateResult{Conflict: true} rather than an error.
func (s *Sender) TaskCreate(ctx context.Context, pid string, ttl int64, action resonate.PromiseCreateReq) (TaskCreateResult, error) {
	payload := map[string]any{
		"pid":    pid,
		"ttl":    ttl,
		"action": s.wrapAction("promise.create", action),
	}
	status, data, err := s.sendEnvelope(ctx, "task.create", payload, true)
	if err != nil {
		return TaskCreateResult{}, err
	}
	if status == 409 {
		return TaskCreateResult{Conflict: true}, nil
	}
	acq, err := parseTaskAcquireFromData(data)
	if err != nil {
		return TaskCreateResult{}, err
	}
	return TaskCreateResult{Created: &acq}, nil
}

// TaskFulfill settles the task's root promise as part of a single
// fence+settle operation.
func (s *Sender) TaskFulfill(ctx context.Context, id string, version int64, action resonate.PromiseSettleReq) (resonate.PromiseRecord, error) {
	payload := map[string]any{
		"id":      id,
		"version": version,
		"action":  s.wrapAction("promise.settle", action),
	}
	_, data, err := s.sendEnvelope(ctx, "task.fulfill", payload, false)
	if err != nil {
		return resonate.PromiseRecord{}, err
	}
	return parseRecord[resonate.PromiseRecord](data, "promise")
}

// TaskSuspend registers callbacks for awaited promises and suspends the task.
// Returns Redirected=true when at least one awaited promise was already
// settled, in which case the caller should retry rather than suspend.
func (s *Sender) TaskSuspend(ctx context.Context, id string, version int64, actions []resonate.PromiseRegisterCallbackData) (SuspendResult, error) {
	wrapped := make([]subEnvelope, len(actions))
	for i, a := range actions {
		wrapped[i] = s.wrapAction("promise.register_callback", a)
	}
	payload := map[string]any{
		"id":      id,
		"version": version,
		"actions": wrapped,
	}
	status, data, err := s.sendEnvelope(ctx, "task.suspend", payload, false)
	if err != nil {
		return SuspendResult{}, err
	}
	return parseSuspendResult(status, data), nil
}

// TaskRelease gives up the lease on a task without fulfilling it; the server
// will retry it on the next tick.
func (s *Sender) TaskRelease(ctx context.Context, id string, version int64) error {
	_, _, err := s.sendEnvelope(ctx, "task.release", map[string]any{
		"id":      id,
		"version": version,
	}, false)
	return err
}

// TaskHalt prevents a task from being acquired or making progress.
func (s *Sender) TaskHalt(ctx context.Context, id string) error {
	_, _, err := s.sendEnvelope(ctx, "task.halt", map[string]any{"id": id}, false)
	return err
}

// TaskContinue transitions a halted task back to pending.
func (s *Sender) TaskContinue(ctx context.Context, id string) error {
	_, _, err := s.sendEnvelope(ctx, "task.continue", map[string]any{"id": id}, false)
	return err
}

// TaskFenceCreate creates a promise via task.fence, executing only if the
// task lease (id, version) is still valid.
func (s *Sender) TaskFenceCreate(ctx context.Context, id string, version int64, req resonate.PromiseCreateReq) (TaskFenceResult, error) {
	return s.taskFence(ctx, id, version, "promise.create", req)
}

// TaskFenceSettle settles a promise via task.fence, executing only if the
// task lease (id, version) is still valid.
func (s *Sender) TaskFenceSettle(ctx context.Context, id string, version int64, req resonate.PromiseSettleReq) (TaskFenceResult, error) {
	return s.taskFence(ctx, id, version, "promise.settle", req)
}

func (s *Sender) taskFence(ctx context.Context, id string, version int64, subKind string, action any) (TaskFenceResult, error) {
	payload := map[string]any{
		"id":      id,
		"version": version,
		"action":  s.wrapAction(subKind, action),
	}
	_, data, err := s.sendEnvelope(ctx, "task.fence", payload, false)
	if err != nil {
		return TaskFenceResult{}, err
	}
	var wrap struct {
		Action struct {
			Data struct {
				Promise json.RawMessage `json:"promise"`
			} `json:"data"`
		} `json:"action"`
	}
	if err := json.Unmarshal(data, &wrap); err != nil {
		return TaskFenceResult{}, &resonate.DecodingError{Msg: fmt.Sprintf("fence response: %v", err)}
	}
	if len(wrap.Action.Data.Promise) == 0 {
		return TaskFenceResult{}, &resonate.DecodingError{Msg: "missing promise in fence action response"}
	}
	var promise resonate.PromiseRecord
	if err := json.Unmarshal(wrap.Action.Data.Promise, &promise); err != nil {
		return TaskFenceResult{}, &resonate.DecodingError{Msg: fmt.Sprintf("invalid promise in fence response: %v", err)}
	}
	return TaskFenceResult{Promise: promise, Preload: parsePreloadedFromData(data)}, nil
}

// TaskHeartbeat extends the lease on one or more tasks owned by pid.
func (s *Sender) TaskHeartbeat(ctx context.Context, pid string, tasks []TaskRef) error {
	_, _, err := s.sendEnvelope(ctx, "task.heartbeat", map[string]any{
		"pid":   pid,
		"tasks": tasks,
	}, false)
	return err
}

// TaskSearchOptions controls a TaskSearch call.
type TaskSearchOptions struct {
	State  string
	Limit  *uint32
	Cursor string
}

// TaskSearch returns a paginated list of tasks matching the filter.
func (s *Sender) TaskSearch(ctx context.Context, opts TaskSearchOptions) (TaskSearchResult, error) {
	payload := map[string]any{}
	if opts.State != "" {
		payload["state"] = opts.State
	}
	if opts.Limit != nil {
		payload["limit"] = *opts.Limit
	}
	if opts.Cursor != "" {
		payload["cursor"] = opts.Cursor
	}
	items, cursor, err := searchList[resonate.TaskRecord](ctx, s, "task.search", payload, "tasks")
	if err != nil {
		return TaskSearchResult{}, err
	}
	return TaskSearchResult{Tasks: items, Cursor: cursor}, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Schedules
// ──────────────────────────────────────────────────────────────────────────

// ScheduleCreate creates a new schedule.
func (s *Sender) ScheduleCreate(ctx context.Context, req ScheduleCreateReq) (resonate.ScheduleRecord, error) {
	_, data, err := s.sendEnvelope(ctx, "schedule.create", req, false)
	if err != nil {
		return resonate.ScheduleRecord{}, err
	}
	return parseRecord[resonate.ScheduleRecord](data, "schedule")
}

// ScheduleGet fetches a schedule by ID.
func (s *Sender) ScheduleGet(ctx context.Context, id string) (resonate.ScheduleRecord, error) {
	_, data, err := s.sendEnvelope(ctx, "schedule.get", map[string]any{"id": id}, false)
	if err != nil {
		return resonate.ScheduleRecord{}, err
	}
	return parseRecord[resonate.ScheduleRecord](data, "schedule")
}

// ScheduleDelete removes a schedule by ID.
func (s *Sender) ScheduleDelete(ctx context.Context, id string) error {
	_, _, err := s.sendEnvelope(ctx, "schedule.delete", map[string]any{"id": id}, false)
	return err
}

// ScheduleSearchOptions controls a ScheduleSearch call.
type ScheduleSearchOptions struct {
	Tags   map[string]string
	Limit  *uint32
	Cursor string
}

// ScheduleSearch returns a paginated list of schedules.
func (s *Sender) ScheduleSearch(ctx context.Context, opts ScheduleSearchOptions) (ScheduleSearchResult, error) {
	payload := map[string]any{}
	if opts.Tags != nil {
		payload["tags"] = opts.Tags
	}
	if opts.Limit != nil {
		payload["limit"] = *opts.Limit
	}
	if opts.Cursor != "" {
		payload["cursor"] = opts.Cursor
	}
	items, cursor, err := searchList[resonate.ScheduleRecord](ctx, s, "schedule.search", payload, "schedules")
	if err != nil {
		return ScheduleSearchResult{}, err
	}
	return ScheduleSearchResult{Schedules: items, Cursor: cursor}, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Response parsers
// ──────────────────────────────────────────────────────────────────────────

func parseTaskAcquireFromData(data json.RawMessage) (TaskAcquireResult, error) {
	var wrap struct {
		Task    json.RawMessage   `json:"task"`
		Promise json.RawMessage   `json:"promise"`
		Preload []json.RawMessage `json:"preload"`
	}
	if err := json.Unmarshal(data, &wrap); err != nil {
		return TaskAcquireResult{}, &resonate.DecodingError{Msg: fmt.Sprintf("task.acquire response: %v", err)}
	}
	var task resonate.TaskRecord
	if err := json.Unmarshal(wrap.Task, &task); err != nil {
		return TaskAcquireResult{}, &resonate.DecodingError{Msg: fmt.Sprintf("invalid task: %v", err)}
	}
	var promise resonate.PromiseRecord
	if err := json.Unmarshal(wrap.Promise, &promise); err != nil {
		return TaskAcquireResult{}, &resonate.DecodingError{Msg: fmt.Sprintf("invalid promise: %v", err)}
	}
	preload := make([]resonate.PromiseRecord, 0, len(wrap.Preload))
	for _, raw := range wrap.Preload {
		var p resonate.PromiseRecord
		if err := json.Unmarshal(raw, &p); err == nil {
			preload = append(preload, p)
		}
	}
	return TaskAcquireResult{Task: task, Promise: promise, Preload: preload}, nil
}

func parsePreloadedFromData(data json.RawMessage) []resonate.PromiseRecord {
	var wrap struct {
		Preload []json.RawMessage `json:"preload"`
	}
	if err := json.Unmarshal(data, &wrap); err != nil {
		return nil
	}
	out := make([]resonate.PromiseRecord, 0, len(wrap.Preload))
	for _, raw := range wrap.Preload {
		var p resonate.PromiseRecord
		if err := json.Unmarshal(raw, &p); err == nil {
			out = append(out, p)
		}
	}
	return out
}

func parseSuspendResult(status int, data json.RawMessage) SuspendResult {
	if status == 300 {
		return SuspendResult{Redirected: true, Preload: parsePreloadedFromData(data)}
	}
	return SuspendResult{}
}

func parseRecord[T any](data json.RawMessage, field string) (T, error) {
	var zero T
	wrap := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &wrap); err != nil {
		return zero, &resonate.DecodingError{Msg: fmt.Sprintf("missing %q in response", field)}
	}
	raw, ok := wrap[field]
	if !ok || len(raw) == 0 {
		return zero, &resonate.DecodingError{Msg: fmt.Sprintf("missing %q in response", field)}
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, &resonate.DecodingError{Msg: fmt.Sprintf("invalid %s: %v", field, err)}
	}
	return out, nil
}

// searchList runs a paginated search RPC, unmarshaling response items from the
// named field into a typed slice. Items that fail to decode are skipped.
func searchList[T any](ctx context.Context, s *Sender, kind string, payload map[string]any, field string) ([]T, *string, error) {
	_, data, err := s.sendEnvelope(ctx, kind, payload, false)
	if err != nil {
		return nil, nil, err
	}
	var aux map[string]json.RawMessage
	if err := json.Unmarshal(data, &aux); err != nil {
		return nil, nil, &resonate.DecodingError{Msg: fmt.Sprintf("%s response: %v", kind, err)}
	}
	var items []json.RawMessage
	if raw, ok := aux[field]; ok {
		_ = json.Unmarshal(raw, &items)
	}
	var cursor *string
	if raw, ok := aux["cursor"]; ok {
		_ = json.Unmarshal(raw, &cursor)
	}
	out := make([]T, 0, len(items))
	for _, raw := range items {
		var v T
		if err := json.Unmarshal(raw, &v); err == nil {
			out = append(out, v)
		}
	}
	return out, cursor, nil
}
