package resonate

import (
	stdctx "context"
	"time"
)

// Schedules is the direct schedule API. A schedule creates a fresh promise on
// every cron firing, using the configured promise ID template, param, and
// tags.
//
// Returned ScheduleRecords have PromiseParam already codec-decoded: read it
// with rec.PromiseParam.Decode(&out). Obtain via [Resonate.Schedules].
type Schedules struct {
	r *Resonate
}

// ScheduleCreateOptions controls Schedules.Create.
type ScheduleCreateOptions struct {
	// PromiseParam is an arbitrary Go value codec-encoded into each scheduled
	// promise's param field. Nil stores an empty payload.
	PromiseParam any

	// PromiseTags are stored verbatim on each scheduled promise.
	PromiseTags map[string]string
}

// Get fetches a schedule by ID (with the configured prefix applied). Returns
// *ServerError with Code 404 when the schedule does not exist.
func (s *Schedules) Get(ctx stdctx.Context, id string) (ScheduleRecord, error) {
	rec, err := s.r.sender.ScheduleGet(ctx, s.r.prefixID(id))
	if err != nil {
		return ScheduleRecord{}, err
	}
	return s.decode(rec)
}

// Create creates a schedule that fires per the cron expression, creating a
// promise from the promiseID template (which may include placeholders like
// {{.timestamp}}) on each run. The configured prefix is applied to both the
// schedule ID and the promise ID template. promiseTimeout is the lifetime of
// each created promise, relative to its creation; a value <= 0 defaults to
// DefaultTopLevelTimeout (24h).
func (s *Schedules) Create(ctx stdctx.Context, id, cron, promiseID string, promiseTimeout time.Duration, opts ...ScheduleCreateOptions) (ScheduleRecord, error) {
	opt := firstOpt(opts)
	if promiseTimeout <= 0 {
		promiseTimeout = DefaultTopLevelTimeout
	}
	param, err := s.r.codec.Encode(opt.PromiseParam)
	if err != nil {
		return ScheduleRecord{}, err
	}
	tags := opt.PromiseTags
	if tags == nil {
		tags = map[string]string{}
	}
	rec, err := s.r.sender.ScheduleCreate(ctx, ScheduleCreateReq{
		ID:             s.r.prefixID(id),
		Cron:           cron,
		PromiseID:      s.r.prefixID(promiseID),
		PromiseTimeout: promiseTimeout.Milliseconds(),
		PromiseParam:   param,
		PromiseTags:    tags,
	})
	if err != nil {
		return ScheduleRecord{}, err
	}
	return s.decode(rec)
}

// Delete removes a schedule by ID (with the configured prefix applied).
// Promises already created by the schedule are unaffected. Returns
// *ServerError with Code 404 when the schedule does not exist.
func (s *Schedules) Delete(ctx stdctx.Context, id string) error {
	return s.r.sender.ScheduleDelete(ctx, s.r.prefixID(id))
}

func (s *Schedules) decode(rec ScheduleRecord) (ScheduleRecord, error) {
	param, err := s.r.codec.DecodeValue(rec.PromiseParam)
	if err != nil {
		return ScheduleRecord{}, err
	}
	rec.PromiseParam = param
	return rec, nil
}
