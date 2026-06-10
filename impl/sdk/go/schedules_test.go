package resonate_test

import (
	"errors"
	"testing"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

func TestSchedulesCreateGetRoundtrip(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()

	created, err := r.Schedules().Create(ctx, "s1", "0 * * * *", "run-{{.timestamp}}", time.Hour,
		resonate.ScheduleCreateOptions{
			PromiseParam: orderInput{Item: "report", Count: 1},
			PromiseTags:  map[string]string{"kind": "report"},
		})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID != "s1" {
		t.Errorf("ID = %q, want s1", created.ID)
	}
	if created.Cron != "0 * * * *" {
		t.Errorf("Cron = %q", created.Cron)
	}
	if created.PromiseID != "run-{{.timestamp}}" {
		t.Errorf("PromiseID = %q", created.PromiseID)
	}
	if created.PromiseTimeout != time.Hour.Milliseconds() {
		t.Errorf("PromiseTimeout = %d, want %d", created.PromiseTimeout, time.Hour.Milliseconds())
	}

	got, err := r.Schedules().Get(ctx, "s1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var param orderInput
	if err := got.PromiseParam.Decode(&param); err != nil {
		t.Fatalf("PromiseParam.Decode: %v", err)
	}
	if param != (orderInput{Item: "report", Count: 1}) {
		t.Errorf("param = %+v", param)
	}
	if got.PromiseTags["kind"] != "report" {
		t.Errorf("tags = %v", got.PromiseTags)
	}
}

func TestSchedulesDelete(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()

	if _, err := r.Schedules().Create(ctx, "s1", "0 * * * *", "run", time.Hour); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := r.Schedules().Delete(ctx, "s1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	var se *resonate.ServerError
	if _, err := r.Schedules().Get(ctx, "s1"); !errors.As(err, &se) || se.Code != 404 {
		t.Errorf("Get after delete: err = %v, want *ServerError{Code: 404}", err)
	}
	se = nil
	if err := r.Schedules().Delete(ctx, "missing"); !errors.As(err, &se) || se.Code != 404 {
		t.Errorf("Delete missing: err = %v, want *ServerError{Code: 404}", err)
	}
}

func TestSchedulesPrefixAppliedToBothIDs(t *testing.T) {
	r := newLocal(t, localConfig{Prefix: "pre"})
	ctx, cancel := testCtx(t)
	defer cancel()

	rec, err := r.Schedules().Create(ctx, "s1", "0 * * * *", "run-{{.timestamp}}", time.Hour)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rec.ID != "pre:s1" {
		t.Errorf("ID = %q, want pre:s1", rec.ID)
	}
	if rec.PromiseID != "pre:run-{{.timestamp}}" {
		t.Errorf("PromiseID = %q, want pre:run-{{.timestamp}}", rec.PromiseID)
	}
	if _, err := r.Schedules().Get(ctx, "s1"); err != nil {
		t.Errorf("Get(s1): %v", err)
	}
}
