package dst

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/network"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/test"
)

const (
	Q_RATIO = 5 // max of 4 cqes per request, plus tolerance
	SQ_SIZE = 100000
	CQ_SIZE = SQ_SIZE * Q_RATIO
	T_DELTA = 10
)

type DST struct {
	Ticks                 int64
	SQEsPerTick           int
	Ids                   int
	Ikeys                 int
	Data                  int
	Headers               int
	Retries               int
	Subscriptions         int
	PromiseCacheSize      int
	TimeoutCacheSize      int
	NotificationCacheSize int
}

func (d *DST) Run(t *testing.T, r *rand.Rand, seed int64) {
	// config
	cfg := &system.Config{
		PromiseCacheSize:      d.PromiseCacheSize,
		TimeoutCacheSize:      d.TimeoutCacheSize,
		NotificationCacheSize: d.NotificationCacheSize,
		SubmissionBatchSize:   test.RangeIntn(r, 1, d.SQEsPerTick),
		CompletionBatchSize:   test.RangeIntn(r, 1, d.SQEsPerTick*Q_RATIO),
	}

	// metrics
	reg := prometheus.NewRegistry()
	metrics := metrics.New(reg)

	// instatiate api/aio
	api := api.New(SQ_SIZE, metrics)
	aio := aio.NewDST()

	// instatiate aio subsystems
	network := network.NewDST(r, 0.5)
	store, err := sqlite.New(sqlite.Config{Path: "test.db"})
	if err != nil {
		t.Fatal(err)
	}

	// add subsystems
	aio.AddSubsystem(types.Network, network)
	aio.AddSubsystem(types.Store, store)

	// start api/aio
	if err := api.Start(); err != nil {
		t.Fatal(err)
	}
	if err := aio.Start(); err != nil {
		t.Fatal(err)
	}

	// instatiate system
	system := system.New(cfg, api, aio, metrics)
	system.AddOnRequest(types.ReadPromise, coroutines.ReadPromise)
	system.AddOnRequest(types.SearchPromises, coroutines.SearchPromises)
	system.AddOnRequest(types.CreatePromise, coroutines.CreatePromise)
	system.AddOnRequest(types.ResolvePromise, coroutines.ResolvePromise)
	system.AddOnRequest(types.RejectPromise, coroutines.RejectPromise)
	system.AddOnRequest(types.CancelPromise, coroutines.CancelPromise)
	system.AddOnRequest(types.ReadSubscriptions, coroutines.ReadSubscriptions)
	system.AddOnRequest(types.CreateSubscription, coroutines.CreateSubscription)
	system.AddOnRequest(types.DeleteSubscription, coroutines.DeleteSubscription)
	system.AddOnTick(2, coroutines.TimeoutPromises)
	system.AddOnTick(1, coroutines.NotifySubscriptions)

	// generator
	generator := NewGenerator(r, d.Ids, d.Ikeys, d.Data, d.Headers, d.Retries, d.Subscriptions, d.Time(d.Ticks))
	generator.AddRequest(generator.GenerateReadPromise)
	generator.AddRequest(generator.GenerateSearchPromises)
	generator.AddRequest(generator.GenerateCreatePromise)
	generator.AddRequest(generator.GenerateCancelPromise)
	generator.AddRequest(generator.GenerateResolvePromise)
	generator.AddRequest(generator.GenerateRejectPromise)
	generator.AddRequest(generator.GenerateReadSubscriptions)
	generator.AddRequest(generator.GenerateCreateSubscription)
	generator.AddRequest(generator.GenerateDeleteSubscription)

	// model
	model := NewModel()
	model.AddResponse(types.ReadPromise, model.ValidateReadPromise)
	model.AddResponse(types.SearchPromises, model.ValidateSearchPromises)
	model.AddResponse(types.CreatePromise, model.ValidatCreatePromise)
	model.AddResponse(types.CancelPromise, model.ValidateCancelPromise)
	model.AddResponse(types.ResolvePromise, model.ValidateResolvePromise)
	model.AddResponse(types.RejectPromise, model.ValidateRejectPromise)
	model.AddResponse(types.ReadSubscriptions, model.ValidateReadSubscriptions)
	model.AddResponse(types.CreateSubscription, model.ValidateCreateSubscription)
	model.AddResponse(types.DeleteSubscription, model.ValidateDeleteSubscription)

	t.Log(d)

	// test loop
	for tick := int64(0); tick < d.Ticks; tick++ {
		time := d.Time(tick)

		for _, req := range generator.Generate(r, time, r.Intn(d.SQEsPerTick)) {
			req := req
			api.Enqueue(&bus.SQE[types.Request, types.Response]{
				Submission: req,
				Callback: func(res *types.Response, err error) {
					var errMsg string
					if err != nil {
						errMsg = err.Error()
					} else {
						errMsg = "<nil>"
					}

					t.Logf("t=%d req=%s res=%s err=%s", time, req, res, errMsg)

					if err := model.Step(req, res, err); err != nil {
						t.Fatal(err)
					}
				},
			})
		}

		system.Tick(time, nil)
	}

	// shutodown api/aio
	api.Shutdown()
	aio.Shutdown()

	// stop api/aio
	if err := api.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := aio.Stop(); err != nil {
		t.Fatal(err)
	}

	// reset store
	if err := store.Reset(); err != nil {
		t.Fatal(err)
	}

	t.Log(d)
}

func (d *DST) Time(tick int64) int64 {
	return tick * T_DELTA
}

func (d *DST) String() string {
	return fmt.Sprintf(
		"DST(ticks=%d, time=0->%d, sqesPerTick=%d, ids=%d, ikeys=%d, data=%d, headers=%d, retries=%d, promiseCacheSize=%d, timeoutCacheSize=%d, notificationCacheSize=%d)\n",
		d.Ticks,
		d.Time(d.Ticks),
		d.SQEsPerTick,
		d.Ids,
		d.Ikeys,
		d.Data,
		d.Retries,
		d.Headers,
		d.PromiseCacheSize,
		d.TimeoutCacheSize,
		d.NotificationCacheSize,
	)
}
