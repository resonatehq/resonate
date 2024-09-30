package grpc

import (
	"context"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) ReadSchedule(c context.Context, r *pb.ReadScheduleRequest) (*pb.ReadScheduleResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.ReadSchedule,
		ReadSchedule: &t_api.ReadScheduleRequest{
			Id: r.Id,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.ReadSchedule != nil, "result must not be nil")
	return &pb.ReadScheduleResponse{
		Schedule: protoSchedule(res.ReadSchedule.Schedule),
	}, nil
}

func (s *server) SearchSchedules(c context.Context, r *pb.SearchSchedulesRequest) (*pb.SearchSchedulesResponse, error) {
	req, err := s.api.SearchSchedules(r.Id, r.Tags, int(r.Limit), r.Cursor)
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind:            t_api.SearchSchedules,
		SearchSchedules: req,
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.SearchSchedules != nil, "result must not be nil")

	schedules := make([]*pb.Schedule, len(res.SearchSchedules.Schedules))
	for i, schedule := range res.SearchSchedules.Schedules {
		schedules[i] = protoSchedule(schedule)
	}

	var cursor string
	if res.SearchSchedules.Cursor != nil {
		var err error
		cursor, err = res.SearchSchedules.Cursor.Encode()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &pb.SearchSchedulesResponse{
		Schedules: schedules,
		Cursor:    cursor,
	}, nil
}

func (s *server) CreateSchedule(c context.Context, r *pb.CreateScheduleRequest) (*pb.CreatedScheduleResponse, error) {
	if err := s.api.ValidateCron(r.Cron); err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	var idempotencyKey *idempotency.Key
	if r.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(r.IdempotencyKey))
	}

	var headers map[string]string
	if r.PromiseParam != nil {
		headers = r.PromiseParam.Headers
	}

	var data []byte
	if r.PromiseParam != nil {
		data = r.PromiseParam.Data
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CreateSchedule,
		CreateSchedule: &t_api.CreateScheduleRequest{
			Id:             r.Id,
			Description:    r.Description,
			Cron:           r.Cron,
			Tags:           r.Tags,
			PromiseId:      r.PromiseId,
			PromiseTimeout: r.PromiseTimeout,
			PromiseParam:   promise.Value{Headers: headers, Data: data},
			PromiseTags:    r.PromiseTags,
			IdempotencyKey: idempotencyKey,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CreateSchedule != nil, "result must not be nil")
	return &pb.CreatedScheduleResponse{
		Schedule: protoSchedule(res.CreateSchedule.Schedule),
	}, nil
}

func (s *server) DeleteSchedule(c context.Context, r *pb.DeleteScheduleRequest) (*pb.DeleteScheduleResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.DeleteSchedule,
		DeleteSchedule: &t_api.DeleteScheduleRequest{
			Id: r.Id,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.DeleteSchedule != nil, "result must not be nil")
	return &pb.DeleteScheduleResponse{}, nil
}

// Helper functions

func protoSchedule(schedule *schedule.Schedule) *pb.Schedule {
	if schedule == nil {
		return nil
	}

	return &pb.Schedule{
		Id:             schedule.Id,
		Description:    schedule.Description,
		Cron:           schedule.Cron,
		Tags:           schedule.Tags,
		PromiseId:      schedule.PromiseId,
		PromiseTimeout: schedule.PromiseTimeout,
		PromiseParam:   &pb.Value{Headers: schedule.PromiseParam.Headers, Data: schedule.PromiseParam.Data},
		PromiseTags:    schedule.PromiseTags,
		LastRunTime:    util.SafeDeref(schedule.LastRunTime),
		NextRunTime:    schedule.NextRunTime,
		IdempotencyKey: string(util.SafeDeref(schedule.IdempotencyKey)),
		CreatedOn:      schedule.CreatedOn,
	}
}
