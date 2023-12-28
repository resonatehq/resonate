// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v4.25.1
// source: internal/app/subsystems/api/grpc/api/schedule.proto

package api

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type PromiseValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data    []byte            `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	Headers map[string]string `protobuf:"bytes,2,rep,name=headers,proto3" json:"headers,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *PromiseValue) Reset() {
	*x = PromiseValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PromiseValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PromiseValue) ProtoMessage() {}

func (x *PromiseValue) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PromiseValue.ProtoReflect.Descriptor instead.
func (*PromiseValue) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{0}
}

func (x *PromiseValue) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *PromiseValue) GetHeaders() map[string]string {
	if x != nil {
		return x.Headers
	}
	return nil
}

type Schedule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id             string            `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Description    string            `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	Cron           string            `protobuf:"bytes,3,opt,name=cron,proto3" json:"cron,omitempty"`
	Tags           map[string]string `protobuf:"bytes,4,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	PromiseId      string            `protobuf:"bytes,5,opt,name=promiseId,proto3" json:"promiseId,omitempty"`
	PromiseTimeout int64             `protobuf:"varint,6,opt,name=promiseTimeout,proto3" json:"promiseTimeout,omitempty"`
	PromiseParam   *PromiseValue     `protobuf:"bytes,7,opt,name=promiseParam,proto3" json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `protobuf:"bytes,8,rep,name=promiseTags,proto3" json:"promiseTags,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	LastRunTime    int64             `protobuf:"varint,9,opt,name=lastRunTime,proto3" json:"lastRunTime,omitempty"`
	NextRunTime    int64             `protobuf:"varint,10,opt,name=nextRunTime,proto3" json:"nextRunTime,omitempty"`
	IdempotencyKey string            `protobuf:"bytes,11,opt,name=idempotencyKey,proto3" json:"idempotencyKey,omitempty"`
	CreatedOn      int64             `protobuf:"varint,12,opt,name=createdOn,proto3" json:"createdOn,omitempty"`
}

func (x *Schedule) Reset() {
	*x = Schedule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Schedule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Schedule) ProtoMessage() {}

func (x *Schedule) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Schedule.ProtoReflect.Descriptor instead.
func (*Schedule) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{1}
}

func (x *Schedule) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Schedule) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *Schedule) GetCron() string {
	if x != nil {
		return x.Cron
	}
	return ""
}

func (x *Schedule) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

func (x *Schedule) GetPromiseId() string {
	if x != nil {
		return x.PromiseId
	}
	return ""
}

func (x *Schedule) GetPromiseTimeout() int64 {
	if x != nil {
		return x.PromiseTimeout
	}
	return 0
}

func (x *Schedule) GetPromiseParam() *PromiseValue {
	if x != nil {
		return x.PromiseParam
	}
	return nil
}

func (x *Schedule) GetPromiseTags() map[string]string {
	if x != nil {
		return x.PromiseTags
	}
	return nil
}

func (x *Schedule) GetLastRunTime() int64 {
	if x != nil {
		return x.LastRunTime
	}
	return 0
}

func (x *Schedule) GetNextRunTime() int64 {
	if x != nil {
		return x.NextRunTime
	}
	return 0
}

func (x *Schedule) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

func (x *Schedule) GetCreatedOn() int64 {
	if x != nil {
		return x.CreatedOn
	}
	return 0
}

type CreateScheduleRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Schedule  *Schedule `protobuf:"bytes,1,opt,name=schedule,proto3" json:"schedule,omitempty"`
	RequestId string    `protobuf:"bytes,2,opt,name=requestId,proto3" json:"requestId,omitempty"`
}

func (x *CreateScheduleRequest) Reset() {
	*x = CreateScheduleRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateScheduleRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateScheduleRequest) ProtoMessage() {}

func (x *CreateScheduleRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateScheduleRequest.ProtoReflect.Descriptor instead.
func (*CreateScheduleRequest) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{2}
}

func (x *CreateScheduleRequest) GetSchedule() *Schedule {
	if x != nil {
		return x.Schedule
	}
	return nil
}

func (x *CreateScheduleRequest) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

type CreatedScheduleResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Noop     bool      `protobuf:"varint,1,opt,name=noop,proto3" json:"noop,omitempty"`
	Schedule *Schedule `protobuf:"bytes,2,opt,name=schedule,proto3" json:"schedule,omitempty"`
}

func (x *CreatedScheduleResponse) Reset() {
	*x = CreatedScheduleResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreatedScheduleResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreatedScheduleResponse) ProtoMessage() {}

func (x *CreatedScheduleResponse) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreatedScheduleResponse.ProtoReflect.Descriptor instead.
func (*CreatedScheduleResponse) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{3}
}

func (x *CreatedScheduleResponse) GetNoop() bool {
	if x != nil {
		return x.Noop
	}
	return false
}

func (x *CreatedScheduleResponse) GetSchedule() *Schedule {
	if x != nil {
		return x.Schedule
	}
	return nil
}

type ReadScheduleRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id        string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	RequestId string `protobuf:"bytes,2,opt,name=requestId,proto3" json:"requestId,omitempty"`
}

func (x *ReadScheduleRequest) Reset() {
	*x = ReadScheduleRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadScheduleRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadScheduleRequest) ProtoMessage() {}

func (x *ReadScheduleRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadScheduleRequest.ProtoReflect.Descriptor instead.
func (*ReadScheduleRequest) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{4}
}

func (x *ReadScheduleRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *ReadScheduleRequest) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

type ReadScheduleResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Schedule *Schedule `protobuf:"bytes,1,opt,name=schedule,proto3" json:"schedule,omitempty"`
}

func (x *ReadScheduleResponse) Reset() {
	*x = ReadScheduleResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadScheduleResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadScheduleResponse) ProtoMessage() {}

func (x *ReadScheduleResponse) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadScheduleResponse.ProtoReflect.Descriptor instead.
func (*ReadScheduleResponse) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{5}
}

func (x *ReadScheduleResponse) GetSchedule() *Schedule {
	if x != nil {
		return x.Schedule
	}
	return nil
}

type SearchSchedulesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id        string            `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Tags      map[string]string `protobuf:"bytes,2,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Limit     int32             `protobuf:"varint,3,opt,name=limit,proto3" json:"limit,omitempty"`
	Cursor    string            `protobuf:"bytes,4,opt,name=cursor,proto3" json:"cursor,omitempty"`
	RequestId string            `protobuf:"bytes,5,opt,name=requestId,proto3" json:"requestId,omitempty"`
}

func (x *SearchSchedulesRequest) Reset() {
	*x = SearchSchedulesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SearchSchedulesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SearchSchedulesRequest) ProtoMessage() {}

func (x *SearchSchedulesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SearchSchedulesRequest.ProtoReflect.Descriptor instead.
func (*SearchSchedulesRequest) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{6}
}

func (x *SearchSchedulesRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *SearchSchedulesRequest) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

func (x *SearchSchedulesRequest) GetLimit() int32 {
	if x != nil {
		return x.Limit
	}
	return 0
}

func (x *SearchSchedulesRequest) GetCursor() string {
	if x != nil {
		return x.Cursor
	}
	return ""
}

func (x *SearchSchedulesRequest) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

type SearchSchedulesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cursor    string      `protobuf:"bytes,1,opt,name=cursor,proto3" json:"cursor,omitempty"`
	Schedules []*Schedule `protobuf:"bytes,2,rep,name=schedules,proto3" json:"schedules,omitempty"`
}

func (x *SearchSchedulesResponse) Reset() {
	*x = SearchSchedulesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SearchSchedulesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SearchSchedulesResponse) ProtoMessage() {}

func (x *SearchSchedulesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SearchSchedulesResponse.ProtoReflect.Descriptor instead.
func (*SearchSchedulesResponse) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{7}
}

func (x *SearchSchedulesResponse) GetCursor() string {
	if x != nil {
		return x.Cursor
	}
	return ""
}

func (x *SearchSchedulesResponse) GetSchedules() []*Schedule {
	if x != nil {
		return x.Schedules
	}
	return nil
}

type DeleteScheduleRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id        string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	RequestId string `protobuf:"bytes,2,opt,name=requestId,proto3" json:"requestId,omitempty"`
}

func (x *DeleteScheduleRequest) Reset() {
	*x = DeleteScheduleRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteScheduleRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteScheduleRequest) ProtoMessage() {}

func (x *DeleteScheduleRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteScheduleRequest.ProtoReflect.Descriptor instead.
func (*DeleteScheduleRequest) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{8}
}

func (x *DeleteScheduleRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *DeleteScheduleRequest) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

type DeleteScheduleResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *DeleteScheduleResponse) Reset() {
	*x = DeleteScheduleResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteScheduleResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteScheduleResponse) ProtoMessage() {}

func (x *DeleteScheduleResponse) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteScheduleResponse.ProtoReflect.Descriptor instead.
func (*DeleteScheduleResponse) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP(), []int{9}
}

var File_internal_app_subsystems_api_grpc_api_schedule_proto protoreflect.FileDescriptor

var file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDesc = []byte{
	0x0a, 0x33, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x70, 0x2f, 0x73,
	0x75, 0x62, 0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x73, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x67, 0x72,
	0x70, 0x63, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x22,
	0x9d, 0x01, 0x0a, 0x0c, 0x50, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04,
	0x64, 0x61, 0x74, 0x61, 0x12, 0x3d, 0x0a, 0x07, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x73, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x2e, 0x50, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x2e, 0x48, 0x65,
	0x61, 0x64, 0x65, 0x72, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x68, 0x65, 0x61, 0x64,
	0x65, 0x72, 0x73, 0x1a, 0x3a, 0x0a, 0x0c, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x73, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22,
	0xce, 0x04, 0x0a, 0x08, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x12, 0x0e, 0x0a, 0x02,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x20, 0x0a, 0x0b,
	0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x12,
	0x0a, 0x04, 0x63, 0x72, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x63, 0x72,
	0x6f, 0x6e, 0x12, 0x30, 0x0a, 0x04, 0x74, 0x61, 0x67, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x1c, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x53, 0x63, 0x68, 0x65,
	0x64, 0x75, 0x6c, 0x65, 0x2e, 0x54, 0x61, 0x67, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x04,
	0x74, 0x61, 0x67, 0x73, 0x12, 0x1c, 0x0a, 0x09, 0x70, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65, 0x49,
	0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x70, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65,
	0x49, 0x64, 0x12, 0x26, 0x0a, 0x0e, 0x70, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65, 0x54, 0x69, 0x6d,
	0x65, 0x6f, 0x75, 0x74, 0x18, 0x06, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0e, 0x70, 0x72, 0x6f, 0x6d,
	0x69, 0x73, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x12, 0x3a, 0x0a, 0x0c, 0x70, 0x72,
	0x6f, 0x6d, 0x69, 0x73, 0x65, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x16, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x50, 0x72, 0x6f, 0x6d,
	0x69, 0x73, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x0c, 0x70, 0x72, 0x6f, 0x6d, 0x69, 0x73,
	0x65, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x12, 0x45, 0x0a, 0x0b, 0x70, 0x72, 0x6f, 0x6d, 0x69, 0x73,
	0x65, 0x54, 0x61, 0x67, 0x73, 0x18, 0x08, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x73, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e,
	0x50, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65, 0x54, 0x61, 0x67, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65, 0x54, 0x61, 0x67, 0x73, 0x12, 0x20, 0x0a,
	0x0b, 0x6c, 0x61, 0x73, 0x74, 0x52, 0x75, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x18, 0x09, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x0b, 0x6c, 0x61, 0x73, 0x74, 0x52, 0x75, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x12,
	0x20, 0x0a, 0x0b, 0x6e, 0x65, 0x78, 0x74, 0x52, 0x75, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x18, 0x0a,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x6e, 0x65, 0x78, 0x74, 0x52, 0x75, 0x6e, 0x54, 0x69, 0x6d,
	0x65, 0x12, 0x26, 0x0a, 0x0e, 0x69, 0x64, 0x65, 0x6d, 0x70, 0x6f, 0x74, 0x65, 0x6e, 0x63, 0x79,
	0x4b, 0x65, 0x79, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x69, 0x64, 0x65, 0x6d, 0x70,
	0x6f, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x4b, 0x65, 0x79, 0x12, 0x1c, 0x0a, 0x09, 0x63, 0x72, 0x65,
	0x61, 0x74, 0x65, 0x64, 0x4f, 0x6e, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x63, 0x72,
	0x65, 0x61, 0x74, 0x65, 0x64, 0x4f, 0x6e, 0x1a, 0x37, 0x0a, 0x09, 0x54, 0x61, 0x67, 0x73, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01,
	0x1a, 0x3e, 0x0a, 0x10, 0x50, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65, 0x54, 0x61, 0x67, 0x73, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01,
	0x22, 0x65, 0x0a, 0x15, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75,
	0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x2e, 0x0a, 0x08, 0x73, 0x63, 0x68,
	0x65, 0x64, 0x75, 0x6c, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x73, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52,
	0x08, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x72, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x72, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x22, 0x5d, 0x0a, 0x17, 0x43, 0x72, 0x65, 0x61, 0x74,
	0x65, 0x64, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x6f, 0x6f, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x04, 0x6e, 0x6f, 0x6f, 0x70, 0x12, 0x2e, 0x0a, 0x08, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75,
	0x6c, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64,
	0x75, 0x6c, 0x65, 0x2e, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x08, 0x73, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x22, 0x43, 0x0a, 0x13, 0x52, 0x65, 0x61, 0x64, 0x53, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a,
	0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x1c, 0x0a,
	0x09, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x09, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x22, 0x46, 0x0a, 0x14, 0x52,
	0x65, 0x61, 0x64, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x12, 0x2e, 0x0a, 0x08, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x2e, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x08, 0x73, 0x63, 0x68, 0x65, 0x64,
	0x75, 0x6c, 0x65, 0x22, 0xed, 0x01, 0x0a, 0x16, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x53, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e,
	0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x3e,
	0x0a, 0x04, 0x74, 0x61, 0x67, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x73,
	0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x53, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x54,
	0x61, 0x67, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x04, 0x74, 0x61, 0x67, 0x73, 0x12, 0x14,
	0x0a, 0x05, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x6c,
	0x69, 0x6d, 0x69, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x63, 0x75, 0x72, 0x73, 0x6f, 0x72, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x63, 0x75, 0x72, 0x73, 0x6f, 0x72, 0x12, 0x1c, 0x0a, 0x09,
	0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x09, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x1a, 0x37, 0x0a, 0x09, 0x54, 0x61,
	0x67, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a,
	0x02, 0x38, 0x01, 0x22, 0x63, 0x0a, 0x17, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x53, 0x63, 0x68,
	0x65, 0x64, 0x75, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x16,
	0x0a, 0x06, 0x63, 0x75, 0x72, 0x73, 0x6f, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06,
	0x63, 0x75, 0x72, 0x73, 0x6f, 0x72, 0x12, 0x30, 0x0a, 0x09, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75,
	0x6c, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x73, 0x63, 0x68, 0x65,
	0x64, 0x75, 0x6c, 0x65, 0x2e, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x09, 0x73,
	0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x73, 0x22, 0x45, 0x0a, 0x15, 0x44, 0x65, 0x6c, 0x65,
	0x74, 0x65, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69,
	0x64, 0x12, 0x1c, 0x0a, 0x09, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64, 0x22,
	0x18, 0x0a, 0x16, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x32, 0xe5, 0x02, 0x0a, 0x09, 0x53, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x73, 0x12, 0x56, 0x0a, 0x0e, 0x43, 0x72, 0x65, 0x61, 0x74,
	0x65, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x12, 0x1f, 0x2e, 0x73, 0x63, 0x68, 0x65,
	0x64, 0x75, 0x6c, 0x65, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x63, 0x68, 0x65, 0x64,
	0x75, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x21, 0x2e, 0x73, 0x63, 0x68,
	0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x53, 0x63, 0x68,
	0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12,
	0x4f, 0x0a, 0x0c, 0x52, 0x65, 0x61, 0x64, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x12,
	0x1d, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x52, 0x65, 0x61, 0x64, 0x53,
	0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e,
	0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x52, 0x65, 0x61, 0x64, 0x53, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00,
	0x12, 0x58, 0x0a, 0x0f, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75,
	0x6c, 0x65, 0x73, 0x12, 0x20, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x53,
	0x65, 0x61, 0x72, 0x63, 0x68, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x73, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x21, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x2e, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x73,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x55, 0x0a, 0x0e, 0x44, 0x65,
	0x6c, 0x65, 0x74, 0x65, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x12, 0x1f, 0x2e, 0x73,
	0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e,
	0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53,
	0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x42, 0x45, 0x5a, 0x43, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x72, 0x65, 0x73, 0x6f, 0x6e, 0x61, 0x74, 0x65, 0x68, 0x71, 0x2f, 0x72, 0x65, 0x73, 0x6f, 0x6e,
	0x61, 0x74, 0x65, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x70,
	0x2f, 0x73, 0x75, 0x62, 0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x73, 0x2f, 0x61, 0x70, 0x69, 0x2f,
	0x67, 0x72, 0x70, 0x63, 0x2f, 0x61, 0x70, 0x69, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescOnce sync.Once
	file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescData = file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDesc
)

func file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescGZIP() []byte {
	file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescOnce.Do(func() {
		file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescData = protoimpl.X.CompressGZIP(file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescData)
	})
	return file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDescData
}

var file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes = make([]protoimpl.MessageInfo, 14)
var file_internal_app_subsystems_api_grpc_api_schedule_proto_goTypes = []interface{}{
	(*PromiseValue)(nil),            // 0: schedule.PromiseValue
	(*Schedule)(nil),                // 1: schedule.Schedule
	(*CreateScheduleRequest)(nil),   // 2: schedule.CreateScheduleRequest
	(*CreatedScheduleResponse)(nil), // 3: schedule.CreatedScheduleResponse
	(*ReadScheduleRequest)(nil),     // 4: schedule.ReadScheduleRequest
	(*ReadScheduleResponse)(nil),    // 5: schedule.ReadScheduleResponse
	(*SearchSchedulesRequest)(nil),  // 6: schedule.SearchSchedulesRequest
	(*SearchSchedulesResponse)(nil), // 7: schedule.SearchSchedulesResponse
	(*DeleteScheduleRequest)(nil),   // 8: schedule.DeleteScheduleRequest
	(*DeleteScheduleResponse)(nil),  // 9: schedule.DeleteScheduleResponse
	nil,                             // 10: schedule.PromiseValue.HeadersEntry
	nil,                             // 11: schedule.Schedule.TagsEntry
	nil,                             // 12: schedule.Schedule.PromiseTagsEntry
	nil,                             // 13: schedule.SearchSchedulesRequest.TagsEntry
}
var file_internal_app_subsystems_api_grpc_api_schedule_proto_depIdxs = []int32{
	10, // 0: schedule.PromiseValue.headers:type_name -> schedule.PromiseValue.HeadersEntry
	11, // 1: schedule.Schedule.tags:type_name -> schedule.Schedule.TagsEntry
	0,  // 2: schedule.Schedule.promiseParam:type_name -> schedule.PromiseValue
	12, // 3: schedule.Schedule.promiseTags:type_name -> schedule.Schedule.PromiseTagsEntry
	1,  // 4: schedule.CreateScheduleRequest.schedule:type_name -> schedule.Schedule
	1,  // 5: schedule.CreatedScheduleResponse.schedule:type_name -> schedule.Schedule
	1,  // 6: schedule.ReadScheduleResponse.schedule:type_name -> schedule.Schedule
	13, // 7: schedule.SearchSchedulesRequest.tags:type_name -> schedule.SearchSchedulesRequest.TagsEntry
	1,  // 8: schedule.SearchSchedulesResponse.schedules:type_name -> schedule.Schedule
	2,  // 9: schedule.Schedules.CreateSchedule:input_type -> schedule.CreateScheduleRequest
	4,  // 10: schedule.Schedules.ReadSchedule:input_type -> schedule.ReadScheduleRequest
	6,  // 11: schedule.Schedules.SearchSchedules:input_type -> schedule.SearchSchedulesRequest
	8,  // 12: schedule.Schedules.DeleteSchedule:input_type -> schedule.DeleteScheduleRequest
	3,  // 13: schedule.Schedules.CreateSchedule:output_type -> schedule.CreatedScheduleResponse
	5,  // 14: schedule.Schedules.ReadSchedule:output_type -> schedule.ReadScheduleResponse
	7,  // 15: schedule.Schedules.SearchSchedules:output_type -> schedule.SearchSchedulesResponse
	9,  // 16: schedule.Schedules.DeleteSchedule:output_type -> schedule.DeleteScheduleResponse
	13, // [13:17] is the sub-list for method output_type
	9,  // [9:13] is the sub-list for method input_type
	9,  // [9:9] is the sub-list for extension type_name
	9,  // [9:9] is the sub-list for extension extendee
	0,  // [0:9] is the sub-list for field type_name
}

func init() { file_internal_app_subsystems_api_grpc_api_schedule_proto_init() }
func file_internal_app_subsystems_api_grpc_api_schedule_proto_init() {
	if File_internal_app_subsystems_api_grpc_api_schedule_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PromiseValue); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Schedule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateScheduleRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreatedScheduleResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadScheduleRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadScheduleResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SearchSchedulesRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SearchSchedulesResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteScheduleRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteScheduleResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   14,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_internal_app_subsystems_api_grpc_api_schedule_proto_goTypes,
		DependencyIndexes: file_internal_app_subsystems_api_grpc_api_schedule_proto_depIdxs,
		MessageInfos:      file_internal_app_subsystems_api_grpc_api_schedule_proto_msgTypes,
	}.Build()
	File_internal_app_subsystems_api_grpc_api_schedule_proto = out.File
	file_internal_app_subsystems_api_grpc_api_schedule_proto_rawDesc = nil
	file_internal_app_subsystems_api_grpc_api_schedule_proto_goTypes = nil
	file_internal_app_subsystems_api_grpc_api_schedule_proto_depIdxs = nil
}
