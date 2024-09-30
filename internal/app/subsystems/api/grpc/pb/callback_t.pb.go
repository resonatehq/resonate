// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.23.3
// source: internal/app/subsystems/api/grpc/pb/callback_t.proto

package pb

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

type Callback struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id        string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	PromiseId string `protobuf:"bytes,2,opt,name=promiseId,proto3" json:"promiseId,omitempty"`
	Timeout   int64  `protobuf:"varint,3,opt,name=timeout,proto3" json:"timeout,omitempty"`
	CreatedOn int64  `protobuf:"varint,4,opt,name=createdOn,proto3" json:"createdOn,omitempty"`
}

func (x *Callback) Reset() {
	*x = Callback{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Callback) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Callback) ProtoMessage() {}

func (x *Callback) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Callback.ProtoReflect.Descriptor instead.
func (*Callback) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescGZIP(), []int{0}
}

func (x *Callback) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Callback) GetPromiseId() string {
	if x != nil {
		return x.PromiseId
	}
	return ""
}

func (x *Callback) GetTimeout() int64 {
	if x != nil {
		return x.Timeout
	}
	return 0
}

func (x *Callback) GetCreatedOn() int64 {
	if x != nil {
		return x.CreatedOn
	}
	return 0
}

type Recv struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Recv:
	//
	//	*Recv_Logical
	//	*Recv_Physical
	Recv isRecv_Recv `protobuf_oneof:"recv"`
}

func (x *Recv) Reset() {
	*x = Recv{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Recv) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Recv) ProtoMessage() {}

func (x *Recv) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Recv.ProtoReflect.Descriptor instead.
func (*Recv) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescGZIP(), []int{1}
}

func (m *Recv) GetRecv() isRecv_Recv {
	if m != nil {
		return m.Recv
	}
	return nil
}

func (x *Recv) GetLogical() string {
	if x, ok := x.GetRecv().(*Recv_Logical); ok {
		return x.Logical
	}
	return ""
}

func (x *Recv) GetPhysical() *PhysicalRecv {
	if x, ok := x.GetRecv().(*Recv_Physical); ok {
		return x.Physical
	}
	return nil
}

type isRecv_Recv interface {
	isRecv_Recv()
}

type Recv_Logical struct {
	Logical string `protobuf:"bytes,1,opt,name=logical,proto3,oneof"`
}

type Recv_Physical struct {
	Physical *PhysicalRecv `protobuf:"bytes,2,opt,name=physical,proto3,oneof"`
}

func (*Recv_Logical) isRecv_Recv() {}

func (*Recv_Physical) isRecv_Recv() {}

type PhysicalRecv struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Type string `protobuf:"bytes,1,opt,name=type,proto3" json:"type,omitempty"`
	Data []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *PhysicalRecv) Reset() {
	*x = PhysicalRecv{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PhysicalRecv) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PhysicalRecv) ProtoMessage() {}

func (x *PhysicalRecv) ProtoReflect() protoreflect.Message {
	mi := &file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PhysicalRecv.ProtoReflect.Descriptor instead.
func (*PhysicalRecv) Descriptor() ([]byte, []int) {
	return file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescGZIP(), []int{2}
}

func (x *PhysicalRecv) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *PhysicalRecv) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

var File_internal_app_subsystems_api_grpc_pb_callback_t_proto protoreflect.FileDescriptor

var file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDesc = []byte{
	0x0a, 0x34, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x70, 0x2f, 0x73,
	0x75, 0x62, 0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x73, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x67, 0x72,
	0x70, 0x63, 0x2f, 0x70, 0x62, 0x2f, 0x63, 0x61, 0x6c, 0x6c, 0x62, 0x61, 0x63, 0x6b, 0x5f, 0x74,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0a, 0x63, 0x61, 0x6c, 0x6c, 0x62, 0x61, 0x63, 0x6b,
	0x5f, 0x74, 0x22, 0x70, 0x0a, 0x08, 0x43, 0x61, 0x6c, 0x6c, 0x62, 0x61, 0x63, 0x6b, 0x12, 0x0e,
	0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x1c,
	0x0a, 0x09, 0x70, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65, 0x49, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x09, 0x70, 0x72, 0x6f, 0x6d, 0x69, 0x73, 0x65, 0x49, 0x64, 0x12, 0x18, 0x0a, 0x07,
	0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x74,
	0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x12, 0x1c, 0x0a, 0x09, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65,
	0x64, 0x4f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x63, 0x72, 0x65, 0x61, 0x74,
	0x65, 0x64, 0x4f, 0x6e, 0x22, 0x62, 0x0a, 0x04, 0x52, 0x65, 0x63, 0x76, 0x12, 0x1a, 0x0a, 0x07,
	0x6c, 0x6f, 0x67, 0x69, 0x63, 0x61, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52,
	0x07, 0x6c, 0x6f, 0x67, 0x69, 0x63, 0x61, 0x6c, 0x12, 0x36, 0x0a, 0x08, 0x70, 0x68, 0x79, 0x73,
	0x69, 0x63, 0x61, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x63, 0x61, 0x6c,
	0x6c, 0x62, 0x61, 0x63, 0x6b, 0x5f, 0x74, 0x2e, 0x50, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c,
	0x52, 0x65, 0x63, 0x76, 0x48, 0x00, 0x52, 0x08, 0x70, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c,
	0x42, 0x06, 0x0a, 0x04, 0x72, 0x65, 0x63, 0x76, 0x22, 0x36, 0x0a, 0x0c, 0x50, 0x68, 0x79, 0x73,
	0x69, 0x63, 0x61, 0x6c, 0x52, 0x65, 0x63, 0x76, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x12, 0x0a, 0x04,
	0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61,
	0x42, 0x44, 0x5a, 0x42, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x72,
	0x65, 0x73, 0x6f, 0x6e, 0x61, 0x74, 0x65, 0x68, 0x71, 0x2f, 0x72, 0x65, 0x73, 0x6f, 0x6e, 0x61,
	0x74, 0x65, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x70, 0x2f,
	0x73, 0x75, 0x62, 0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x73, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x67,
	0x72, 0x70, 0x63, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescOnce sync.Once
	file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescData = file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDesc
)

func file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescGZIP() []byte {
	file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescOnce.Do(func() {
		file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescData = protoimpl.X.CompressGZIP(file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescData)
	})
	return file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDescData
}

var file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_internal_app_subsystems_api_grpc_pb_callback_t_proto_goTypes = []interface{}{
	(*Callback)(nil),     // 0: callback_t.Callback
	(*Recv)(nil),         // 1: callback_t.Recv
	(*PhysicalRecv)(nil), // 2: callback_t.PhysicalRecv
}
var file_internal_app_subsystems_api_grpc_pb_callback_t_proto_depIdxs = []int32{
	2, // 0: callback_t.Recv.physical:type_name -> callback_t.PhysicalRecv
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_internal_app_subsystems_api_grpc_pb_callback_t_proto_init() }
func file_internal_app_subsystems_api_grpc_pb_callback_t_proto_init() {
	if File_internal_app_subsystems_api_grpc_pb_callback_t_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Callback); i {
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
		file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Recv); i {
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
		file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PhysicalRecv); i {
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
	file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes[1].OneofWrappers = []interface{}{
		(*Recv_Logical)(nil),
		(*Recv_Physical)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_internal_app_subsystems_api_grpc_pb_callback_t_proto_goTypes,
		DependencyIndexes: file_internal_app_subsystems_api_grpc_pb_callback_t_proto_depIdxs,
		MessageInfos:      file_internal_app_subsystems_api_grpc_pb_callback_t_proto_msgTypes,
	}.Build()
	File_internal_app_subsystems_api_grpc_pb_callback_t_proto = out.File
	file_internal_app_subsystems_api_grpc_pb_callback_t_proto_rawDesc = nil
	file_internal_app_subsystems_api_grpc_pb_callback_t_proto_goTypes = nil
	file_internal_app_subsystems_api_grpc_pb_callback_t_proto_depIdxs = nil
}
