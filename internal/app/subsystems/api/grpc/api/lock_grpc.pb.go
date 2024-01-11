// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v4.25.1
// source: internal/app/subsystems/api/grpc/api/lock.proto

package api

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// LocksClient is the client API for Locks service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type LocksClient interface {
	AcquireLock(ctx context.Context, in *AcquireLockRequest, opts ...grpc.CallOption) (*AcquireLockResponse, error)
	BulkHeartbeat(ctx context.Context, in *BulkHeartbeatRequest, opts ...grpc.CallOption) (*BulkHeartbeatResponse, error)
	ReleaseLock(ctx context.Context, in *ReleaseLockRequest, opts ...grpc.CallOption) (*ReleaseLockResponse, error)
}

type locksClient struct {
	cc grpc.ClientConnInterface
}

func NewLocksClient(cc grpc.ClientConnInterface) LocksClient {
	return &locksClient{cc}
}

func (c *locksClient) AcquireLock(ctx context.Context, in *AcquireLockRequest, opts ...grpc.CallOption) (*AcquireLockResponse, error) {
	out := new(AcquireLockResponse)
	err := c.cc.Invoke(ctx, "/lock.Locks/AcquireLock", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *locksClient) BulkHeartbeat(ctx context.Context, in *BulkHeartbeatRequest, opts ...grpc.CallOption) (*BulkHeartbeatResponse, error) {
	out := new(BulkHeartbeatResponse)
	err := c.cc.Invoke(ctx, "/lock.Locks/BulkHeartbeat", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *locksClient) ReleaseLock(ctx context.Context, in *ReleaseLockRequest, opts ...grpc.CallOption) (*ReleaseLockResponse, error) {
	out := new(ReleaseLockResponse)
	err := c.cc.Invoke(ctx, "/lock.Locks/ReleaseLock", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// LocksServer is the server API for Locks service.
// All implementations must embed UnimplementedLocksServer
// for forward compatibility
type LocksServer interface {
	AcquireLock(context.Context, *AcquireLockRequest) (*AcquireLockResponse, error)
	BulkHeartbeat(context.Context, *BulkHeartbeatRequest) (*BulkHeartbeatResponse, error)
	ReleaseLock(context.Context, *ReleaseLockRequest) (*ReleaseLockResponse, error)
	mustEmbedUnimplementedLocksServer()
}

// UnimplementedLocksServer must be embedded to have forward compatible implementations.
type UnimplementedLocksServer struct {
}

func (UnimplementedLocksServer) AcquireLock(context.Context, *AcquireLockRequest) (*AcquireLockResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AcquireLock not implemented")
}
func (UnimplementedLocksServer) BulkHeartbeat(context.Context, *BulkHeartbeatRequest) (*BulkHeartbeatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BulkHeartbeat not implemented")
}
func (UnimplementedLocksServer) ReleaseLock(context.Context, *ReleaseLockRequest) (*ReleaseLockResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReleaseLock not implemented")
}
func (UnimplementedLocksServer) mustEmbedUnimplementedLocksServer() {}

// UnsafeLocksServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to LocksServer will
// result in compilation errors.
type UnsafeLocksServer interface {
	mustEmbedUnimplementedLocksServer()
}

func RegisterLocksServer(s grpc.ServiceRegistrar, srv LocksServer) {
	s.RegisterService(&Locks_ServiceDesc, srv)
}

func _Locks_AcquireLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AcquireLockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LocksServer).AcquireLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lock.Locks/AcquireLock",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LocksServer).AcquireLock(ctx, req.(*AcquireLockRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Locks_BulkHeartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BulkHeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LocksServer).BulkHeartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lock.Locks/BulkHeartbeat",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LocksServer).BulkHeartbeat(ctx, req.(*BulkHeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Locks_ReleaseLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReleaseLockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LocksServer).ReleaseLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lock.Locks/ReleaseLock",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LocksServer).ReleaseLock(ctx, req.(*ReleaseLockRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Locks_ServiceDesc is the grpc.ServiceDesc for Locks service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Locks_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "lock.Locks",
	HandlerType: (*LocksServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AcquireLock",
			Handler:    _Locks_AcquireLock_Handler,
		},
		{
			MethodName: "BulkHeartbeat",
			Handler:    _Locks_BulkHeartbeat_Handler,
		},
		{
			MethodName: "ReleaseLock",
			Handler:    _Locks_ReleaseLock_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "internal/app/subsystems/api/grpc/api/lock.proto",
}
