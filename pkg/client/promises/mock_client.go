// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/client/promises/openapi.go

// Package promises is a generated GoMock package.
package promises

import (
	context "context"
	io "io"
	http "net/http"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockHttpRequestDoer is a mock of HttpRequestDoer interface.
type MockHttpRequestDoer struct {
	ctrl     *gomock.Controller
	recorder *MockHttpRequestDoerMockRecorder
}

// MockHttpRequestDoerMockRecorder is the mock recorder for MockHttpRequestDoer.
type MockHttpRequestDoerMockRecorder struct {
	mock *MockHttpRequestDoer
}

// NewMockHttpRequestDoer creates a new mock instance.
func NewMockHttpRequestDoer(ctrl *gomock.Controller) *MockHttpRequestDoer {
	mock := &MockHttpRequestDoer{ctrl: ctrl}
	mock.recorder = &MockHttpRequestDoerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHttpRequestDoer) EXPECT() *MockHttpRequestDoerMockRecorder {
	return m.recorder
}

// Do mocks base method.
func (m *MockHttpRequestDoer) Do(req *http.Request) (*http.Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Do", req)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Do indicates an expected call of Do.
func (mr *MockHttpRequestDoerMockRecorder) Do(req interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Do", reflect.TypeOf((*MockHttpRequestDoer)(nil).Do), req)
}

// MockClientInterface is a mock of ClientInterface interface.
type MockClientInterface struct {
	ctrl     *gomock.Controller
	recorder *MockClientInterfaceMockRecorder
}

// MockClientInterfaceMockRecorder is the mock recorder for MockClientInterface.
type MockClientInterfaceMockRecorder struct {
	mock *MockClientInterface
}

// NewMockClientInterface creates a new mock instance.
func NewMockClientInterface(ctrl *gomock.Controller) *MockClientInterface {
	mock := &MockClientInterface{ctrl: ctrl}
	mock.recorder = &MockClientInterfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClientInterface) EXPECT() *MockClientInterfaceMockRecorder {
	return m.recorder
}

// CreatePromise mocks base method.
func (m *MockClientInterface) CreatePromise(ctx context.Context, params *CreatePromiseParams, body CreatePromiseJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params, body}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreatePromise", varargs...)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreatePromise indicates an expected call of CreatePromise.
func (mr *MockClientInterfaceMockRecorder) CreatePromise(ctx, params, body interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params, body}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreatePromise", reflect.TypeOf((*MockClientInterface)(nil).CreatePromise), varargs...)
}

// CreatePromiseWithBody mocks base method.
func (m *MockClientInterface) CreatePromiseWithBody(ctx context.Context, params *CreatePromiseParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params, contentType, body}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreatePromiseWithBody", varargs...)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreatePromiseWithBody indicates an expected call of CreatePromiseWithBody.
func (mr *MockClientInterfaceMockRecorder) CreatePromiseWithBody(ctx, params, contentType, body interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params, contentType, body}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreatePromiseWithBody", reflect.TypeOf((*MockClientInterface)(nil).CreatePromiseWithBody), varargs...)
}

// GetPromise mocks base method.
func (m *MockClientInterface) GetPromise(ctx context.Context, id Id, reqEditors ...RequestEditorFn) (*http.Response, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, id}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetPromise", varargs...)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPromise indicates an expected call of GetPromise.
func (mr *MockClientInterfaceMockRecorder) GetPromise(ctx, id interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, id}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPromise", reflect.TypeOf((*MockClientInterface)(nil).GetPromise), varargs...)
}

// PatchPromisesId mocks base method.
func (m *MockClientInterface) PatchPromisesId(ctx context.Context, id Id, params *PatchPromisesIdParams, body PatchPromisesIdJSONRequestBody, reqEditors ...RequestEditorFn) (*http.Response, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, id, params, body}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PatchPromisesId", varargs...)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PatchPromisesId indicates an expected call of PatchPromisesId.
func (mr *MockClientInterfaceMockRecorder) PatchPromisesId(ctx, id, params, body interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, id, params, body}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PatchPromisesId", reflect.TypeOf((*MockClientInterface)(nil).PatchPromisesId), varargs...)
}

// PatchPromisesIdWithBody mocks base method.
func (m *MockClientInterface) PatchPromisesIdWithBody(ctx context.Context, id Id, params *PatchPromisesIdParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*http.Response, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, id, params, contentType, body}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PatchPromisesIdWithBody", varargs...)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PatchPromisesIdWithBody indicates an expected call of PatchPromisesIdWithBody.
func (mr *MockClientInterfaceMockRecorder) PatchPromisesIdWithBody(ctx, id, params, contentType, body interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, id, params, contentType, body}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PatchPromisesIdWithBody", reflect.TypeOf((*MockClientInterface)(nil).PatchPromisesIdWithBody), varargs...)
}

// SearchPromises mocks base method.
func (m *MockClientInterface) SearchPromises(ctx context.Context, params *SearchPromisesParams, reqEditors ...RequestEditorFn) (*http.Response, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SearchPromises", varargs...)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SearchPromises indicates an expected call of SearchPromises.
func (mr *MockClientInterfaceMockRecorder) SearchPromises(ctx, params interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SearchPromises", reflect.TypeOf((*MockClientInterface)(nil).SearchPromises), varargs...)
}

// MockClientWithResponsesInterface is a mock of ClientWithResponsesInterface interface.
type MockClientWithResponsesInterface struct {
	ctrl     *gomock.Controller
	recorder *MockClientWithResponsesInterfaceMockRecorder
}

// MockClientWithResponsesInterfaceMockRecorder is the mock recorder for MockClientWithResponsesInterface.
type MockClientWithResponsesInterfaceMockRecorder struct {
	mock *MockClientWithResponsesInterface
}

// NewMockClientWithResponsesInterface creates a new mock instance.
func NewMockClientWithResponsesInterface(ctrl *gomock.Controller) *MockClientWithResponsesInterface {
	mock := &MockClientWithResponsesInterface{ctrl: ctrl}
	mock.recorder = &MockClientWithResponsesInterfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClientWithResponsesInterface) EXPECT() *MockClientWithResponsesInterfaceMockRecorder {
	return m.recorder
}

// CreatePromiseWithBodyWithResponse mocks base method.
func (m *MockClientWithResponsesInterface) CreatePromiseWithBodyWithResponse(ctx context.Context, params *CreatePromiseParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*CreatePromiseResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params, contentType, body}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreatePromiseWithBodyWithResponse", varargs...)
	ret0, _ := ret[0].(*CreatePromiseResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreatePromiseWithBodyWithResponse indicates an expected call of CreatePromiseWithBodyWithResponse.
func (mr *MockClientWithResponsesInterfaceMockRecorder) CreatePromiseWithBodyWithResponse(ctx, params, contentType, body interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params, contentType, body}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreatePromiseWithBodyWithResponse", reflect.TypeOf((*MockClientWithResponsesInterface)(nil).CreatePromiseWithBodyWithResponse), varargs...)
}

// CreatePromiseWithResponse mocks base method.
func (m *MockClientWithResponsesInterface) CreatePromiseWithResponse(ctx context.Context, params *CreatePromiseParams, body CreatePromiseJSONRequestBody, reqEditors ...RequestEditorFn) (*CreatePromiseResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params, body}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreatePromiseWithResponse", varargs...)
	ret0, _ := ret[0].(*CreatePromiseResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreatePromiseWithResponse indicates an expected call of CreatePromiseWithResponse.
func (mr *MockClientWithResponsesInterfaceMockRecorder) CreatePromiseWithResponse(ctx, params, body interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params, body}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreatePromiseWithResponse", reflect.TypeOf((*MockClientWithResponsesInterface)(nil).CreatePromiseWithResponse), varargs...)
}

// GetPromiseWithResponse mocks base method.
func (m *MockClientWithResponsesInterface) GetPromiseWithResponse(ctx context.Context, id Id, reqEditors ...RequestEditorFn) (*GetPromiseResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, id}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetPromiseWithResponse", varargs...)
	ret0, _ := ret[0].(*GetPromiseResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPromiseWithResponse indicates an expected call of GetPromiseWithResponse.
func (mr *MockClientWithResponsesInterfaceMockRecorder) GetPromiseWithResponse(ctx, id interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, id}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPromiseWithResponse", reflect.TypeOf((*MockClientWithResponsesInterface)(nil).GetPromiseWithResponse), varargs...)
}

// PatchPromisesIdWithBodyWithResponse mocks base method.
func (m *MockClientWithResponsesInterface) PatchPromisesIdWithBodyWithResponse(ctx context.Context, id Id, params *PatchPromisesIdParams, contentType string, body io.Reader, reqEditors ...RequestEditorFn) (*PatchPromisesIdResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, id, params, contentType, body}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PatchPromisesIdWithBodyWithResponse", varargs...)
	ret0, _ := ret[0].(*PatchPromisesIdResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PatchPromisesIdWithBodyWithResponse indicates an expected call of PatchPromisesIdWithBodyWithResponse.
func (mr *MockClientWithResponsesInterfaceMockRecorder) PatchPromisesIdWithBodyWithResponse(ctx, id, params, contentType, body interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, id, params, contentType, body}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PatchPromisesIdWithBodyWithResponse", reflect.TypeOf((*MockClientWithResponsesInterface)(nil).PatchPromisesIdWithBodyWithResponse), varargs...)
}

// PatchPromisesIdWithResponse mocks base method.
func (m *MockClientWithResponsesInterface) PatchPromisesIdWithResponse(ctx context.Context, id Id, params *PatchPromisesIdParams, body PatchPromisesIdJSONRequestBody, reqEditors ...RequestEditorFn) (*PatchPromisesIdResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, id, params, body}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PatchPromisesIdWithResponse", varargs...)
	ret0, _ := ret[0].(*PatchPromisesIdResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PatchPromisesIdWithResponse indicates an expected call of PatchPromisesIdWithResponse.
func (mr *MockClientWithResponsesInterfaceMockRecorder) PatchPromisesIdWithResponse(ctx, id, params, body interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, id, params, body}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PatchPromisesIdWithResponse", reflect.TypeOf((*MockClientWithResponsesInterface)(nil).PatchPromisesIdWithResponse), varargs...)
}

// SearchPromisesWithResponse mocks base method.
func (m *MockClientWithResponsesInterface) SearchPromisesWithResponse(ctx context.Context, params *SearchPromisesParams, reqEditors ...RequestEditorFn) (*SearchPromisesResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range reqEditors {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SearchPromisesWithResponse", varargs...)
	ret0, _ := ret[0].(*SearchPromisesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SearchPromisesWithResponse indicates an expected call of SearchPromisesWithResponse.
func (mr *MockClientWithResponsesInterfaceMockRecorder) SearchPromisesWithResponse(ctx, params interface{}, reqEditors ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, reqEditors...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SearchPromisesWithResponse", reflect.TypeOf((*MockClientWithResponsesInterface)(nil).SearchPromisesWithResponse), varargs...)
}
