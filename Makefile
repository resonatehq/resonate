.PHONY: gen-proto
gen-proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/app/subsystems/api/grpc/pb/promise_t.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/app/subsystems/api/grpc/pb/promise.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/app/subsystems/api/grpc/pb/callback_t.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/app/subsystems/api/grpc/pb/callback.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/app/subsystems/api/grpc/pb/schedule_t.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/app/subsystems/api/grpc/pb/schedule.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/app/subsystems/api/grpc/pb/lock.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/app/subsystems/api/grpc/pb/task.proto

.PHONY: deps
deps:
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest
	go install go.uber.org/mock/mockgen@latest

.PHONY: gen-openapi
gen-openapi:
	oapi-codegen -generate types,client -package openapi -o pkg/client/openapi/openapi.go ./api/openapi.yml

.PHONY: gen-mock
gen-mock: gen-openapi
	mockgen -package openapi -source=pkg/client/openapi/openapi.go -destination=pkg/client/openapi/mock.go
