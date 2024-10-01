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
	go install github.com/deepmap/oapi-codegen/v2/cmd/oapi-codegen@latest

.PHONY: gen-openapi
gen-openapi:
	oapi-codegen -generate types,client -package promises ./api/promises-openapi.yml > pkg/client/promises/openapi.go
	oapi-codegen -generate types,client -package schedules ./api/schedules-openapi.yml > pkg/client/schedules/openapi.go

.PHONY: gen-mock
gen-mock:
	mockgen -source=pkg/client/promises/openapi.go -destination=pkg/client/promises/mock_client.go -package promises
	mockgen -source=pkg/client/schedules/openapi.go -destination=pkg/client/schedules/mock_client.go -package schedules
