.PHONY: gen-proto
gen-proto: 
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/app/subsystems/api/grpc/api/promise.proto