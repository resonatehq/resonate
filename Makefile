.PHONY: deps
deps:
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest
	go install go.uber.org/mock/mockgen@latest

.PHONY: gen-openapi
gen-openapi:
	oapi-codegen -generate types,client -package v1 -o pkg/client/v1/v1.go ./api/openapi.yml

.PHONY: gen-mock
gen-mock: gen-openapi
	mockgen -package v1 -source=pkg/client/v1/v1.go -destination=pkg/client/v1/v1_mock.go

.PHONY: test
test:
	go test -v -coverprofile=coverage.out -coverpkg=./... ./... && \
	go test -C ./internal/app/plugins/sqs -v ./... && \
	go test -C ./internal/app/plugins/kafka -v ./... && \
	go test -C ./internal/app/subsystems/api/kafka -v ./...
