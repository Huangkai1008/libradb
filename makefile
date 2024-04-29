test:
	go test -v ./...
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out
clean:
	rm -f coverage.out
lint:
	golangci-lint run