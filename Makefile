.PHONY: test
test:
	@echo "testing..."
	@go clean -testcache
	@go test ./... -v
