all: install


LD_FLAGS = -w -s -X github.com/cosmos/cosmos-sdk/types.DBBackend=goleveldb

BUILD_FLAGS := -ldflags '$(LD_FLAGS)'

build:
	@echo "Building cosmos-pruner"
	@go build -tags goleveldb -mod readonly $(BUILD_FLAGS) -o build/cosmos-pruner main.go

install:
	@echo "Installing cosmos-pruner"
	@go install -tags goleveldb -mod readonly $(BUILD_FLAGS) ./...

clean:
	rm -rf build

.PHONY: all lint test race msan tools clean build
