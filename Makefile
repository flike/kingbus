COMMIT_HASH=$(shell git rev-parse --short HEAD || echo "GitNotFound")
BUILD_DATE=$(shell date '+%Y-%m-%d %H:%M:%S')
PKG_NAME=kingbus.$(shell date "+%Y%m%d")

all: kingbus

kingbus:
	$(info COMMIT_HASH: $(COMMIT_HASH))
	cd ./cmd/kingbus/ && CGO_ENABLED=0 go build -ldflags "-X \"main.BuildVersion=${COMMIT_HASH}\" -X \"main.BuildDate=$(BUILD_DATE)\"" -o ../../build/bin/kingbus
	chmod +x ./build/bin/kingbus
	cp etc/kingbus.yaml ./build/bin
clean:
	@rm -rf build

test:
	cd raft/ && go test -v
	cd raft/membership && go test -v
	cd server && go test -v
	cd storage && go test -v

package:kingbus
	mkdir -p build/${PKG_NAME}/bin
	cp build/bin/kingbus build/${PKG_NAME}/bin
	cp etc/kingbus.yaml build/${PKG_NAME}/bin
	cd build/${PKG_NAME} && tar -czvf ../${PKG_NAME}.tgz .
	rm -rf build/${PKG_NAME}
	rm -rf build/bin
