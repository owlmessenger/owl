
protobuf:
	cd ./pkg/feeds/internal/wire && ./build_protobuf.sh

test:
	go test -v ./...

