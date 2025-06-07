module github.com/owlmessenger/owl

go 1.22

toolchain go1.24.1

require (
	github.com/dchest/siphash v1.2.3
	github.com/gotvc/got v0.0.5-0.20250607143630-2d373fdf704c
	github.com/jmoiron/sqlx v1.3.5
	github.com/mattn/go-sqlite3 v1.14.12
	github.com/sourcegraph/jsonrpc2 v0.1.0
	github.com/spf13/cobra v1.2.1
	github.com/stretchr/testify v1.8.4
	go.brendoncarroll.net/exp v0.0.0-20250112210235-9d4b62bdbd02
	go.brendoncarroll.net/p2p v0.0.0-20241118201502-2abd1a6f58e7
	go.brendoncarroll.net/state v0.0.0-20241118200920-627c9c196901
	go.brendoncarroll.net/stdctx v0.0.0-20241118190518-40d09f4d11e7
	go.brendoncarroll.net/tai64 v0.0.0-20241118171318-6e12d283d5e4
	go.inet256.org/diet256 v0.0.4-0.20250607160832-38815b72af25
	go.inet256.org/inet256 v0.0.8-0.20250106023139-eb36ad7a3f3f
	go.uber.org/zap v1.24.0
	golang.org/x/crypto v0.28.0
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1
	golang.org/x/sync v0.8.0
	google.golang.org/grpc v1.69.2
	google.golang.org/protobuf v1.35.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/pprof v0.0.0-20210407192527-94a9f03dee38 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/onsi/ginkgo/v2 v2.9.5 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/quic-go/qtls-go1-20 v0.3.4 // indirect
	github.com/quic-go/quic-go v0.37.4 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	lukechampine.com/blake3 v1.1.7 // indirect
)

replace github.com/lucas-clemente/quic-go => github.com/quic-go/quic-go v0.38.0
