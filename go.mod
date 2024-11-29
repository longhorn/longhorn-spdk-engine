module github.com/longhorn/longhorn-spdk-engine

go 1.22.7

toolchain go1.23.3

replace github.com/longhorn/types v0.0.0-20241123075624-48c550af4eab => github.com/chanyilin/types v0.0.0-20241127133205-f9c284b13214

replace github.com/longhorn/go-spdk-helper v0.0.0-20241124090314-c396ae715a7f => github.com/chanyilin/go-spdk-helper v0.0.0-20241125130336-15bee7b0eec2

require (
	github.com/0xPolygon/polygon-edge v1.3.3
	github.com/google/uuid v1.6.0
	github.com/longhorn/backupstore v0.0.0-20241124092526-138305866a87
	github.com/longhorn/go-common-libs v0.0.0-20241124035508-d6221574e626
	github.com/longhorn/go-spdk-helper v0.0.0-20241124090314-c396ae715a7f
	github.com/longhorn/types v0.0.0-20241123075624-48c550af4eab
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.9.3
	go.uber.org/multierr v1.11.0
	golang.org/x/net v0.31.0
	google.golang.org/grpc v1.68.0
	google.golang.org/protobuf v1.35.2
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	k8s.io/apimachinery v0.31.3
)

require (
	github.com/RoaringBitmap/roaring v1.9.4 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.16.0 // indirect
	github.com/c9s/goprocinfo v0.0.0-20210130143923-c95fcf8c64a8 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/gammazero/deque v1.0.0 // indirect
	github.com/gammazero/workerpool v1.1.3 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/moby/sys/mountinfo v0.7.2 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/runc v1.2.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.60.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/slok/goresilience v0.2.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	golang.org/x/exp v0.0.0-20241108190413-2d47ceb2692f // indirect
	golang.org/x/sys v0.27.0 // indirect
	golang.org/x/text v0.20.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241118233622-e639e219e697 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/mount-utils v0.31.3 // indirect
	k8s.io/utils v0.0.0-20241104163129-6fe5fd82f078 // indirect
)
