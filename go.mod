module github.com/longhorn/longhorn-spdk-engine

go 1.21

require (
	github.com/RoaringBitmap/roaring v1.2.3
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/longhorn/go-common-libs v0.0.0-20231201083854-bf0165ef3a22
	github.com/longhorn/go-spdk-helper v0.0.0-20231115022355-4dcc6f22bc08
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.9.3
	go.uber.org/multierr v1.11.0
	golang.org/x/net v0.7.0
	golang.org/x/sys v0.11.0
	google.golang.org/grpc v1.53.0
	google.golang.org/protobuf v1.28.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

require (
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/c9s/goprocinfo v0.0.0-20210130143923-c95fcf8c64a8 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/longhorn/nsfilelock v0.0.0-20200723175406-fa7c83ad0003 // indirect
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
)

replace github.com/longhorn/go-common-libs v0.0.0-20231201083854-bf0165ef3a22 => github.com/derekbit/go-common-libs v0.0.0-20231206172621-5a55cbc3f8a8
