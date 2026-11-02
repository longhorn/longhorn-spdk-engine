package spdk

import (
	"context"
	"fmt"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func newQosTestEngine() *Engine {
	return NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
}

func (s *TestSuite) TestQosLimitProtoConversion(c *C) {
	fmt.Println("Testing qosLimit conversion from and to proto")

	c.Assert(qosLimitFromProto(nil), Equals, qosLimit{})

	limit := qosLimitFromProto(&spdkrpc.VolumeQosLimit{RwIosPerSec: 1000, RwMbytesPerSec: 100})
	c.Assert(limit, Equals, qosLimit{rwIOsPerSec: 1000, rwMBPerSec: 100})

	proto := limit.toProto()
	c.Assert(proto.RwIosPerSec, Equals, int64(1000))
	c.Assert(proto.RwMbytesPerSec, Equals, int64(100))
}

func (s *TestSuite) TestEngineSetQosLimit(c *C) {
	tests := []struct {
		name  string
		state lhtypes.InstanceState
		limit *spdkrpc.VolumeQosLimit
		want  qosLimit
	}{
		{
			name:  "pending engine records the limit",
			state: lhtypes.InstanceStatePending,
			limit: &spdkrpc.VolumeQosLimit{RwIosPerSec: 1000, RwMbytesPerSec: 100},
			want:  qosLimit{rwIOsPerSec: 1000, rwMBPerSec: 100},
		},
		{
			name:  "error engine records the limit",
			state: lhtypes.InstanceStateError,
			limit: &spdkrpc.VolumeQosLimit{RwIosPerSec: 1000, RwMbytesPerSec: 100},
			want:  qosLimit{rwIOsPerSec: 1000, rwMBPerSec: 100},
		},
		{
			name:  "nil limit clears the recorded limit",
			state: lhtypes.InstanceStatePending,
			limit: nil,
			want:  qosLimit{},
		},
	}

	for _, tc := range tests {
		fmt.Println("Testing SetQosLimit:", tc.name)

		e := newQosTestEngine()
		e.State = tc.state
		e.qosLimit = qosLimit{rwIOsPerSec: 500, rwMBPerSec: 50}
		e.appliedQosLimit = qosLimit{rwIOsPerSec: 500, rwMBPerSec: 50}

		// A nil SPDK client proves SPDK is not called when the engine is not running.
		err := e.SetQosLimit(nil, tc.limit)
		c.Assert(err, IsNil, Commentf("case=%s", tc.name))
		c.Assert(e.qosLimit, Equals, tc.want, Commentf("case=%s", tc.name))
		c.Assert(e.appliedQosLimit, Equals, qosLimit{rwIOsPerSec: 500, rwMBPerSec: 50}, Commentf("case=%s", tc.name))
	}
}

func (s *TestSuite) TestEngineApplyQosLimitSkipsWhenAlreadyApplied(c *C) {
	fmt.Println("Testing Engine.applyQosLimitNoLock skips SPDK when the limit is already applied")

	e := newQosTestEngine()

	// Unlimited on a new raid bdev: nothing to apply.
	c.Assert(e.applyQosLimitNoLock(nil), IsNil)

	e.qosLimit = qosLimit{rwIOsPerSec: 1000, rwMBPerSec: 100}
	e.appliedQosLimit = e.qosLimit
	c.Assert(e.applyQosLimitNoLock(nil), IsNil)
	c.Assert(e.appliedQosLimit, Equals, e.qosLimit)
}

func (s *TestSuite) TestEngineGetReportsAppliedQosLimitOnlyWhenRunning(c *C) {
	fmt.Println("Testing Engine.Get reports the applied QoS limit only while running")

	e := newQosTestEngine()
	e.qosLimit = qosLimit{rwIOsPerSec: 2000, rwMBPerSec: 200}
	e.appliedQosLimit = qosLimit{rwIOsPerSec: 1000, rwMBPerSec: 100}

	e.State = lhtypes.InstanceStateRunning
	res := e.Get()
	c.Assert(res.QosLimit, NotNil)
	c.Assert(res.QosLimit.RwIosPerSec, Equals, int64(1000))
	c.Assert(res.QosLimit.RwMbytesPerSec, Equals, int64(100))

	for _, state := range []lhtypes.InstanceState{lhtypes.InstanceStatePending, lhtypes.InstanceStateError} {
		e.State = state
		c.Assert(e.Get().QosLimit, IsNil, Commentf("state=%s", state))
	}
}

func (s *TestSuite) TestEngineValidateQosLimitSyncsAppliedLimitFromSPDK(c *C) {
	fmt.Println("Testing Engine.validateQosLimitNoLock records the limit SPDK reports")

	e := newQosTestEngine()
	e.qosLimit = qosLimit{rwIOsPerSec: 1000, rwMBPerSec: 100}

	bdevRaid := &spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			AssignedRateLimits: spdktypes.AssignedRateLimits{RwIosPerSec: 1000, RwMbytesPerSec: 100},
		},
	}

	// SPDK already enforces the requested limit, so only the record is
	// updated and SPDK is not called.
	updateRequired := false
	e.validateQosLimitNoLock(nil, bdevRaid, &updateRequired)
	c.Assert(e.appliedQosLimit, Equals, e.qosLimit)
	c.Assert(updateRequired, Equals, true)

	updateRequired = false
	e.validateQosLimitNoLock(nil, bdevRaid, &updateRequired)
	c.Assert(updateRequired, Equals, false)
}

func (s *TestSuite) TestServerEngineSetQosLimit(c *C) {
	fmt.Println("Testing Server.EngineSetQosLimit validation and lookup")

	e := newQosTestEngine()
	srv := &Server{
		engineMap: map[string]*Engine{e.Name: e},
	}

	cases := []struct {
		name     string
		req      *spdkrpc.EngineSetQosLimitRequest
		wantCode grpccodes.Code
	}{
		{"rejects empty name", &spdkrpc.EngineSetQosLimitRequest{}, grpccodes.InvalidArgument},
		{"rejects negative IOPS", &spdkrpc.EngineSetQosLimitRequest{Name: e.Name, QosLimit: &spdkrpc.VolumeQosLimit{RwIosPerSec: -1}}, grpccodes.InvalidArgument},
		{"rejects negative bandwidth", &spdkrpc.EngineSetQosLimitRequest{Name: e.Name, QosLimit: &spdkrpc.VolumeQosLimit{RwMbytesPerSec: -1}}, grpccodes.InvalidArgument},
		{"rejects IOPS not a multiple of 1000", &spdkrpc.EngineSetQosLimitRequest{Name: e.Name, QosLimit: &spdkrpc.VolumeQosLimit{RwIosPerSec: 1500}}, grpccodes.InvalidArgument},
		{"returns not found for unknown engine", &spdkrpc.EngineSetQosLimitRequest{Name: "missing"}, grpccodes.NotFound},
	}
	for _, tc := range cases {
		_, err := srv.EngineSetQosLimit(context.Background(), tc.req)
		c.Assert(err, NotNil, Commentf("case=%s", tc.name))
		c.Assert(grpcstatus.Code(err), Equals, tc.wantCode, Commentf("case=%s err=%v", tc.name, err))
	}

	_, err := srv.EngineSetQosLimit(context.Background(), &spdkrpc.EngineSetQosLimitRequest{
		Name:     e.Name,
		QosLimit: &spdkrpc.VolumeQosLimit{RwIosPerSec: 1000, RwMbytesPerSec: 100},
	})
	c.Assert(err, IsNil)
	c.Assert(e.qosLimit, Equals, qosLimit{rwIOsPerSec: 1000, rwMBPerSec: 100})
}

func (s *TestSuite) TestServerEngineCreateRejectsInvalidQosLimit(c *C) {
	fmt.Println("Testing Server.EngineCreate rejects an invalid QoS limit before creating the engine")

	tests := []struct {
		name  string
		limit *spdkrpc.VolumeQosLimit
	}{
		{
			name:  "negative IOPS",
			limit: &spdkrpc.VolumeQosLimit{RwIosPerSec: -1},
		},
		{
			name:  "IOPS not a multiple of 1000",
			limit: &spdkrpc.VolumeQosLimit{RwIosPerSec: 1500},
		},
	}

	for _, tc := range tests {
		srv := &Server{
			engineMap: map[string]*Engine{},
		}

		_, err := srv.EngineCreate(context.Background(), &spdkrpc.EngineCreateRequest{
			Name:       "engine-a",
			VolumeName: "vol-a",
			SpecSize:   10,
			QosLimit:   tc.limit,
		})
		c.Assert(err, NotNil, Commentf("case=%s", tc.name))
		c.Assert(grpcstatus.Code(err), Equals, grpccodes.InvalidArgument, Commentf("case=%s err=%v", tc.name, err))
		c.Assert(len(srv.engineMap), Equals, 0, Commentf("case=%s", tc.name))
	}
}

func (s *TestSuite) TestIsQosLimitChangeInProgressError(c *C) {
	fmt.Println("Testing isQosLimitChangeInProgressError only matches SPDK EAGAIN for bdev_set_qos_limit")

	// Error returned by SPDK when a previous QoS change on the bdev is still in progress.
	eagainErr := fmt.Errorf(`failed to send bdev_set_qos_limit: error sending message, id 9174, method bdev_set_qos_limit, params map[name:vol-e r_mbytes_per_sec:0 rw_ios_per_sec:20000 rw_mbytes_per_sec:200 w_mbytes_per_sec:0]: {"code": -32602,"message": "Failed to configure rate limit: Resource temporarily unavailable"}`)

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "SPDK EAGAIN",
			err:  eagainErr,
			want: true,
		},
		{
			name: "wrapped SPDK EAGAIN",
			err:  fmt.Errorf("failed to set QoS limit on raid bdev vol-e: %w", eagainErr),
			want: true,
		},
		{
			name: "invalid rate limit",
			err:  fmt.Errorf(`failed to send bdev_set_qos_limit: {"code": -32602,"message": "Failed to configure rate limit: Invalid argument"}`),
			want: false,
		},
	}

	for _, tc := range tests {
		c.Assert(isQosLimitChangeInProgressError(tc.err), Equals, tc.want, Commentf("case=%s", tc.name))
	}
}
