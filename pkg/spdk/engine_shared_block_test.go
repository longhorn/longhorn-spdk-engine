package spdk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	. "gopkg.in/check.v1"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestGetTargetCntlidRange(c *C) {
	const ip = "10.42.1.21"

	// Without shared block the target keeps accepting a single host.
	for _, port := range []int32{1, 20006, 65535} {
		minCntlid, maxCntlid := getTargetCntlidRange(ip, port, false)
		c.Assert(minCntlid, Equals, getTargetCntlid(ip, port))
		c.Assert(maxCntlid, Equals, minCntlid)
	}

	for _, port := range []int32{1, 20006, 20011, 65535} {
		minCntlid, maxCntlid := getTargetCntlidRange(ip, port, true)
		c.Assert(minCntlid >= 1, Equals, true)
		c.Assert(maxCntlid <= maxValidCntlid, Equals, true)
		c.Assert(int(maxCntlid)-int(minCntlid)+1, Equals, sharedBlockMaxFrontends)
	}
}

func (s *TestSuite) TestEngineSharedBlockRaidUUIDAndPtplFile(c *C) {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false, "/metadata")
	c.Assert(e.getRaidBdevCreateUUID(), Equals, "")
	c.Assert(e.getReservationPtplFile(), Equals, "")

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "/metadata")
	c.Assert(e.getRaidBdevCreateUUID(), Equals, getStableVolumeRaidUUID("vol-a"))
	c.Assert(e.getRaidBdevCreateUUID(), Not(Equals), getStableVolumeNsUUID("vol-a"))
	e.sharedBlockState.volumeUID = "uid-a"
	c.Assert(e.getReservationPtplFile(), Equals, filepath.Join("/metadata", reservationPtplSubDir, "vol-a", "uid-a-0.json"))
	e.sharedBlockState.reservationEpoch = 3
	c.Assert(e.getReservationPtplFile(), Equals, filepath.Join("/metadata", reservationPtplSubDir, "vol-a", "uid-a-3.json"))

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "")
	c.Assert(e.getReservationPtplFile(), Equals, "")
}

func (s *TestSuite) TestEngineSharedBlockReportedInProto(c *C) {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false, "")
	c.Assert(e.getWithoutLock().SharedBlock, Equals, false)

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "")
	c.Assert(e.getWithoutLock().SharedBlock, Equals, true)
}

func (s *TestSuite) TestEngineFrontendSharedBlockOnlySupportsBlockdev(c *C) {
	for _, frontend := range []string{lhtypes.FrontendSPDKTCPNvmf, lhtypes.FrontendUBLK} {
		ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", frontend, 1048576, 0, 0, make(chan interface{}, 1), nil, true)

		err := ef.handleFrontend(nil, "10.0.0.1:20006")
		c.Assert(err, ErrorMatches, ".*not supported for shared block engine frontend.*")
	}

	// The maintenance mode attaches a shared block volume without a frontend.
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	c.Assert(ef.handleFrontend(nil, "10.0.0.1:20006"), IsNil)
}

func (s *TestSuite) TestEngineFrontendSharedBlockRejectsSwitchover(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.State = lhtypes.InstanceStateRunning

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(errors.Is(err, ErrSwitchOverTargetInvalidInput), Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-a")
	c.Assert(ef.getWithoutLock().SharedBlock, Equals, true)
}

func (s *TestSuite) TestSaveAndLoadEngineFrontendRecordPreservesSharedBlock(c *C) {
	tmpDir := c.MkDir()

	ef := NewEngineFrontend("ef-1", "engine-a", "vol-a",
		lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 3000

	err := saveEngineFrontendRecord(tmpDir, ef)
	c.Assert(err, IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)
	c.Assert(records[0].SharedBlock, Equals, true)
}

const (
	testReservationHostA = "0f2a8bc4-1d9e-4b5c-8a7f-3e6d2c1b0a99"
	testReservationHostB = "11111111-2222-3333-4444-555555555555"
)

func newTestReservationState(epoch uint64) *spdkrpc.ReservationState {
	return &spdkrpc.ReservationState{
		Epoch:        epoch,
		Type:         3,
		HolderKey:    0xAAAA,
		HolderHostId: testReservationHostA,
		Registrants: []*spdkrpc.ReservationRegistrant{
			{Key: 0xAAAA, HostId: testReservationHostA},
			{Key: 0xFFFFFFFFFFFFFFFF, HostId: testReservationHostB},
		},
	}
}

func (s *TestSuite) TestParseReservationPtplData(c *C) {
	const bdevUUID = "5a6c7d8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"

	tests := []struct {
		name  string
		data  string
		valid bool
	}{
		{"written by SPDK", `{"ptpl":true,"rtype":3,"crkey":43690,"bdev_uuid":"` + bdevUUID + `","holder_uuid":"` + testReservationHostA + `","registrants":[{"rkey":43690,"host_uuid":"` + testReservationHostA + `"}]}`, true},
		{"bdev UUID in upper case", `{"ptpl":true,"rtype":3,"crkey":43690,"bdev_uuid":"` + strings.ToUpper(bdevUUID) + `","holder_uuid":"` + testReservationHostA + `","registrants":[{"rkey":43690,"host_uuid":"` + testReservationHostA + `"}]}`, true},
		{"registered without reservation", `{"ptpl":true,"rtype":0,"crkey":0,"bdev_uuid":"` + bdevUUID + `","holder_uuid":"","registrants":[{"rkey":43690,"host_uuid":"` + testReservationHostA + `"}]}`, true},
		{"no registrants", `{"ptpl":true,"rtype":0,"crkey":0,"bdev_uuid":"","holder_uuid":"","registrants":[]}`, true},
		{"empty", ``, false},
		{"whitespace only", " \n", false},
		{"truncated", `{"ptpl":true,"rtype":3,"crkey":43690,"bdev_uuid":"` + bdevUUID, false},
		{"bdev UUID missing", `{"ptpl":true,"registrants":[]}`, false},
		{"registrants missing", `{"ptpl":true,"bdev_uuid":"` + bdevUUID + `"}`, false},
		{"host UUID missing", `{"ptpl":true,"bdev_uuid":"` + bdevUUID + `","registrants":[{"rkey":1}]}`, false},
		{"holder key not registered", `{"ptpl":true,"rtype":3,"crkey":1,"bdev_uuid":"` + bdevUUID + `","registrants":[{"rkey":2,"host_uuid":"` + testReservationHostA + `"}]}`, false},
		{"other bdev UUID", `{"ptpl":true,"rtype":0,"crkey":0,"bdev_uuid":"` + testReservationHostA + `","registrants":[{"rkey":2,"host_uuid":"` + testReservationHostA + `"}]}`, false},
	}

	for _, test := range tests {
		_, err := parseReservationPtplData([]byte(test.data), bdevUUID)
		c.Assert(err == nil, Equals, test.valid, Commentf("%s: %v", test.name, err))
	}

	regs := make([]string, sharedBlockMaxFrontends+1)
	for i := range regs {
		regs[i] = fmt.Sprintf(`{"rkey":%d,"host_uuid":"%s"}`, i+1, testReservationHostA)
	}
	data := `{"ptpl":true,"bdev_uuid":"` + bdevUUID + `","registrants":[` + strings.Join(regs, ",") + `]}`
	_, err := parseReservationPtplData([]byte(data), bdevUUID)
	c.Assert(err, ErrorMatches, ".*exceed the maximum.*")
}

func (s *TestSuite) TestReservationStatePtplDataRoundTrip(c *C) {
	const bdevUUID = "5a6c7d8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"

	state := newTestReservationState(7)
	data, err := reservationStateToPtplData(state, bdevUUID)
	c.Assert(err, IsNil)

	record, err := parseReservationPtplData(data, bdevUUID)
	c.Assert(err, IsNil)
	got := reservationPtplRecordToState(record, 7)
	c.Assert(proto.Equal(got, state), Equals, true, Commentf("got %v", got))

	// A state without registrants has nothing to restore.
	data, err = reservationStateToPtplData(&spdkrpc.ReservationState{Epoch: 7}, bdevUUID)
	c.Assert(err, IsNil)
	c.Assert(data, IsNil)

	// A state without persistence has no registrants.
	record, err = parseReservationPtplData([]byte(`{"ptpl":false,"bdev_uuid":"","registrants":[{"rkey":1,"host_uuid":"`+testReservationHostA+`"}]}`), bdevUUID)
	c.Assert(err, IsNil)
	got = reservationPtplRecordToState(record, 7)
	c.Assert(got.Epoch, Equals, uint64(7))
	c.Assert(len(got.Registrants), Equals, 0)
}

func (s *TestSuite) TestEnginePrepareReservationPtplFile(c *C) {
	metadataDir := c.MkDir()
	newEngine := func(seed *spdkrpc.ReservationState) *Engine {
		e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
		e.sharedBlockState.reservationEpoch = seed.Epoch
		e.sharedBlockState.reservationSeed = seed
		e.sharedBlockState.volumeUID = "uid-a"
		if seed.VolumeUid != "" {
			e.sharedBlockState.volumeUID = seed.VolumeUid
		}
		return e
	}
	bdevUUID := getStableVolumeRaidUUID("vol-a")
	readState := func(file string) *spdkrpc.ReservationState {
		data, err := os.ReadFile(file)
		c.Assert(err, IsNil)
		record, err := parseReservationPtplData(data, bdevUUID)
		c.Assert(err, IsNil)
		return reservationPtplRecordToState(record, 0)
	}

	// The first engine of an epoch rebuilds the file from the synced state.
	synced := newTestReservationState(1)
	e := newEngine(synced)
	ptplFile := e.getReservationPtplFile()
	c.Assert(os.MkdirAll(filepath.Dir(ptplFile), 0755), IsNil)
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	c.Assert(len(readState(ptplFile).Registrants), Equals, 2)

	// A newer local state of the same epoch wins over the synced state, e.g.,
	// SPDK persisted a preemption the manager has not synced yet.
	newer := newTestReservationState(1)
	newer.Registrants = newer.Registrants[:1]
	data, err := reservationStateToPtplData(newer, bdevUUID)
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(ptplFile, data, 0644), IsNil)
	e = newEngine(synced)
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	c.Assert(len(readState(ptplFile).Registrants), Equals, 1)

	// A partial local file falls back to the synced state.
	c.Assert(os.WriteFile(ptplFile, data[:20], 0644), IsNil)
	e = newEngine(synced)
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	c.Assert(len(readState(ptplFile).Registrants), Equals, 2)

	// An empty local file may be left by a crash in the middle of a write, so
	// it falls back to the synced state, which is empty once the hosts disabled
	// persistence.
	for _, empty := range [][]byte{nil, []byte("\n")} {
		c.Assert(os.WriteFile(ptplFile, empty, 0644), IsNil)
		e = newEngine(synced)
		c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
		c.Assert(len(readState(ptplFile).Registrants), Equals, 2)

		c.Assert(os.WriteFile(ptplFile, empty, 0644), IsNil)
		e = newEngine(&spdkrpc.ReservationState{Epoch: synced.Epoch})
		c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
		_, err = os.Stat(ptplFile)
		c.Assert(os.IsNotExist(err), Equals, true)
	}

	// A new epoch ignores and removes the file of the previous epoch, which
	// may be stale.
	c.Assert(os.WriteFile(ptplFile, data, 0644), IsNil)
	e = newEngine(newTestReservationState(2))
	newFile := e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(newFile), IsNil)
	c.Assert(len(readState(newFile).Registrants), Equals, 2)
	_, err = os.Stat(ptplFile)
	c.Assert(os.IsNotExist(err), Equals, true)

	// A new volume with the same name ignores and removes the file of the
	// deleted volume, even of the same epoch.
	newVolumeSeed := &spdkrpc.ReservationState{Epoch: 2, VolumeUid: "uid-b"}
	e = newEngine(newVolumeSeed)
	ptplFile = e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	_, err = os.Stat(newFile)
	c.Assert(os.IsNotExist(err), Equals, true)
	_, err = os.Stat(ptplFile)
	c.Assert(os.IsNotExist(err), Equals, true)

	// Without a synced state, there is nothing to restore.
	e = newEngine(&spdkrpc.ReservationState{Epoch: 3})
	ptplFile = e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	entries, err := os.ReadDir(filepath.Dir(ptplFile))
	c.Assert(err, IsNil)
	c.Assert(len(entries), Equals, 0)

	// A synced state SPDK cannot restore fails the engine target instead of
	// exposing it without the reservation, even with an invalid local file.
	invalid := newTestReservationState(4)
	invalid.HolderKey = 0x1234
	e = newEngine(invalid)
	ptplFile = e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(ptplFile), NotNil)
	_, err = os.Stat(ptplFile)
	c.Assert(os.IsNotExist(err), Equals, true)

	c.Assert(os.WriteFile(ptplFile, nil, 0644), IsNil)
	c.Assert(e.prepareReservationPtplFile(ptplFile), NotNil)
	data, err = os.ReadFile(ptplFile)
	c.Assert(err, IsNil)
	c.Assert(len(data), Equals, 0)
}

func (s *TestSuite) TestEngineCreateSharedBlockRequiresVolumeUID(c *C) {
	server := &Server{engineMap: map[string]*Engine{}}
	for _, reservation := range []*spdkrpc.ReservationState{nil, {Epoch: 1}} {
		_, err := server.EngineCreate(context.Background(), &spdkrpc.EngineCreateRequest{
			Name:        "engine-a",
			VolumeName:  "vol-a",
			SpecSize:    1048576,
			SharedBlock: true,
			Reservation: reservation,
		})
		c.Assert(grpcstatus.Code(err), Equals, grpccodes.InvalidArgument)
	}

	// The volume name and UID must not point out of the directory of the
	// ptpl files.
	for _, names := range [][2]string{{"..", "uid-a"}, {"vol-a", ".."}, {"vol-a", "../../frontends/uid-a"}} {
		_, err := server.EngineCreate(context.Background(), &spdkrpc.EngineCreateRequest{
			Name:        "engine-a",
			VolumeName:  names[0],
			SpecSize:    1048576,
			SharedBlock: true,
			Reservation: &spdkrpc.ReservationState{VolumeUid: names[1]},
		})
		c.Assert(grpcstatus.Code(err), Equals, grpccodes.InvalidArgument, Commentf("volume name %q, UID %q", names[0], names[1]))
	}
	c.Assert(server.engineMap, HasLen, 0)
}

func (s *TestSuite) TestEngineReservationCleanup(c *C) {
	metadataDir := c.MkDir()
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
	e.sharedBlockState.volumeUID = "uid-a"
	other := NewEngine("engine-b", "vol-b", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
	for _, file := range []string{e.getReservationPtplFile(), other.getReservationPtplFile()} {
		c.Assert(os.MkdirAll(filepath.Dir(file), 0755), IsNil)
		c.Assert(os.WriteFile(file, []byte("{}"), 0644), IsNil)
	}
	server := &Server{metadataDir: metadataDir, engineMap: map[string]*Engine{e.Name: e}}
	cleanup := func(volumeName string) error {
		_, err := server.EngineReservationCleanup(context.Background(), &spdkrpc.EngineReservationCleanupRequest{VolumeName: volumeName})
		return err
	}

	// The state of an existing engine is kept.
	c.Assert(grpcstatus.Code(cleanup("vol-a")), Equals, grpccodes.FailedPrecondition)
	_, err := os.Stat(e.getReservationPtplFile())
	c.Assert(err, IsNil)

	// A name that may point out of the directory of the volume is rejected.
	for _, name := range []string{"", ".", "..", "/", "//", "/vol-a", "vol-a/", "../vol-b", "vol-a/.."} {
		c.Assert(grpcstatus.Code(cleanup(name)), Equals, grpccodes.InvalidArgument, Commentf("volume name %q", name))
	}

	delete(server.engineMap, e.Name)
	c.Assert(cleanup("vol-a"), IsNil)
	_, err = os.Stat(e.getReservationPtplDir())
	c.Assert(os.IsNotExist(err), Equals, true)
	_, err = os.Stat(other.getReservationPtplFile())
	c.Assert(err, IsNil)

	// Nothing to clean up for a missing directory.
	c.Assert(cleanup("vol-a"), IsNil)
}

func (s *TestSuite) TestEngineGetReservationState(c *C) {
	metadataDir := c.MkDir()
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
	e.sharedBlockState.reservationEpoch = 5
	ptplFile := e.getReservationPtplFile()
	c.Assert(os.MkdirAll(filepath.Dir(ptplFile), 0755), IsNil)

	// Not reported before the ptpl file is prepared, even if the engine is
	// running, e.g., in the maintenance mode without a target.
	e.State = lhtypes.InstanceStateRunning
	c.Assert(e.getWithoutLock().Reservation, IsNil)
	e.sharedBlockState.reservationPrepared = true

	// No file means no persisted state.
	state := e.getWithoutLock().Reservation
	c.Assert(state, NotNil)
	c.Assert(state.Epoch, Equals, uint64(5))
	c.Assert(len(state.Registrants), Equals, 0)

	data, err := reservationStateToPtplData(newTestReservationState(5), getStableVolumeRaidUUID("vol-a"))
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(ptplFile, data, 0644), IsNil)
	state = e.getWithoutLock().Reservation
	c.Assert(proto.Equal(state, newTestReservationState(5)), Equals, true, Commentf("got %v", state))

	// A partial file is unknown, e.g., SPDK is writing it.
	c.Assert(os.WriteFile(ptplFile, data[:20], 0644), IsNil)
	c.Assert(e.getWithoutLock().Reservation, IsNil)

	// An empty file is unknown, even an old one, as a crash in the middle of
	// a write may leave it empty.
	c.Assert(os.WriteFile(ptplFile, nil, 0644), IsNil)
	old := time.Now().Add(-time.Hour)
	c.Assert(os.Chtimes(ptplFile, old, old), IsNil)
	c.Assert(e.getWithoutLock().Reservation, IsNil)

	// Engines without shared block report nothing.
	other := NewEngine("engine-b", "vol-b", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false, metadataDir)
	other.State = lhtypes.InstanceStateRunning
	other.sharedBlockState.reservationPrepared = true
	c.Assert(other.getWithoutLock().Reservation, IsNil)
}
