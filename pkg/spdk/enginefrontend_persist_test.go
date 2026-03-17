package spdk

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// --- saveEngineFrontendRecord / loadEngineFrontendRecords round-trip ---

func (s *TestSuite) TestSaveAndLoadEngineFrontendRecord(c *C) {
	tmpDir := c.MkDir()

	ef := NewEngineFrontend("ef-1", "engine-a", "vol-a",
		lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 3000

	err := saveEngineFrontendRecord(tmpDir, ef)
	c.Assert(err, IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)
	c.Assert(records[0].Name, Equals, "ef-1")
	c.Assert(records[0].EngineName, Equals, "engine-a")
	c.Assert(records[0].VolumeName, Equals, "vol-a")
	c.Assert(records[0].Frontend, Equals, lhtypes.FrontendSPDKTCPBlockdev)
	c.Assert(records[0].SpecSize, Equals, uint64(1048576))
	c.Assert(records[0].TargetIP, Equals, "10.0.0.1")
	c.Assert(records[0].TargetPort, Equals, int32(3000))
}

func (s *TestSuite) TestSaveRecordPersistsTargetIPAndPort(c *C) {
	tmpDir := c.MkDir()

	ef := NewEngineFrontend("ef-nvmf", "engine-b", "vol-b",
		lhtypes.FrontendSPDKTCPNvmf, 2097152, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend.TargetIP = "192.168.1.10"
	ef.NvmeTcpFrontend.TargetPort = 4420

	err := saveEngineFrontendRecord(tmpDir, ef)
	c.Assert(err, IsNil)

	// Read raw JSON to verify fields are present.
	data, err := os.ReadFile(engineFrontendRecordPath(tmpDir, "vol-b"))
	c.Assert(err, IsNil)

	var raw map[string]interface{}
	c.Assert(json.Unmarshal(data, &raw), IsNil)
	c.Assert(raw["targetIP"], Equals, "192.168.1.10")
	c.Assert(raw["targetPort"], Equals, float64(4420)) // JSON numbers are float64
}

// --- UBLK frontend should NOT be persisted (Issue #4) ---

func (s *TestSuite) TestSaveRecordSkipsUblkFrontend(c *C) {
	tmpDir := c.MkDir()

	ef := NewEngineFrontend("ef-ublk", "engine-c", "vol-c",
		lhtypes.FrontendUBLK, 1048576, 0, 0, make(chan interface{}, 1))

	err := saveEngineFrontendRecord(tmpDir, ef)
	c.Assert(err, IsNil)

	// Verify no file was written.
	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 0)
}

// --- Empty metadataDir is a no-op ---

func (s *TestSuite) TestSaveRecordEmptyMetadataDirIsNoop(c *C) {
	ef := NewEngineFrontend("ef-x", "engine-x", "vol-x",
		lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))

	c.Assert(saveEngineFrontendRecord("", ef), IsNil)
}

func (s *TestSuite) TestLoadRecordsEmptyMetadataDirReturnsNil(c *C) {
	records, err := loadEngineFrontendRecords("")
	c.Assert(err, IsNil)
	c.Assert(records, IsNil)
}

func (s *TestSuite) TestLoadRecordsNonExistentDirReturnsNil(c *C) {
	records, err := loadEngineFrontendRecords("/tmp/nonexistent-dir-for-test-" + time.Now().Format("20060102150405"))
	c.Assert(err, IsNil)
	c.Assert(records, IsNil)
}

// --- Corrupted records are cleaned up (Issue #5) ---

func (s *TestSuite) TestLoadRecordsRemovesCorruptedJSON(c *C) {
	tmpDir := c.MkDir()

	// Create a corrupted record.
	volDir := filepath.Join(tmpDir, engineFrontendSubDir, "vol-corrupt")
	c.Assert(os.MkdirAll(volDir, 0700), IsNil)
	c.Assert(os.WriteFile(filepath.Join(volDir, engineFrontendRecFile), []byte("{invalid json"), 0600), IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 0)

	// Verify the corrupted directory was removed.
	_, statErr := os.Stat(volDir)
	c.Assert(os.IsNotExist(statErr), Equals, true)
}

func (s *TestSuite) TestLoadRecordsRemovesRecordWithEmptyName(c *C) {
	tmpDir := c.MkDir()

	// Create a record with empty Name field.
	volDir := filepath.Join(tmpDir, engineFrontendSubDir, "vol-empty-name")
	c.Assert(os.MkdirAll(volDir, 0700), IsNil)

	record := &EngineFrontendRecord{
		Name:       "",
		VolumeName: "vol-empty-name",
		Frontend:   lhtypes.FrontendSPDKTCPBlockdev,
	}
	data, _ := json.Marshal(record)
	c.Assert(os.WriteFile(filepath.Join(volDir, engineFrontendRecFile), data, 0600), IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 0)

	// Verify directory was removed.
	_, statErr := os.Stat(volDir)
	c.Assert(os.IsNotExist(statErr), Equals, true)
}

func (s *TestSuite) TestLoadRecordsRemovesRecordWithEmptyVolumeName(c *C) {
	tmpDir := c.MkDir()

	volDir := filepath.Join(tmpDir, engineFrontendSubDir, "vol-empty-volname")
	c.Assert(os.MkdirAll(volDir, 0700), IsNil)

	record := &EngineFrontendRecord{
		Name:       "ef-x",
		VolumeName: "",
		Frontend:   lhtypes.FrontendSPDKTCPBlockdev,
	}
	data, _ := json.Marshal(record)
	c.Assert(os.WriteFile(filepath.Join(volDir, engineFrontendRecFile), data, 0600), IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 0)

	_, statErr := os.Stat(volDir)
	c.Assert(os.IsNotExist(statErr), Equals, true)
}

// --- Mixed valid and corrupted records ---

func (s *TestSuite) TestLoadRecordsMixedValidAndCorrupted(c *C) {
	tmpDir := c.MkDir()

	// Create a valid record.
	efValid := NewEngineFrontend("ef-valid", "engine-v", "vol-valid",
		lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1))
	efValid.NvmeTcpFrontend.TargetIP = "10.0.0.5"
	efValid.NvmeTcpFrontend.TargetPort = 5000
	c.Assert(saveEngineFrontendRecord(tmpDir, efValid), IsNil)

	// Create a corrupted record alongside.
	corruptDir := filepath.Join(tmpDir, engineFrontendSubDir, "vol-corrupt")
	c.Assert(os.MkdirAll(corruptDir, 0700), IsNil)
	c.Assert(os.WriteFile(filepath.Join(corruptDir, engineFrontendRecFile), []byte("not-json"), 0600), IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)
	c.Assert(records[0].Name, Equals, "ef-valid")
	c.Assert(records[0].TargetIP, Equals, "10.0.0.5")
	c.Assert(records[0].TargetPort, Equals, int32(5000))

	// Corrupted record should be cleaned up.
	_, statErr := os.Stat(corruptDir)
	c.Assert(os.IsNotExist(statErr), Equals, true)
}

// --- removeEngineFrontendRecord ---

func (s *TestSuite) TestRemoveEngineFrontendRecord(c *C) {
	tmpDir := c.MkDir()

	ef := NewEngineFrontend("ef-rm", "engine-rm", "vol-rm",
		lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	c.Assert(saveEngineFrontendRecord(tmpDir, ef), IsNil)

	// Verify exists.
	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)

	// Remove.
	c.Assert(removeEngineFrontendRecord(tmpDir, "vol-rm"), IsNil)

	records, err = loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 0)
}

// --- Backward compatibility: old records without targetIP/targetPort ---

func (s *TestSuite) TestLoadRecordsBackwardCompatibleWithOldFormat(c *C) {
	tmpDir := c.MkDir()

	// Simulate an old-format record (no targetIP/targetPort fields).
	volDir := filepath.Join(tmpDir, engineFrontendSubDir, "vol-old")
	c.Assert(os.MkdirAll(volDir, 0700), IsNil)

	oldRecord := `{
  "name": "ef-old",
  "engineName": "engine-old",
  "volumeName": "vol-old",
  "frontend": "spdk-tcp-blockdev",
  "specSize": 1048576,
  "engineIP": "10.0.0.99"
}`
	c.Assert(os.WriteFile(filepath.Join(volDir, engineFrontendRecFile), []byte(oldRecord), 0600), IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)
	c.Assert(records[0].Name, Equals, "ef-old")
	c.Assert(records[0].TargetIP, Equals, "")         // Not present in old format.
	c.Assert(records[0].TargetPort, Equals, int32(0)) // Not present in old format.
}

// --- Overwrite: saving again updates the record ---

func (s *TestSuite) TestSaveRecordOverwritesPrevious(c *C) {
	tmpDir := c.MkDir()

	ef := NewEngineFrontend("ef-ow", "engine-ow", "vol-ow",
		lhtypes.FrontendSPDKTCPNvmf, 1048576, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 3000
	c.Assert(saveEngineFrontendRecord(tmpDir, ef), IsNil)

	// Update port and save again (simulating switchover).
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.2"
	ef.NvmeTcpFrontend.TargetPort = 4000
	c.Assert(saveEngineFrontendRecord(tmpDir, ef), IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)
	c.Assert(records[0].TargetIP, Equals, "10.0.0.2")
	c.Assert(records[0].TargetPort, Equals, int32(4000))
}

// --- RecoverFromHost tests ---

func (s *TestSuite) TestRecoverFromHostNvmfReconstructsEndpoint(c *C) {
	updateCh := make(chan interface{}, 1)

	ef := NewEngineFrontend("ef-nvmf-recover", "engine-r", "vol-r",
		lhtypes.FrontendSPDKTCPNvmf, 1048576, 0, 0, updateCh)
	ef.EngineIP = "10.0.0.5"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.5"
	ef.NvmeTcpFrontend.TargetPort = 4420

	err := ef.RecoverFromHost(nil)
	c.Assert(err, IsNil)

	got := ef.Get()
	c.Assert(got.State, Equals, string(lhtypes.InstanceStateRunning))

	expectedNqn := helpertypes.GetNQN("engine-r")
	expectedEndpoint := GetNvmfEndpoint(expectedNqn, "10.0.0.5", 4420)
	c.Assert(got.Endpoint, Equals, expectedEndpoint)
	c.Assert(got.TargetIp, Equals, "10.0.0.5")
	c.Assert(got.TargetPort, Equals, int32(4420))

	// Drain the update channel.
	select {
	case <-updateCh:
	case <-time.After(time.Second):
		c.Fatal("expected update on UpdateCh")
	}
}

func (s *TestSuite) TestRecoverFromHostNvmfNoPortLeavesEmptyEndpoint(c *C) {
	updateCh := make(chan interface{}, 1)

	ef := NewEngineFrontend("ef-nvmf-noport", "engine-np", "vol-np",
		lhtypes.FrontendSPDKTCPNvmf, 1048576, 0, 0, updateCh)
	ef.EngineIP = "10.0.0.6"
	// TargetPort is 0 (not recovered from old record).

	err := ef.RecoverFromHost(nil)
	c.Assert(err, IsNil)

	got := ef.Get()
	c.Assert(got.State, Equals, string(lhtypes.InstanceStateRunning))
	// Endpoint should remain empty because port is 0.
	c.Assert(got.Endpoint, Equals, "")
	c.Assert(got.TargetIp, Equals, "10.0.0.6")
	c.Assert(got.TargetPort, Equals, int32(0))
}

func (s *TestSuite) TestRecoverFromHostEmptyFrontend(c *C) {
	updateCh := make(chan interface{}, 1)

	ef := NewEngineFrontend("ef-empty", "engine-e", "vol-e",
		lhtypes.FrontendEmpty, 1024, 0, 0, updateCh)

	err := ef.RecoverFromHost(nil)
	c.Assert(err, IsNil)

	got := ef.Get()
	c.Assert(got.State, Equals, string(lhtypes.InstanceStateRunning))
	c.Assert(got.Endpoint, Equals, "")
}

func (s *TestSuite) TestRecoverFromHostRejectsNonPendingState(c *C) {
	updateCh := make(chan interface{}, 1)

	ef := NewEngineFrontend("ef-running", "engine-run", "vol-run",
		lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning

	err := ef.RecoverFromHost(nil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*invalid state.*")
}

func (s *TestSuite) TestRecoverFromHostUnsupportedFrontendSetsError(c *C) {
	updateCh := make(chan interface{}, 1)

	ef := NewEngineFrontend("ef-unknown", "engine-u", "vol-u",
		"unknown-frontend", 1024, 0, 0, updateCh)

	err := ef.RecoverFromHost(nil)
	c.Assert(err, NotNil)

	got := ef.Get()
	c.Assert(got.State, Equals, string(lhtypes.InstanceStateError))
	c.Assert(got.ErrorMsg, Matches, ".*unsupported frontend type.*")
}

// --- recoverEngineFrontends integration test ---

func (s *TestSuite) TestRecoverEngineFrontendsRestoresTargetIPAndPort(c *C) {
	tmpDir := c.MkDir()

	// Persist a record.
	ef := NewEngineFrontend("ef-int", "engine-int", "vol-int",
		lhtypes.FrontendSPDKTCPNvmf, 1048576, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.10"
	ef.NvmeTcpFrontend.TargetPort = 5555
	c.Assert(saveEngineFrontendRecord(tmpDir, ef), IsNil)

	// Load records and build EngineFrontend — simulating what recoverEngineFrontends does.
	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)

	record := records[0]
	updateCh := make(chan interface{}, 10)
	recovered := NewEngineFrontend(record.Name, record.EngineName, record.VolumeName,
		record.Frontend, record.SpecSize, 0, 0, updateCh)
	if recovered.NvmeTcpFrontend != nil {
		if record.TargetIP != "" {
			recovered.NvmeTcpFrontend.TargetIP = record.TargetIP
			recovered.EngineIP = record.TargetIP
		}
		if record.TargetPort != 0 {
			recovered.NvmeTcpFrontend.TargetPort = record.TargetPort
		}
	}

	// Now RecoverFromHost should reconstruct proper endpoint.
	c.Assert(recovered.RecoverFromHost(nil), IsNil)

	got := recovered.Get()
	c.Assert(got.State, Equals, string(lhtypes.InstanceStateRunning))

	expectedNqn := helpertypes.GetNQN("engine-int")
	expectedEndpoint := GetNvmfEndpoint(expectedNqn, "10.0.0.10", 5555)
	c.Assert(got.Endpoint, Equals, expectedEndpoint)
	c.Assert(got.TargetIp, Equals, "10.0.0.10")
	c.Assert(got.TargetPort, Equals, int32(5555))
}

// --- Empty directory with no record file ---

func (s *TestSuite) TestLoadRecordsSkipsDirWithoutRecordFile(c *C) {
	tmpDir := c.MkDir()

	// Create an empty volume directory (no enginefrontend.json inside).
	emptyDir := filepath.Join(tmpDir, engineFrontendSubDir, "vol-empty")
	c.Assert(os.MkdirAll(emptyDir, 0700), IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 0)

	// The empty directory should still exist (it's not corrupted, just missing).
	_, statErr := os.Stat(emptyDir)
	c.Assert(statErr, IsNil)
}
