package types

import (
	"reflect"
	"strings"
	"testing"

	lhbackup "github.com/longhorn/go-common-libs/backup"

	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

// TestBackupParametersEncodeDecodeRoundTrip exercises the label-smuggling
// contract used by the v2 (SPDK) backup path: parameters passed by callers
// (longhorn-manager -> longhorn-instance-manager) must survive being encoded
// into the Labels wire field and decoded again in longhorn-spdk-engine's
// server_replica.ReplicaBackupCreate. If either side drops a well-known key,
// v2 backups silently ignore caller intent (e.g. "backup-mode=full" is
// treated as incremental) — this is the bug that motivated introducing this
// helper pair.
func TestBackupParametersEncodeDecodeRoundTrip(t *testing.T) {
	parameters := map[string]string{
		lhbackup.LonghornBackupParameterBackupMode:      string(lhbackup.LonghornBackupModeFull),
		lhbackup.LonghornBackupParameterBackupBlockSize: "2097152",
	}
	userLabels := []string{"longhorn.io/volume-name=vol-a"}

	encoded := EncodeBackupParametersIntoLabels(userLabels, parameters)

	// The encoded wire form must use the reserved DNS-qualified prefix so it
	// cannot be confused with a user label sharing the plain parameter name.
	for _, kv := range encoded[len(userLabels):] {
		if !strings.HasPrefix(kv, backupParameterLabelPrefix) {
			t.Fatalf("encoded parameter %q does not use reserved prefix %q", kv, backupParameterLabelPrefix)
		}
	}

	// Use the real server-side parser (pkg/util.ParseLabels) rather than a
	// hand-rolled mimic, so this test tracks its actual last-wins/validation
	// behavior instead of drifting from it.
	labelMap, err := util.ParseLabels(encoded)
	if err != nil {
		t.Fatalf("ParseLabels(%v) returned unexpected error: %v", encoded, err)
	}

	got := ExtractBackupParametersFromLabels(labelMap)
	if !reflect.DeepEqual(got, parameters) {
		t.Fatalf("round-trip parameters = %v; want %v", got, parameters)
	}
	// Reserved-prefix entries must be consumed and not linger as
	// user-visible backup labels.
	for k := range labelMap {
		if strings.HasPrefix(k, backupParameterLabelPrefix) {
			t.Fatalf("extract left reserved key %q behind in labelMap", k)
		}
	}
	if labelMap["longhorn.io/volume-name"] != "vol-a" {
		t.Fatalf("user label was dropped: %v", labelMap)
	}
}

func TestEncodeBackupParametersIntoLabelsIsNoOpWhenEmpty(t *testing.T) {
	labels := []string{"foo=bar"}
	got := EncodeBackupParametersIntoLabels(labels, nil)
	if !reflect.DeepEqual(got, labels) {
		t.Fatalf("nil parameters mutated labels: %v", got)
	}
	got = EncodeBackupParametersIntoLabels(labels, map[string]string{"unrelated": "v"})
	if !reflect.DeepEqual(got, labels) {
		t.Fatalf("unrecognised parameter leaked into labels: %v", got)
	}
}

func TestExtractBackupParametersFromLabelsIsNoOpWhenAbsent(t *testing.T) {
	parameters := ExtractBackupParametersFromLabels(nil)
	if len(parameters) != 0 {
		t.Fatalf("nil labelMap produced non-empty parameters: %v", parameters)
	}
	labelMap := map[string]string{"foo": "bar"}
	parameters = ExtractBackupParametersFromLabels(labelMap)
	if len(parameters) != 0 {
		t.Fatalf("missing well-known keys produced non-empty parameters: %v", parameters)
	}
	if labelMap["foo"] != "bar" {
		t.Fatalf("unrelated label was mutated: %v", labelMap)
	}
}

// TestExtractDoesNotHijackPlainUserLabels guards the reserved-prefix
// contract: a user label whose name coincides with a well-known parameter
// key (e.g. "backup-mode=incremental") must be preserved verbatim and must
// NOT be extracted into the parameters map, because it lacks the reserved
// namespace prefix.
func TestExtractDoesNotHijackPlainUserLabels(t *testing.T) {
	labelMap := map[string]string{
		lhbackup.LonghornBackupParameterBackupMode:      string(lhbackup.LonghornBackupModeIncremental),
		lhbackup.LonghornBackupParameterBackupBlockSize: "16777216",
		"unrelated": "v",
	}
	original := map[string]string{}
	for k, v := range labelMap {
		original[k] = v
	}

	got := ExtractBackupParametersFromLabels(labelMap)
	if len(got) != 0 {
		t.Fatalf("plain user labels were hijacked into parameters: %v", got)
	}
	if !reflect.DeepEqual(labelMap, original) {
		t.Fatalf("plain user labels were mutated: got %v; want %v", labelMap, original)
	}
}

// TestExtractIgnoresUnknownReservedKeys ensures the decoder only accepts
// suffixes registered in wellKnownBackupParameterKeys. An unrecognised entry
// inside the reserved namespace is left alone so an accidental typo cannot
// silently disappear into parameters.
func TestExtractIgnoresUnknownReservedKeys(t *testing.T) {
	labelMap := map[string]string{
		backupParameterLabelPrefix + "unknown-key": "v",
	}
	got := ExtractBackupParametersFromLabels(labelMap)
	if len(got) != 0 {
		t.Fatalf("unknown reserved key leaked into parameters: %v", got)
	}
	if labelMap[backupParameterLabelPrefix+"unknown-key"] != "v" {
		t.Fatalf("unknown reserved key was unexpectedly removed: %v", labelMap)
	}
}

// TestEncodedKeysPassParseLabelsValidation guards against the reserved keys
// being invalid per util.ParseLabels' validation rules (which reject
// non-qualified names) — a regression here would silently disable the wire
// contract with a startup-time ParseLabels error.
func TestEncodedKeysPassParseLabelsValidation(t *testing.T) {
	parameters := map[string]string{
		lhbackup.LonghornBackupParameterBackupMode:      string(lhbackup.LonghornBackupModeFull),
		lhbackup.LonghornBackupParameterBackupBlockSize: "2097152",
	}
	encoded := EncodeBackupParametersIntoLabels(nil, parameters)
	if _, err := util.ParseLabels(encoded); err != nil {
		t.Fatalf("encoded reserved labels %v failed ParseLabels validation: %v", encoded, err)
	}
}
