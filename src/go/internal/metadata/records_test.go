package metadata

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// The golden texts are the compact JSON that schema version 1 of the Rust
// implementation writes. The same texts are pinned in
// src/rust/bitacora-meta/tests/records_golden_test.rs, so a record that Go
// writes is byte for byte what Rust writes, and the other way round.

const goldenStream = `{"schema_version":1,"stream_id":7,"name":"orders",` +
	`"replication":{"replica_count":3,"write_quorum":2},` +
	`"retention":{"keep_sealed_for_millis":null},` +
	`"epoch":12,"writer":{"writer_identity":"compute-2","expires_at":1795000000000},` +
	`"segments":[{"segment_id":41,"epoch":11,"first_sequence":9001,` +
	`"sealed":{"end":9500,"at":1794999990000},` +
	`"copies":[{"replica_id":"replica-a","state":"good"},` +
	`{"replica_id":"replica-b","state":"bad"},` +
	`{"replica_id":"replica-c","state":"unverified"}],` +
	`"tiered":{"object_key":"streams/7/segments/00000000000000000041","size_bytes":4096,` +
	`"uploaded_at":1794999995000,"are_local_copies_dropped":false}},` +
	`{"segment_id":42,"epoch":12,"first_sequence":9501,"sealed":null,` +
	`"copies":[{"replica_id":"replica-a","state":"unverified"}],"tiered":null}],` +
	`"next_segment_id":43,"standby_replicas":["replica-b","replica-c"],` +
	`"truncate_before":8000,"low_water_mark":9001,"materializers":["pages"]}`

const goldenCheckpoint = `{"schema_version":1,"materializer":"pages","checkpoint":9000,` +
	`"fencing_token":4,"manifest":"manifests/9000"}`

const goldenReplica = `{"schema_version":1,"replica_id":"replica-a","address":"10.0.0.11:7000",` +
	`"failure_domains":["zone-1","rack-4","host-11"],"capacity_bytes":960000000000,` +
	`"free_bytes":412000000000,"is_draining":false,"heartbeat_at":1795000000000}`

const goldenDeletedStream = `{"stream_id":7,"name":"orders"}`

// goldenStreamRecord is the stream the golden text describes.
func goldenStreamRecord() StreamRecord {
	truncateBefore := uint64(8000)
	return StreamRecord{
		SchemaVersion: SchemaVersion,
		StreamID:      7,
		Name:          "orders",
		Replication:   ReplicationSettings{ReplicaCount: 3, WriteQuorum: 2},
		Epoch:         12,
		Writer:        &WriterLease{WriterIdentity: "compute-2", ExpiresAt: 1_795_000_000_000},
		Segments: []SegmentRecord{
			{
				SegmentID:     41,
				Epoch:         11,
				FirstSequence: 9001,
				Sealed:        &SealRecord{End: 9500, At: 1_794_999_990_000},
				Copies: []CopyRecord{
					{ReplicaID: "replica-a", State: CopyGood},
					{ReplicaID: "replica-b", State: CopyBad},
					{ReplicaID: "replica-c", State: CopyUnverified},
				},
				Tiered: &TieredRecord{
					ObjectKey:  "streams/7/segments/00000000000000000041",
					SizeBytes:  4096,
					UploadedAt: 1_794_999_995_000,
				},
			},
			{
				SegmentID:     42,
				Epoch:         12,
				FirstSequence: 9501,
				Copies:        []CopyRecord{{ReplicaID: "replica-a", State: CopyUnverified}},
			},
		},
		NextSegmentID:   43,
		StandbyReplicas: []string{"replica-b", "replica-c"},
		TruncateBefore:  &truncateBefore,
		LowWaterMark:    9001,
		Materializers:   []string{"pages"},
	}
}

func TestStreamRecordIsWrittenAsTheGoldenText(t *testing.T) {
	written, err := encodeStream(goldenStreamRecord())
	if err != nil {
		t.Fatalf("encodeStream: %v", err)
	}

	if string(written) != goldenStream {
		t.Errorf("written:\n%s\nwant:\n%s", written, goldenStream)
	}
}

func TestStreamRecordIsReadFromTheGoldenText(t *testing.T) {
	read, err := decodeStream("streams/orders", []byte(goldenStream))
	if err != nil {
		t.Fatalf("decodeStream: %v", err)
	}

	if want := goldenStreamRecord(); !reflect.DeepEqual(read, want) {
		t.Errorf("read %+v, want %+v", read, want)
	}
}

func TestOtherRecordsRoundTripThroughTheirGoldenTexts(t *testing.T) {
	manifest := "manifests/9000"
	tests := []struct {
		name   string
		golden string
		record any
		empty  any
	}{
		{"checkpoint", goldenCheckpoint, &CheckpointRecord{
			SchemaVersion: SchemaVersion, Materializer: "pages", Checkpoint: 9000, FencingToken: 4, Manifest: &manifest,
		}, &CheckpointRecord{}},
		{"replica", goldenReplica, &ReplicaRecord{
			SchemaVersion: SchemaVersion, ReplicaID: "replica-a", Address: "10.0.0.11:7000",
			FailureDomains: []string{"zone-1", "rack-4", "host-11"},
			CapacityBytes:  960_000_000_000, FreeBytes: 412_000_000_000, HeartbeatAt: 1_795_000_000_000,
		}, &ReplicaRecord{}},
		{"deleted stream", goldenDeletedStream, &DeletedStreamRecord{StreamID: 7, Name: "orders"}, &DeletedStreamRecord{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			written, err := json.Marshal(test.record)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(written) != test.golden {
				t.Errorf("written:\n%s\nwant:\n%s", written, test.golden)
			}

			if err := json.Unmarshal([]byte(test.golden), test.empty); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !reflect.DeepEqual(test.empty, test.record) {
				t.Errorf("read %+v, want %+v", test.empty, test.record)
			}
		})
	}
}

// A list that is empty must be written as [], which is a list to every
// implementation; null is not.
func TestEmptyListsAreWrittenAsEmptyListsNotNull(t *testing.T) {
	stream := StreamRecord{
		SchemaVersion: SchemaVersion,
		Name:          "orders",
		Segments:      []SegmentRecord{{SegmentID: 1, FirstSequence: 1}},
	}

	written, err := encodeStream(stream)
	if err != nil {
		t.Fatalf("encodeStream: %v", err)
	}

	for _, want := range []string{`"copies":[]`, `"materializers":[]`, `"standby_replicas":null`} {
		if !strings.Contains(string(written), want) {
			t.Errorf("written record lacks %s:\n%s", want, written)
		}
	}
}

func TestRecordOfANewerSchemaIsRefused(t *testing.T) {
	newer := strings.Replace(goldenStream, `"schema_version":1`, `"schema_version":2`, 1)

	_, err := decodeStream("streams/orders", []byte(newer))

	if !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("decodeStream error = %v, want ErrCorruptRecord", err)
	}
}

func TestSegmentContains(t *testing.T) {
	sealed := SegmentRecord{FirstSequence: 10, Sealed: &SealRecord{End: 19}}
	open := SegmentRecord{FirstSequence: 20}
	tests := []struct {
		name     string
		segment  SegmentRecord
		sequence uint64
		want     bool
	}{
		{"before a sealed segment", sealed, 9, false},
		{"first record of a sealed segment", sealed, 10, true},
		{"sealed end", sealed, 19, true},
		{"past the sealed end", sealed, 20, false},
		{"before an open segment", open, 19, false},
		{"anywhere after the start of an open segment", open, 1_000_000, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.segment.Contains(test.sequence); got != test.want {
				t.Errorf("Contains(%d) = %v, want %v", test.sequence, got, test.want)
			}
		})
	}
}
