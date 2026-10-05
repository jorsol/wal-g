package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

var (
	fullBackup = BackupInfo{
		BackupName:     "base_000000010000000000000002",
		WalFileName:    "000000010000000000000002",
		StartTime:      time.Unix(1000, 0),
		FinishTime:     time.Unix(2000, 0),
		CompressedSize: 100,
	}
	deltaBackup = BackupInfo{
		BackupName:     "base_000000010000000000000004_D_000000010000000000000002",
		WalFileName:    "000000010000000000000004",
		StartTime:      time.Unix(3000, 0),
		FinishTime:     time.Unix(4000, 0),
		CompressedSize: 10,
	}
)

func newTestExporter(t *testing.T) (*WalgExporter, *prometheus.Registry) {
	t.Helper()
	e := NewWalgExporter(slog.New(slog.DiscardHandler), "",
		time.Minute, time.Minute, time.Minute,
		5*time.Second, 5*time.Second, 5*time.Second, "")
	// The pedantic registry also checks that Collect is consistent with Describe
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(e))
	return e, reg
}

func loadJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, v))
}

func TestUpdateBackupMetricsDropsStaleSeries(t *testing.T) {
	e, reg := newTestExporter(t)

	// Nothing is exported before the first successful scrape
	require.Equal(t, 0, testutil.CollectAndCount(e, "walg_backup_info", "walg_backups"))

	e.updateBackupMetrics([]BackupInfo{fullBackup, deltaBackup})
	require.Equal(t, 2, testutil.CollectAndCount(e, "walg_backup_info"))

	e.updateBackupMetrics([]BackupInfo{fullBackup})
	expected := `
# HELP walg_backups Number of backups by type
# TYPE walg_backups gauge
walg_backups{backup_type="delta"} 0
walg_backups{backup_type="full"} 1
# HELP walg_backup_start_timestamp Start time of the backup (Unix timestamp).
# TYPE walg_backup_start_timestamp gauge
walg_backup_start_timestamp{backup_name="base_000000010000000000000002"} 1000
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"walg_backups", "walg_backup_start_timestamp"))
	require.Equal(t, 1, testutil.CollectAndCount(e, "walg_backup_info"))
}

func TestUpdateBackupMetricsDuplicateBackupNames(t *testing.T) {
	e, reg := newTestExporter(t)

	duplicate := fullBackup
	duplicate.CompressedSize = 42
	e.updateBackupMetrics([]BackupInfo{fullBackup, duplicate})

	// Duplicates must not produce duplicate series, which would fail the whole scrape;
	// the last one wins, as it did with GaugeVec.WithLabelValues
	expected := `
# HELP walg_backup_compressed_size_bytes Compressed size of the backup in bytes.
# TYPE walg_backup_compressed_size_bytes gauge
walg_backup_compressed_size_bytes{backup_name="base_000000010000000000000002"} 42
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"walg_backup_compressed_size_bytes"))
}

func TestCollectDropsInvalidLabelValues(t *testing.T) {
	e, reg := newTestExporter(t)

	invalid := fullBackup
	invalid.BackupName = "base_\xff"
	e.updateBackupMetrics([]BackupInfo{fullBackup, invalid})

	_, err := reg.Gather()
	require.NoError(t, err)
	require.Equal(t, 1, testutil.CollectAndCount(e, "walg_backup_info"))
}

func TestCollectNeverSeesPartialBackupMetrics(t *testing.T) {
	e, reg := newTestExporter(t)

	var backups []BackupInfo
	loadJSON(t, "testdata/backup-list.json", &backups)
	e.updateBackupMetrics(backups)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(stop)
		wg.Wait()
	}()
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				e.updateBackupMetrics(backups)
			}
		}
	})

	for range 500 {
		mfs, err := reg.Gather()
		require.NoError(t, err)

		series := make(map[string]int)
		for _, mf := range mfs {
			series[mf.GetName()] = len(mf.GetMetric())
		}
		require.Equal(t, 2, series["walg_backups"])
		require.Equal(t, len(backups), series["walg_backup_info"])
		require.Equal(t, len(backups), series["walg_backup_finish_timestamp"])
	}
}

func TestUpdateWalMetrics(t *testing.T) {
	e, reg := newTestExporter(t)

	var verifyData WalVerifyResponse
	loadJSON(t, "testdata/wal-verify.json", &verifyData)
	e.updateWalMetrics(&verifyData)

	expected := `
# HELP walg_wal_integrity_status WAL integrity status per timeline (1 = no lost segments, 0 = lost segments)
# TYPE walg_wal_integrity_status gauge
walg_wal_integrity_status{timeline_hex="00000043",timeline_id="67"} 0
walg_wal_integrity_status{timeline_hex="00000044",timeline_id="68"} 1
# HELP walg_wal_verify_status WAL verify status (1 = OK, 0 = FAILURE, 2 = WARNING, -1 = UNKNOWN)
# TYPE walg_wal_verify_status gauge
walg_wal_verify_status{operation="integrity"} 0
walg_wal_verify_status{operation="timeline"} 1
# HELP walg_wal_segments Number of WAL segments per timeline and wal-verify integrity status
# TYPE walg_wal_segments gauge
walg_wal_segments{status="FOUND",timeline_hex="00000043",timeline_id="67"} 33379
walg_wal_segments{status="FOUND",timeline_hex="00000044",timeline_id="68"} 39
walg_wal_segments{status="MISSING_LOST",timeline_hex="00000043",timeline_id="67"} 1
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"walg_wal_integrity_status", "walg_wal_verify_status", "walg_wal_segments"))
}

func TestUpdateWalMetricsOnlyLostSegmentsFailTimeline(t *testing.T) {
	e, reg := newTestExporter(t)

	e.updateWalMetrics(&WalVerifyResponse{
		Integrity: IntegrityData{Status: "WARNING", Details: []IntegrityDetail{
			{TimelineID: 10, SegmentsCount: 100, Status: "FOUND"},
			{TimelineID: 10, SegmentsCount: 2, Status: "MISSING_DELAYED"},
			{TimelineID: 10, SegmentsCount: 3, Status: "MISSING_UPLOADING"},
			{TimelineID: 11, SegmentsCount: 5, Status: "FOUND"},
			{TimelineID: 11, SegmentsCount: 1, Status: "MISSING_LOST"},
			{TimelineID: 11, SegmentsCount: 7, Status: "FOUND"},
		}},
		Timeline: TimelineData{Status: "OK"},
	})

	// Segments still being uploaded must not mark the timeline as failed
	expected := `
# HELP walg_wal_integrity_status WAL integrity status per timeline (1 = no lost segments, 0 = lost segments)
# TYPE walg_wal_integrity_status gauge
walg_wal_integrity_status{timeline_hex="0000000a",timeline_id="10"} 1
walg_wal_integrity_status{timeline_hex="0000000b",timeline_id="11"} 0
# HELP walg_wal_segments Number of WAL segments per timeline and wal-verify integrity status
# TYPE walg_wal_segments gauge
walg_wal_segments{status="FOUND",timeline_hex="0000000a",timeline_id="10"} 100
walg_wal_segments{status="FOUND",timeline_hex="0000000b",timeline_id="11"} 12
walg_wal_segments{status="MISSING_DELAYED",timeline_hex="0000000a",timeline_id="10"} 2
walg_wal_segments{status="MISSING_LOST",timeline_hex="0000000b",timeline_id="11"} 1
walg_wal_segments{status="MISSING_UPLOADING",timeline_hex="0000000a",timeline_id="10"} 3
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"walg_wal_integrity_status", "walg_wal_segments"))
}

func TestUpdateBackupMetricsCountsDistinctBackups(t *testing.T) {
	e, reg := newTestExporter(t)

	// backup-list reports a backup once per storage when failover storages are configured
	fullOnFailover := fullBackup
	fullOnFailover.StorageName = "failover"
	e.updateBackupMetrics([]BackupInfo{fullBackup, fullOnFailover, deltaBackup})

	expected := `
# HELP walg_backups Number of backups by type
# TYPE walg_backups gauge
walg_backups{backup_type="delta"} 1
walg_backups{backup_type="full"} 1
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "walg_backups"))
}

func TestGetDeltaOriginName(t *testing.T) {
	// A full backup and a delta backup that both started in WAL file ...02
	full := BackupInfo{
		BackupName: "base_000000010000000000000002",
		StartTime:  time.Unix(1000, 0),
		FinishTime: time.Unix(1100, 0),
	}
	deltaSameSegment := BackupInfo{
		BackupName: "base_000000010000000000000002_D_000000010000000000000001",
		StartTime:  time.Unix(1200, 0),
		FinishTime: time.Unix(1300, 0),
	}
	child := BackupInfo{
		BackupName: "base_000000010000000000000004_D_000000010000000000000002",
		StartTime:  time.Unix(2000, 0),
		FinishTime: time.Unix(2100, 0),
	}
	// Starts with the expected parent name, but is not a parent candidate
	unrelated := BackupInfo{
		BackupName: "base_0000000100000000000000020",
		StartTime:  time.Unix(1500, 0),
		FinishTime: time.Unix(1600, 0),
	}

	backups := []BackupInfo{full, unrelated, deltaSameSegment, child}
	require.Equal(t, deltaSameSegment.BackupName, child.GetDeltaOriginName(backups))
	require.Empty(t, full.GetDeltaOriginName(backups))

	// The latest candidate that finished before the child started wins
	require.Equal(t, full.BackupName, child.GetDeltaOriginName([]BackupInfo{full, unrelated, child}))

	// Without a candidate that finished in time, fall back to the first candidate
	early := child
	early.StartTime = time.Unix(900, 0)
	require.Equal(t, full.BackupName, early.GetDeltaOriginName([]BackupInfo{full, deltaSameSegment, early}))

	// The parent was deleted
	require.Empty(t, child.GetDeltaOriginName([]BackupInfo{child}))
}

func TestPitrWindow(t *testing.T) {
	e, reg := newTestExporter(t)

	permanent := fullBackup
	permanent.BackupName = "base_000000010000000000000001"
	permanent.IsPermanent = true
	permanent.Time = time.Now().Add(-48 * time.Hour)
	oldest := fullBackup
	oldest.Time = time.Now().Add(-2 * time.Hour)
	newest := deltaBackup
	newest.Time = time.Now().Add(-time.Hour)

	e.updateBackupMetrics([]BackupInfo{newest, permanent, oldest})
	// Permanent backups are excluded, and the window is computed when collected
	require.InDelta(t, (2 * time.Hour).Seconds(), gatherGauge(t, reg, "walg_pitr_window_seconds"), 60)

	e.updateBackupMetrics([]BackupInfo{permanent})
	require.Zero(t, gatherGauge(t, reg, "walg_pitr_window_seconds"))
}

// gatherGauge returns the value of the single-series gauge with the given name
func gatherGauge(t *testing.T, reg prometheus.Gatherer, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			require.Len(t, mf.GetMetric(), 1)
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	require.Failf(t, "metric not found", "%s", name)
	return 0
}

// writeScript writes an executable shell script that stands in for wal-g
func writeScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal-g")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o755))
	return path
}

func TestScrapeFailureKeepsLastSnapshot(t *testing.T) {
	e, reg := newTestExporter(t)
	e.walgPath = writeScript(t, "cat testdata/backup-list.json")

	e.scrapeBackupMetrics(t.Context())
	require.Equal(t, 1.0, testutil.ToFloat64(e.scrapeSuccess.WithLabelValues(operationBackupList)))
	require.Positive(t, testutil.ToFloat64(e.scrapeLastSuccess.WithLabelValues(operationBackupList)))
	backupInfoCount := testutil.CollectAndCount(e, "walg_backup_info")
	require.Positive(t, backupInfoCount)

	e.walgPath = writeScript(t, "echo 'ERROR: storage is unreachable' >&2; exit 1")
	_, err := e.getBackupInfo(t.Context())
	require.ErrorContains(t, err, "storage is unreachable")

	e.scrapeBackupMetrics(t.Context())
	require.Equal(t, 0.0, testutil.ToFloat64(e.scrapeSuccess.WithLabelValues(operationBackupList)))
	require.Equal(t, 1.0, testutil.ToFloat64(e.errors.WithLabelValues(operationBackupList, "command_failed")))
	// The last good data is still exported, its age is visible through the last success timestamp
	require.Equal(t, backupInfoCount, testutil.CollectAndCount(e, "walg_backup_info"))

	_, err = reg.Gather()
	require.NoError(t, err)
}

func TestRunWalgTimeout(t *testing.T) {
	e, _ := newTestExporter(t)
	e.walgPath = writeScript(t, "exec sleep 10")

	start := time.Now()
	_, err := e.runWalg(t.Context(), 100*time.Millisecond, "backup-list")
	require.ErrorContains(t, err, "timed out")
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestErrorCountersExportedFromStart(t *testing.T) {
	e, _ := newTestExporter(t)
	require.Equal(t, 3, testutil.CollectAndCount(e, "walg_errors_total"))
}
