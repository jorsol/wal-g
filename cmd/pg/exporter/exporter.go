package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	operationBackupList   = "backup-list"
	operationWalVerify    = "wal-verify"
	operationStorageCheck = "storage-check"

	// maxStderrInError is the number of trailing stderr bytes of a failed wal-g command kept in the error
	maxStderrInError = 1024
	// commandWaitDelay bounds how long to wait for a killed wal-g command's output pipes to close
	commandWaitDelay = 5 * time.Second
)

// Descriptions of the metrics that are emitted from snapshots on each Collect
var (
	pitrWindowDesc = prometheus.NewDesc(
		"walg_pitr_window_seconds",
		"Point-in-time recovery window size in seconds",
		nil, nil,
	)

	walVerifyCheckDesc = prometheus.NewDesc(
		"walg_wal_verify_status",
		"WAL verify status (1 = OK, 0 = FAILURE, 2 = WARNING, -1 = UNKNOWN)",
		[]string{"operation"}, nil,
	)

	walIntegrityDesc = prometheus.NewDesc(
		"walg_wal_integrity_status",
		"WAL integrity status per timeline (1 = no lost segments, 0 = lost segments)",
		[]string{"timeline_id", "timeline_hex"}, nil,
	)

	walSegmentsDesc = prometheus.NewDesc(
		"walg_wal_segments",
		"Number of WAL segments per timeline and wal-verify integrity status",
		[]string{"timeline_id", "timeline_hex", "status"}, nil,
	)

	backupCountDesc = prometheus.NewDesc(
		"walg_backups",
		"Number of backups by type",
		[]string{"backup_type"}, nil,
	)

	backupInfoDesc = prometheus.NewDesc(
		"walg_backup_info",
		"Information about stored backups. Value is always 1.",
		[]string{"backup_name", "backup_type", "wal_file", "pg_version", "start_lsn", "finish_lsn", "is_permanent", "delta_origin"}, nil,
	)

	backupStartTimestampDesc = prometheus.NewDesc(
		"walg_backup_start_timestamp",
		"Start time of the backup (Unix timestamp).",
		[]string{"backup_name"}, nil,
	)

	backupFinishTimestampDesc = prometheus.NewDesc(
		"walg_backup_finish_timestamp",
		"Finish time of the backup (Unix timestamp).",
		[]string{"backup_name"}, nil,
	)

	backupUncompressedSizeDesc = prometheus.NewDesc(
		"walg_backup_uncompressed_size_bytes",
		"Uncompressed size of the backup in bytes.",
		[]string{"backup_name"}, nil,
	)

	backupCompressedSizeDesc = prometheus.NewDesc(
		"walg_backup_compressed_size_bytes",
		"Compressed size of the backup in bytes.",
		[]string{"backup_name"}, nil,
	)
)

// WalgExporter implements the Prometheus Collector interface
type WalgExporter struct {
	logger         *slog.Logger // Added structured logger instance
	walgPath       string
	walgConfigPath string

	// Scrape intervals
	backupScrapeInterval  time.Duration
	verifyScrapeInterval  time.Duration
	storageScrapeInterval time.Duration

	// Timeouts of the wal-g commands
	backupTimeout  time.Duration
	verifyTimeout  time.Duration
	storageTimeout time.Duration

	// Metrics
	errors            *prometheus.CounterVec
	scrapeErrors      prometheus.Counter
	scrapeSuccess     *prometheus.GaugeVec
	scrapeLastSuccess *prometheus.GaugeVec

	// Metrics of backups, emitted from backupSnap on each Collect
	backupScrapeDuration prometheus.Gauge
	backupSnap           atomic.Pointer[backupSnapshot]

	// Metrics of wal-verify, emitted from walSnap on each Collect
	verifyScrapeDuration prometheus.Gauge
	walSnap              atomic.Pointer[walSnapshot]

	// Storage aliveness metrics
	storageAlive   prometheus.Gauge
	storageLatency prometheus.Gauge
}

// backupSnapshot holds the per-backup series published by the last successful
// backup-list scrape. It is never mutated after being stored, so Collect always
// sees a complete set. Maps are keyed by label values, so duplicate backups
// overwrite each other instead of producing duplicate series.
type backupSnapshot struct {
	counts       map[string]float64     // backup_type
	info         map[[8]string]struct{} // walg_backup_info labels; value is always 1
	start        map[string]float64     // backup_name
	finish       map[string]float64     // backup_name
	uncompressed map[string]float64     // backup_name
	compressed   map[string]float64     // backup_name

	// pitrEarliest is the time of the earliest non-permanent backup, zero if there is none.
	// The PITR window is derived from it on each Collect so that it never lags behind.
	pitrEarliest time.Time
}

// walSnapshot holds the series published by the last successful wal-verify scrape.
type walSnapshot struct {
	verify    map[string]float64    // operation
	integrity map[[2]string]float64 // timeline_id, timeline_hex
	segments  map[[3]string]float64 // timeline_id, timeline_hex, status
}

// BackupInfo represents backup information from backup-list --detail --json
// Updated to match real wal-g output format
type BackupInfo struct {
	BackupName       string      `json:"backup_name"`
	Time             time.Time   `json:"time"`
	WalFileName      string      `json:"wal_file_name"`
	StorageName      string      `json:"storage_name"`
	StartTime        time.Time   `json:"start_time"`
	FinishTime       time.Time   `json:"finish_time"`
	DateFmt          string      `json:"date_fmt"`
	Hostname         string      `json:"hostname"`
	DataDir          string      `json:"data_dir"`
	PgVersion        int         `json:"pg_version"`
	StartLSN         uint64      `json:"start_lsn"`  // Real wal-g returns this as number
	FinishLSN        uint64      `json:"finish_lsn"` // Real wal-g returns this as number
	IsPermanent      bool        `json:"is_permanent"`
	SystemIdentifier uint64      `json:"system_identifier"`
	UncompressedSize int64       `json:"uncompressed_size"`
	CompressedSize   int64       `json:"compressed_size"`
	UserData         interface{} `json:"user_data,omitempty"`
	// Note: Real WAL-G doesn't include is_full field, we determine it from backup name
}

// WalVerifyResponse represents information from wal-verify integrity timeline --json
type WalVerifyResponse struct {
	Integrity IntegrityData `json:"integrity"`
	Timeline  TimelineData  `json:"timeline"`
}

// IntegrityData represents the inner integrity object
type IntegrityData struct {
	Status  string            `json:"status"`
	Details []IntegrityDetail `json:"details"`
}

// IntegrityDetail represents the individual items in the integrity details array
type IntegrityDetail struct {
	TimelineID    int    `json:"timeline_id"`
	StartSegment  string `json:"start_segment"`
	EndSegment    string `json:"end_segment"`
	SegmentsCount int    `json:"segments_count"`
	Status        string `json:"status"`
}

// TimelineData represents the inner timeline object
type TimelineData struct {
	Status  string         `json:"status"`
	Details TimelineDetail `json:"details"`
}

// TimelineDetail represents the timeline details object
type TimelineDetail struct {
	CurrentTimelineID        int `json:"current_timeline_id"`
	HighestStorageTimelineID int `json:"highest_storage_timeline_id"`
}

// Helper method to get backup type
func (b *BackupInfo) GetBackupType() string {
	// WAL-G doesn't include is_full in JSON output, so we determine backup type
	// from the backup name using the actual WAL-G naming convention:
	// - Incremental backups have "_D_" in their name (added during delta backup creation)
	// - Full backups don't have "_D_" in their name
	if b.IsFullBackup() {
		return "full"
	}
	return "delta"
}

// Helper method to check if backup is full
func (b *BackupInfo) IsFullBackup() bool {
	// In WAL-G, incremental/delta backups get "_D_" suffix added to their name
	// (see backup_push_handler.go line 285: bh.CurBackupInfo.Name = bh.CurBackupInfo.Name + "_D_" + ...)
	// So if backup name contains "_D_", it's incremental; otherwise it's full
	return !strings.Contains(b.BackupName, "_D_")
}

// GetDeltaOriginName extracts the parent backup name for delta backups
func (b *BackupInfo) GetDeltaOriginName(backups []BackupInfo) string {
	if b.IsFullBackup() {
		return "" // Full backups don't have a base backup
	}

	// Find the "_D_" pattern in the backup name
	deltaIndex := strings.Index(b.BackupName, "_D_")
	if deltaIndex == -1 || len(b.BackupName) <= deltaIndex+3 {
		return ""
	}

	// The part after "_D_" is the WAL file of the parent backup, which is either a full
	// backup "base_<wal>" or a delta backup "base_<wal>_D_<wal>"
	expectedParent := "base_" + b.BackupName[deltaIndex+3:] // +3 to skip "_D_"

	// Several backups may start in the same WAL file, so prefer the latest one that
	// finished before this backup started: that is the one it was taken on top of
	var parent *BackupInfo
	fallback := ""
	for i := range backups {
		candidate := &backups[i]
		if b.BackupName == candidate.BackupName ||
			(candidate.BackupName != expectedParent && !strings.HasPrefix(candidate.BackupName, expectedParent+"_D_")) {
			continue
		}
		if fallback == "" {
			fallback = candidate.BackupName
		}
		if candidate.FinishTime.After(b.StartTime) {
			continue
		}
		if parent == nil || candidate.FinishTime.After(parent.FinishTime) {
			parent = candidate
		}
	}

	if parent != nil {
		return parent.BackupName
	}
	return fallback
}

// NewWalgExporter creates a new WAL-G exporter
func NewWalgExporter(
	logger *slog.Logger,
	walgPath string,
	backupScrapeInterval time.Duration,
	verifyScrapeInterval time.Duration,
	storageScrapeInterval time.Duration,
	backupTimeout time.Duration,
	verifyTimeout time.Duration,
	storageTimeout time.Duration,
	walgConfigPath string,
) *WalgExporter {
	e := &WalgExporter{
		logger:                logger,
		walgPath:              walgPath,
		backupScrapeInterval:  backupScrapeInterval,
		verifyScrapeInterval:  verifyScrapeInterval,
		storageScrapeInterval: storageScrapeInterval,
		backupTimeout:         backupTimeout,
		verifyTimeout:         verifyTimeout,
		storageTimeout:        storageTimeout,
		walgConfigPath:        walgConfigPath,

		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "walg_errors_total",
			Help: "Total number of WAL-G errors",
		}, []string{"operation", "error_type"}),

		scrapeSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "walg_scrape_success",
			Help: "Whether the last run of the WAL-G command succeeded (1 = success, 0 = failure)",
		}, []string{"operation"}),

		scrapeLastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "walg_scrape_last_success_timestamp_seconds",
			Help: "Time of the last successful run of the WAL-G command (Unix timestamp).",
		}, []string{"operation"}),

		backupScrapeDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "walg_backup_list_duration_seconds",
			Help: "Time taken to execute 'backup-list' during the last collector run.",
		}),

		verifyScrapeDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "walg_wal_verify_duration_seconds",
			Help: "Time taken to execute 'wal-verify' during the last collector run.",
		}),

		scrapeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "walg_scrape_errors_total",
			Help: "Total number of scrape errors",
		}),

		storageAlive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "walg_storage_up",
			Help: "Storage connectivity status (1 = up, 0 = down)",
		}),

		storageLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "walg_storage_latency_seconds",
			Help: "Storage operation latency in seconds",
		}),
	}

	// Export the error counters from the start so that rate() and increase() see the first error
	e.errors.WithLabelValues(operationBackupList, "command_failed")
	e.errors.WithLabelValues(operationWalVerify, "command_failed")
	e.errors.WithLabelValues(operationStorageCheck, "connectivity_failed")

	return e
}

// Describe implements the Prometheus Collector interface
func (e *WalgExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- pitrWindowDesc
	e.errors.Describe(ch)
	e.scrapeSuccess.Describe(ch)
	e.scrapeLastSuccess.Describe(ch)
	ch <- walVerifyCheckDesc
	ch <- walIntegrityDesc
	ch <- walSegmentsDesc
	ch <- backupCountDesc
	ch <- backupInfoDesc
	ch <- backupStartTimestampDesc
	ch <- backupFinishTimestampDesc
	ch <- backupUncompressedSizeDesc
	ch <- backupCompressedSizeDesc
	e.backupScrapeDuration.Describe(ch)
	e.verifyScrapeDuration.Describe(ch)
	e.scrapeErrors.Describe(ch)
	e.storageAlive.Describe(ch)
	e.storageLatency.Describe(ch)
}

// Collect implements the Prometheus Collector interface
func (e *WalgExporter) Collect(ch chan<- prometheus.Metric) {
	e.errors.Collect(ch)
	e.scrapeSuccess.Collect(ch)
	e.scrapeLastSuccess.Collect(ch)

	if s := e.walSnap.Load(); s != nil {
		for operation, v := range s.verify {
			e.emitGauge(ch, walVerifyCheckDesc, v, operation)
		}
		for labels, v := range s.integrity {
			e.emitGauge(ch, walIntegrityDesc, v, labels[:]...)
		}
		for labels, v := range s.segments {
			e.emitGauge(ch, walSegmentsDesc, v, labels[:]...)
		}
	}

	if s := e.backupSnap.Load(); s != nil {
		e.emitGauge(ch, pitrWindowDesc, pitrWindow(s.pitrEarliest))
		for backupType, v := range s.counts {
			e.emitGauge(ch, backupCountDesc, v, backupType)
		}
		for labels := range s.info {
			e.emitGauge(ch, backupInfoDesc, 1, labels[:]...)
		}
		for name, v := range s.start {
			e.emitGauge(ch, backupStartTimestampDesc, v, name)
		}
		for name, v := range s.finish {
			e.emitGauge(ch, backupFinishTimestampDesc, v, name)
		}
		for name, v := range s.uncompressed {
			e.emitGauge(ch, backupUncompressedSizeDesc, v, name)
		}
		for name, v := range s.compressed {
			e.emitGauge(ch, backupCompressedSizeDesc, v, name)
		}
	}

	e.backupScrapeDuration.Collect(ch)
	e.verifyScrapeDuration.Collect(ch)
	e.scrapeErrors.Collect(ch)
	e.storageAlive.Collect(ch)
	e.storageLatency.Collect(ch)
}

// emitGauge sends a single gauge sample. A series with invalid label values is
// logged and dropped so that it cannot fail the whole scrape.
func (e *WalgExporter) emitGauge(ch chan<- prometheus.Metric, desc *prometheus.Desc, value float64, labelValues ...string) {
	m, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, value, labelValues...)
	if err != nil {
		e.logger.Warn("Dropping invalid metric", "desc", desc.String(), "error", err)
		return
	}
	ch <- m
}

// Start begins the metrics collection loops and blocks until ctx is canceled.
// Each WAL-G command runs in its own loop so that a slow one cannot delay the others.
func (e *WalgExporter) Start(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() { runPeriodically(ctx, e.storageScrapeInterval, e.checkStorageAliveness) })
	wg.Go(func() { runPeriodically(ctx, e.backupScrapeInterval, e.scrapeBackupMetrics) })
	wg.Go(func() { runPeriodically(ctx, e.verifyScrapeInterval, e.scrapeWalMetrics) })

	e.logger.Info("Started periodic WAL-G metrics collection")
	wg.Wait()
	e.logger.Info("Exporter context canceled, stopped metrics collection")
}

// runPeriodically runs scrape immediately and then every interval until ctx is canceled
func runPeriodically(ctx context.Context, interval time.Duration, scrape func(context.Context)) {
	scrape(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scrape(ctx)
		}
	}
}

// setScrapeSuccess records the outcome of the last run of a WAL-G command
func (e *WalgExporter) setScrapeSuccess(operation string, success bool) {
	if !success {
		e.scrapeSuccess.WithLabelValues(operation).Set(0)
		return
	}
	e.scrapeSuccess.WithLabelValues(operation).Set(1)
	e.scrapeLastSuccess.WithLabelValues(operation).SetToCurrentTime()
}

// scrapeBackupMetrics collects backup metrics from WAL-G
func (e *WalgExporter) scrapeBackupMetrics(ctx context.Context) {
	start := time.Now()
	defer func() {
		e.backupScrapeDuration.Set(time.Since(start).Seconds())
	}()

	// Get backup information
	backups, err := e.getBackupInfo(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return // Shutting down
		}
		e.logger.Error("Error getting backup info", "error", err, "operation", operationBackupList)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues(operationBackupList, "command_failed").Inc()
		e.setScrapeSuccess(operationBackupList, false)
		return
	}

	// Update backup metrics
	e.updateBackupMetrics(backups)
	e.setScrapeSuccess(operationBackupList, true)

	e.logger.Info("Metrics for backups scrape completed", "duration", time.Since(start))
}

// scrapeWalMetrics collects wal metrics from WAL-G
func (e *WalgExporter) scrapeWalMetrics(ctx context.Context) {
	start := time.Now()
	defer func() {
		e.verifyScrapeDuration.Set(time.Since(start).Seconds())
	}()

	// Get WAL verify information
	verifyData, err := e.getWalVerify(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return // Shutting down
		}
		e.logger.Error("Error getting WAL verify info", "error", err, "operation", operationWalVerify)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues(operationWalVerify, "command_failed").Inc()
		e.setScrapeSuccess(operationWalVerify, false)
		return
	}

	// Update WAL verify metrics
	e.updateWalMetrics(verifyData)
	e.setScrapeSuccess(operationWalVerify, true)

	e.logger.Info("Metrics for WALs verify scrape completed", "duration", time.Since(start))
}

// runWalg executes wal-g with the given arguments and returns its standard output.
// The command is killed when timeout expires or ctx is canceled.
func (e *WalgExporter) runWalg(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	if e.walgConfigPath != "" {
		args = append(args, "--config", e.walgConfigPath)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, e.walgPath, args...)
	cmd.WaitDelay = commandWaitDelay
	output, err := cmd.Output()
	if err == nil {
		return output, nil
	}

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil, fmt.Errorf("timed out after %s: %w", timeout, err)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		// wal-g logs to stderr; the actual error is at the end of it
		if stderr := strings.TrimSpace(string(exitErr.Stderr)); stderr != "" {
			if len(stderr) > maxStderrInError {
				stderr = "..." + stderr[len(stderr)-maxStderrInError:]
			}
			return nil, fmt.Errorf("%w: %s", err, stderr)
		}
	}
	return nil, err
}

// getBackupInfo executes wal-g backup-list --detail --json
func (e *WalgExporter) getBackupInfo(ctx context.Context) ([]BackupInfo, error) {
	output, err := e.runWalg(ctx, e.backupTimeout, "backup-list", "--detail", "--json")
	if err != nil {
		return nil, fmt.Errorf("failed to execute backup-list: %w", err)
	}

	var backups []BackupInfo
	if err := json.Unmarshal(output, &backups); err != nil {
		return nil, fmt.Errorf("failed to parse backup-list output: %w", err)
	}

	return backups, nil
}

// getWalVerify executes wal-g wal-verify integrity timeline --json
func (e *WalgExporter) getWalVerify(ctx context.Context) (*WalVerifyResponse, error) {
	output, err := e.runWalg(ctx, e.verifyTimeout, "wal-verify", "integrity", "timeline", "--json")
	if err != nil {
		return nil, fmt.Errorf("failed to execute wal-verify: %w", err)
	}

	var walVerifyResponse WalVerifyResponse
	if err := json.Unmarshal(output, &walVerifyResponse); err != nil {
		return nil, fmt.Errorf("failed to parse wal-verify output: %w", err)
	}

	return &walVerifyResponse, nil
}

// updateBackupMetrics publishes a new snapshot of backup-related metrics with detailed labels
func (e *WalgExporter) updateBackupMetrics(backups []BackupInfo) {
	snap := &backupSnapshot{
		counts:       make(map[string]float64, 2),
		info:         make(map[[8]string]struct{}, len(backups)),
		start:        make(map[string]float64, len(backups)),
		finish:       make(map[string]float64, len(backups)),
		uncompressed: make(map[string]float64, len(backups)),
		compressed:   make(map[string]float64, len(backups)),
	}

	fullCount, deltaCount := 0, 0
	// With several storages, backup-list reports the same backup once per storage
	seen := make(map[string]struct{}, len(backups))

	// Create detailed metrics for each backup
	for i := range backups {
		backup := &backups[i]
		backupType := backup.GetBackupType()
		if _, ok := seen[backup.BackupName]; !ok {
			seen[backup.BackupName] = struct{}{}
			if backup.IsFullBackup() {
				fullCount++
			} else {
				deltaCount++
			}
		}

		// The PITR window starts at the earliest non-permanent backup (matches wal-verify behavior)
		if !backup.IsPermanent && (snap.pitrEarliest.IsZero() || backup.Time.Before(snap.pitrEarliest)) {
			snap.pitrEarliest = backup.Time
		}

		isPermanent := strconv.FormatBool(backup.IsPermanent)

		// Get parent backup name for delta backups (empty for full backups)
		deltaOriginBackupName := backup.GetDeltaOriginName(backups)

		// Labels for detailed backup information
		labels := [8]string{
			backup.BackupName,              // backup_name
			backupType,                     // backup_type
			backup.WalFileName,             // wal_file
			strconv.Itoa(backup.PgVersion), // pg_version
			LSN(backup.StartLSN).String(),  // start_lsn
			LSN(backup.FinishLSN).String(), // finish_lsn
			isPermanent,                    // is_permanent
			deltaOriginBackupName,          // delta_origin
		}
		snap.info[labels] = struct{}{}

		// Set start and finish timestamps for this specific backup
		snap.start[backup.BackupName] = float64(backup.StartTime.Unix())
		snap.finish[backup.BackupName] = float64(backup.FinishTime.Unix())

		// Set the size metrics for the specific backup
		snap.uncompressed[backup.BackupName] = float64(backup.UncompressedSize)
		snap.compressed[backup.BackupName] = float64(backup.CompressedSize)
	}

	// Set backup counts by type
	snap.counts["full"] = float64(fullCount)
	snap.counts["delta"] = float64(deltaCount)

	// Publish the complete snapshot at once so a concurrent scrape never sees partial data
	e.backupSnap.Store(snap)
}

// pitrWindow returns the PITR window size in seconds for the given earliest backup time
func pitrWindow(earliest time.Time) float64 {
	if earliest.IsZero() {
		return 0 // No eligible backups
	}
	// Ensure we don't report negative time
	return max(0, time.Since(earliest).Seconds())
}

func mapStatus(status string) float64 {
	switch status {
	case "OK":
		return 1.0
	case "WARNING":
		return 2.0
	case "FAILURE":
		return 0.0
	default:
		return -1.0 // "UNKNOWN" state
	}
}

// updateWalMetrics publishes a new snapshot of WAL-related metrics
func (e *WalgExporter) updateWalMetrics(verifyData *WalVerifyResponse) {
	snap := &walSnapshot{
		verify: map[string]float64{
			"integrity": mapStatus(verifyData.Integrity.Status),
			"timeline":  mapStatus(verifyData.Timeline.Status),
		},
		integrity: make(map[[2]string]float64),
		segments:  make(map[[3]string]float64),
	}

	for _, data := range verifyData.Integrity.Details {
		timelineStr := strconv.Itoa(data.TimelineID)
		// Also show the timeline in hex format
		timelineHex := fmt.Sprintf("%08x", data.TimelineID)
		timeline := [2]string{timelineStr, timelineHex}

		// Only lost segments mark the timeline as failed. Segments that are MISSING_DELAYED or
		// MISSING_UPLOADING are expected near the end of the WAL, wal-verify reports them as WARNING.
		if data.Status == "MISSING_LOST" {
			snap.integrity[timeline] = 0.0
		} else if _, exists := snap.integrity[timeline]; !exists {
			snap.integrity[timeline] = 1.0
		}

		snap.segments[[3]string{timelineStr, timelineHex, data.Status}] += float64(data.SegmentsCount)
	}

	// Publish the complete snapshot at once so a concurrent scrape never sees partial data
	e.walSnap.Store(snap)
}

// checkStorageAliveness checks if the storage backend is accessible
func (e *WalgExporter) checkStorageAliveness(ctx context.Context) {
	start := time.Now()

	// Try a simple WAL-G command to test storage connectivity
	_, err := e.runWalg(ctx, e.storageTimeout, "st", "check", "read")
	latency := time.Since(start).Seconds()
	if err != nil && ctx.Err() != nil {
		return // Shutting down
	}

	// Set latency regardless of success/failure
	e.storageLatency.Set(latency)

	if err != nil {
		e.logger.Error("Storage aliveness check failed", "error", err, "duration", latency)
		e.storageAlive.Set(0)
		e.errors.WithLabelValues(operationStorageCheck, "connectivity_failed").Inc()
		e.setScrapeSuccess(operationStorageCheck, false)
	} else {
		e.storageAlive.Set(1)
		e.setScrapeSuccess(operationStorageCheck, true)
		e.logger.Info("Storage check completed", "duration", latency)
	}
}
