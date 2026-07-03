package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// FileChartStore is the file-backed ChartStore: a write-through wrapper
// around MemoryChartStore that appends every non-empty bucket write to NDJSON
// day segments under a directory, and replays the last window on open. Reads
// always come from memory, so query behavior is identical to the in-memory
// backend; the files only buy history across restarts.
//
// Bounds mirror the in-memory ones: segments older than the window are
// deleted, a healthy fleet appends nothing, and repeated writes of an
// unchanged bucket are deduplicated, so disk usage is proportional to how
// much was wrong inside the window, not to uptime.
var _ ChartStore = (*FileChartStore)(nil)

type FileChartStore struct {
	memory     *MemoryChartStore
	dir        string
	bucketSize time.Duration
	window     int

	mu         sync.Mutex
	file       *os.File
	day        string // UTC date of the open segment, e.g. "2026-07-03"
	lastBucket time.Time
	lastDigest string
	lastErr    error
}

type chartFileRecord struct {
	T      time.Time         `json:"t"`
	States []TriggeringState `json:"states"`
}

// NewFileChartStore opens (or creates) the chart directory, deletes segments
// older than the window, and replays the remaining ones into memory.
func NewFileChartStore(dir string, bucketSize time.Duration, window int) (*FileChartStore, error) {
	if bucketSize <= 0 {
		bucketSize = time.Minute
	}
	if window <= 0 {
		window = 24 * 60
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("chart store dir: %w", err)
	}
	this := &FileChartStore{
		memory:     NewMemoryChartStore(bucketSize, window),
		dir:        dir,
		bucketSize: bucketSize,
		window:     window,
	}
	if err := this.replay(); err != nil {
		return nil, err
	}
	return this, nil
}

// Err reports the last write failure, if any. Record cannot return errors
// (the ChartStore interface has none), so persistent write problems surface
// here while the in-memory chart keeps working.
func (this *FileChartStore) Err() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.lastErr
}

func (this *FileChartStore) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.file == nil {
		return nil
	}
	err := this.file.Close()
	this.file = nil
	return err
}

func (this *FileChartStore) Record(bucket time.Time, triggering []TriggeringState) {
	this.memory.Record(bucket, triggering)
	if len(triggering) == 0 {
		return
	}
	t := bucket.Truncate(this.bucketSize)
	digest := triggeringDigest(triggering)

	this.mu.Lock()
	defer this.mu.Unlock()
	if t.Equal(this.lastBucket) && digest == this.lastDigest {
		return
	}
	if err := this.append(chartFileRecord{T: t, States: triggering}); err != nil {
		this.lastErr = err
		return
	}
	this.lastBucket = t
	this.lastDigest = digest
}

func (this *FileChartStore) Range(scope string, from, to time.Time, buckets int) ([]BucketCounts, error) {
	return this.memory.Range(scope, from, to, buckets)
}

func (this *FileChartStore) Bucket(scope string, t time.Time) ([]TriggeringState, error) {
	return this.memory.Bucket(scope, t)
}

// append writes one record to the day segment for the record's bucket time,
// rolling to a new segment (and sweeping expired ones) on day change. Callers
// hold the lock.
func (this *FileChartStore) append(record chartFileRecord) error {
	day := record.T.UTC().Format(time.DateOnly)
	if this.file == nil || day != this.day {
		if this.file != nil {
			_ = this.file.Close()
			this.file = nil
		}
		this.sweepSegments(time.Now().Add(-time.Duration(this.window) * this.bucketSize))
		file, err := os.OpenFile(this.segmentPath(day), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		this.file = file
		this.day = day
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = this.file.Write(append(data, '\n'))
	return err
}

func (this *FileChartStore) segmentPath(day string) string {
	return filepath.Join(this.dir, "chart-"+day+".ndjson")
}

// replay loads the segments still inside the window into memory and deletes
// the rest.
func (this *FileChartStore) replay() error {
	cutoff := time.Now().Add(-time.Duration(this.window) * this.bucketSize)
	this.sweepSegments(cutoff)
	entries, err := os.ReadDir(this.dir)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if segmentDay(entry.Name()) != "" {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if err := this.replaySegment(filepath.Join(this.dir, name), cutoff); err != nil {
			return fmt.Errorf("chart segment %s: %w", name, err)
		}
	}
	return nil
}

func (this *FileChartStore) replaySegment(path string, cutoff time.Time) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 1 {
			var record chartFileRecord
			if unmarshalErr := json.Unmarshal(line, &record); unmarshalErr == nil && record.T.After(cutoff) {
				this.memory.Record(record.T, record.States)
			}
		}
		if err != nil {
			return nil // io.EOF, or a truncated tail line: keep what replayed
		}
	}
}

// sweepSegments deletes segments whose whole day lies before the cutoff.
// Callers hold the lock (or run before the store is shared).
func (this *FileChartStore) sweepSegments(cutoff time.Time) {
	entries, err := os.ReadDir(this.dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		day := segmentDay(entry.Name())
		if day == "" {
			continue
		}
		date, err := time.Parse(time.DateOnly, day)
		if err != nil {
			continue
		}
		if date.Add(24 * time.Hour).Before(cutoff) {
			_ = os.Remove(filepath.Join(this.dir, entry.Name()))
		}
	}
}

func segmentDay(name string) string {
	day, ok := strings.CutPrefix(name, "chart-")
	if !ok {
		return ""
	}
	day, ok = strings.CutSuffix(day, ".ndjson")
	if !ok {
		return ""
	}
	return day
}

// triggeringDigest fingerprints a bucket's contents so repeated identical
// writes within one bucket (several ingests per minute of an unchanged
// degraded fleet) append only one line.
func triggeringDigest(triggering []TriggeringState) string {
	sorted := append([]TriggeringState(nil), triggering...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Identity < sorted[j].Identity })
	return hashJSON(sorted)
}
