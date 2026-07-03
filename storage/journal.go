package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultJournalMaxSize   = 4 << 20 // compact when the file outgrows this
	defaultJournalRetention = 72 * time.Hour
)

// Journal persists L3 history — transition rings and incidents — as one
// NDJSON file, so a restarted store rehydrates the history that L1/L2 cannot
// rebuild from scrapes. Pass it to NewMemoryStore via WithJournal: the
// constructor replays it (retention applied) and compacts it back to the
// live state, and the store appends each accepted transition and incident
// upsert afterwards. Compaction reruns whenever the file outgrows its size
// bound, so the journal stays proportional to the bounded live history, not
// to uptime.
type Journal struct {
	path      string
	maxSize   int64
	retention time.Duration

	mu      sync.Mutex
	file    *os.File
	size    int64
	lastErr error
}

type JournalOption func(*Journal)

// WithJournalMaxSize sets the file size at which the store rewrites the
// journal from live state.
func WithJournalMaxSize(bytes int64) JournalOption {
	return func(j *Journal) {
		if bytes > 0 {
			j.maxSize = bytes
		}
	}
}

// WithJournalRetention bounds replay: identities whose newest transition is
// older than the retention are dropped on open, so identity churn cannot
// accumulate orphaned rings across restarts.
func WithJournalRetention(retention time.Duration) JournalOption {
	return func(j *Journal) {
		if retention > 0 {
			j.retention = retention
		}
	}
}

type journalEntry struct {
	Kind       string      `json:"kind"` // "transition" | "incident"
	Identity   string      `json:"identity,omitempty"`
	Transition *Transition `json:"transition,omitempty"`
	Incident   *Incident   `json:"incident,omitempty"`
}

// OpenJournal opens (or creates) the journal file for appending. Replay does
// not happen here; NewMemoryStore performs it so caps, dedup, and incident
// TTL apply through the store's own code paths.
func OpenJournal(path string, opts ...JournalOption) (*Journal, error) {
	this := &Journal{
		path:      path,
		maxSize:   defaultJournalMaxSize,
		retention: defaultJournalRetention,
	}
	for _, opt := range opts {
		opt(this)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("journal dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("journal: %w", err)
	}
	this.file = file
	this.size = info.Size()
	return this, nil
}

// Err reports the last append or rewrite failure; the in-memory store keeps
// working regardless.
func (this *Journal) Err() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.lastErr
}

func (this *Journal) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.file == nil {
		return nil
	}
	err := this.file.Close()
	this.file = nil
	return err
}

func (this *Journal) appendTransition(identity string, transition Transition) {
	this.appendEntry(journalEntry{Kind: "transition", Identity: identity, Transition: &transition})
}

func (this *Journal) appendIncident(incident Incident) {
	this.appendEntry(journalEntry{Kind: "incident", Incident: &incident})
}

func (this *Journal) appendEntry(entry journalEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		this.setErr(err)
		return
	}
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.file == nil {
		return
	}
	n, err := this.file.Write(append(data, '\n'))
	this.size += int64(n)
	if err != nil {
		this.lastErr = err
	}
}

func (this *Journal) setErr(err error) {
	this.mu.Lock()
	this.lastErr = err
	this.mu.Unlock()
}

func (this *Journal) needsCompaction() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.size > this.maxSize
}

// read returns every decodable entry in file order. A truncated tail line
// (crash mid-append) is skipped rather than failing the whole replay.
func (this *Journal) read() ([]journalEntry, error) {
	file, err := os.Open(this.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	var out []journalEntry
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 1 {
			var entry journalEntry
			if unmarshalErr := json.Unmarshal(line, &entry); unmarshalErr == nil {
				out = append(out, entry)
			}
		}
		if err != nil {
			return out, nil
		}
	}
}

// rewrite atomically replaces the journal with the given entries (write to a
// temp file, rename over) and reopens the append handle.
func (this *Journal) rewrite(entries []journalEntry) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	temp, err := os.CreateTemp(filepath.Dir(this.path), filepath.Base(this.path)+".compact-*")
	if err != nil {
		this.lastErr = err
		return err
	}
	writer := bufio.NewWriter(temp)
	var size int64
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		n, err := writer.Write(append(data, '\n'))
		size += int64(n)
		if err != nil {
			temp.Close()
			os.Remove(temp.Name())
			this.lastErr = err
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		temp.Close()
		os.Remove(temp.Name())
		this.lastErr = err
		return err
	}
	if err := temp.Close(); err != nil {
		os.Remove(temp.Name())
		this.lastErr = err
		return err
	}
	if err := os.Rename(temp.Name(), this.path); err != nil {
		os.Remove(temp.Name())
		this.lastErr = err
		return err
	}
	if this.file != nil {
		_ = this.file.Close()
	}
	file, err := os.OpenFile(this.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		this.file = nil
		this.lastErr = err
		return err
	}
	this.file = file
	this.size = size
	this.lastErr = nil
	return nil
}
