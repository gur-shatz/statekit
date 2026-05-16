package scraper

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
)

// scrapedMetricsCollector aggregates Prometheus samples scraped from a
// set of targets and re-emits them through the PrometheusCollector API.
// Each target's last successful scrape replaces its prior samples.
type scrapedMetricsCollector struct {
	mu      sync.RWMutex
	samples map[string][]statekit.PrometheusSample // by target name
	descs   map[string]statekit.PrometheusDesc     // by metric name
}

func newScrapedMetricsCollector() *scrapedMetricsCollector {
	return &scrapedMetricsCollector{
		samples: map[string][]statekit.PrometheusSample{},
		descs:   map[string]statekit.PrometheusDesc{},
	}
}

func (this *scrapedMetricsCollector) DescribePrometheus() []statekit.PrometheusDesc {
	this.mu.RLock()
	defer this.mu.RUnlock()
	out := make([]statekit.PrometheusDesc, 0, len(this.descs))
	for _, d := range this.descs {
		out = append(out, d)
	}
	return out
}

func (this *scrapedMetricsCollector) CollectPrometheus() []statekit.PrometheusSample {
	this.mu.RLock()
	defer this.mu.RUnlock()
	var all []statekit.PrometheusSample
	for _, samples := range this.samples {
		all = append(all, samples...)
	}
	return all
}

// update replaces samples for one target. Descriptors are merged across
// targets; on conflict, the first-seen wins (deferred decision).
func (this *scrapedMetricsCollector) update(targetKey string, samples []statekit.PrometheusSample, descs []statekit.PrometheusDesc) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.samples[targetKey] = samples
	for _, d := range descs {
		if _, ok := this.descs[d.Name]; !ok {
			this.descs[d.Name] = d
		}
	}
}

func buildMetrics(target TargetConfig, cfg Config, client *http.Client, collector *scrapedMetricsCollector) *taskRunner {
	labels := targetLabels(cfg.Labels, target, target.Metrics.Labels)
	interval := resolveInterval(target.Metrics.Interval, target.Interval, cfg.Defaults.Interval)
	timeout := resolveTimeout(target.Metrics.Timeout, target.Timeout, cfg.Defaults.Timeout)
	paths := append([]string(nil), target.Metrics.Paths...)

	name := fmt.Sprintf("%s.metrics", targetIdentifier(target))

	tick := func(ctx context.Context) {
		var allSamples []statekit.PrometheusSample
		var allDescs []statekit.PrometheusDesc
		for _, p := range paths {
			descs, samples, err := scrapeMetricsPath(ctx, client, resolveURL(target.BaseURL, p), timeout)
			if err != nil {
				continue
			}
			for i := range samples {
				if samples[i].Labels == nil {
					samples[i].Labels = map[string]string{}
				}
				for k, v := range labels {
					samples[i].Labels[k] = v
				}
			}
			allSamples = append(allSamples, samples...)
			allDescs = append(allDescs, descs...)
		}
		collector.update(targetKey(target), allSamples, allDescs)
	}

	return &taskRunner{name: name, interval: interval, tick: tick}
}

func scrapeMetricsPath(ctx context.Context, client *http.Client, url string, timeout time.Duration) ([]statekit.PrometheusDesc, []statekit.PrometheusSample, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	return parsePrometheus(string(body))
}

// parsePrometheus parses a subset of the Prometheus text exposition
// format that covers what statekit emits: # HELP, # TYPE, and sample
// lines with optional labels.
func parsePrometheus(text string) ([]statekit.PrometheusDesc, []statekit.PrometheusSample, error) {
	descs := map[string]*statekit.PrometheusDesc{}
	var samples []statekit.PrometheusSample

	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		stripped := strings.TrimSpace(line)
		if stripped == "" {
			continue
		}
		if strings.HasPrefix(stripped, "# HELP ") {
			rest := strings.TrimPrefix(stripped, "# HELP ")
			parts := strings.SplitN(rest, " ", 2)
			if len(parts) >= 1 && parts[0] != "" {
				d := descs[parts[0]]
				if d == nil {
					d = &statekit.PrometheusDesc{Name: parts[0]}
					descs[parts[0]] = d
				}
				if len(parts) == 2 {
					d.Help = parts[1]
				}
			}
			continue
		}
		if strings.HasPrefix(stripped, "# TYPE ") {
			rest := strings.TrimPrefix(stripped, "# TYPE ")
			parts := strings.SplitN(rest, " ", 2)
			if len(parts) == 2 {
				d := descs[parts[0]]
				if d == nil {
					d = &statekit.PrometheusDesc{Name: parts[0]}
					descs[parts[0]] = d
				}
				d.Type = statekit.PrometheusType(parts[1])
			}
			continue
		}
		if strings.HasPrefix(stripped, "#") {
			continue
		}
		sample, err := parseSampleLine(stripped)
		if err != nil {
			continue // skip malformed lines silently
		}
		samples = append(samples, sample)
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	out := make([]statekit.PrometheusDesc, 0, len(descs))
	for _, d := range descs {
		out = append(out, *d)
	}
	return out, samples, nil
}

func parseSampleLine(line string) (statekit.PrometheusSample, error) {
	var name string
	var labels map[string]string
	var rest string

	if i := strings.Index(line, "{"); i >= 0 {
		name = strings.TrimSpace(line[:i])
		end := strings.Index(line[i:], "}")
		if end < 0 {
			return statekit.PrometheusSample{}, fmt.Errorf("missing closing brace")
		}
		labels = parseSampleLabels(line[i+1 : i+end])
		rest = strings.TrimSpace(line[i+end+1:])
	} else {
		idx := strings.IndexAny(line, " \t")
		if idx < 0 {
			return statekit.PrometheusSample{}, fmt.Errorf("no value")
		}
		name = strings.TrimSpace(line[:idx])
		rest = strings.TrimSpace(line[idx:])
	}

	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return statekit.PrometheusSample{}, fmt.Errorf("no value")
	}
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return statekit.PrometheusSample{}, fmt.Errorf("invalid value %q: %w", fields[0], err)
	}
	return statekit.PrometheusSample{Name: name, Labels: labels, Value: value}, nil
}

func parseSampleLabels(s string) map[string]string {
	labels := map[string]string{}
	i := 0
	for i < len(s) {
		for i < len(s) && (s[i] == ' ' || s[i] == ',' || s[i] == '\t') {
			i++
		}
		if i >= len(s) {
			break
		}
		start := i
		for i < len(s) && s[i] != '=' {
			i++
		}
		if i >= len(s) {
			break
		}
		name := strings.TrimSpace(s[start:i])
		i++ // skip =
		if i >= len(s) || s[i] != '"' {
			continue
		}
		i++ // skip opening "
		var b strings.Builder
		for i < len(s) && s[i] != '"' {
			if s[i] == '\\' && i+1 < len(s) {
				switch s[i+1] {
				case 'n':
					b.WriteByte('\n')
				case '\\':
					b.WriteByte('\\')
				case '"':
					b.WriteByte('"')
				default:
					b.WriteByte(s[i+1])
				}
				i += 2
			} else {
				b.WriteByte(s[i])
				i++
			}
		}
		if i < len(s) {
			i++ // skip closing "
		}
		labels[name] = b.String()
	}
	return labels
}
