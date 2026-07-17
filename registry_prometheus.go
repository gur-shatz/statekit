package statekit

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"sort"
	"strings"
)

// Per the Prometheus text exposition format:
//   - HELP text escapes backslash and newline.
//   - Label values escape backslash, newline, and double quote.
var (
	helpEscaper       = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
	labelValueEscaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
)

type prometheusOutputOptions struct {
	dropLabels map[string]struct{}
}

// PrometheusOutputOption customizes one Prometheus rendering or handler.
type PrometheusOutputOption func(*prometheusOutputOptions)

// DropPrometheusLabels removes labels from the rendered output after registry
// and sample labels are merged. It is intended for export-boundary projection.
func DropPrometheusLabels(names ...string) PrometheusOutputOption {
	return func(options *prometheusOutputOptions) {
		if options.dropLabels == nil {
			options.dropLabels = map[string]struct{}{}
		}
		for _, name := range names {
			options.dropLabels[name] = struct{}{}
		}
	}
}

func resolvePrometheusOutputOptions(opts []PrometheusOutputOption) prometheusOutputOptions {
	var options prometheusOutputOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return options
}

func (r *Registry) Prometheus(w io.Writer, opts ...PrometheusOutputOption) error {
	options := resolvePrometheusOutputOptions(opts)
	r.mu.RLock()
	labels := maps.Clone(r.labels)
	collectors := append([]PrometheusCollector(nil), r.collectors...)
	descs := make([]PrometheusDesc, 0, len(r.descs))
	for _, desc := range r.descs {
		descs = append(descs, desc)
	}
	r.mu.RUnlock()

	descByName := make(map[string]PrometheusDesc, len(descs))
	for _, desc := range descs {
		descByName[desc.Name] = desc
	}
	for _, collector := range collectors {
		for _, desc := range collector.DescribePrometheus() {
			existing, ok := descByName[desc.Name]
			if ok && !samePrometheusDesc(existing, desc) {
				return fmt.Errorf("conflicting prometheus descriptor %q", desc.Name)
			}
			if !ok {
				descs = append(descs, desc)
				descByName[desc.Name] = desc
			}
		}
	}

	samples := make([]PrometheusSample, 0)
	for _, snap := range r.Snapshot() {
		samples = append(samples, stateSamples(snap)...)
	}
	for _, collector := range collectors {
		samples = append(samples, collector.CollectPrometheus()...)
	}

	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Name != samples[j].Name {
			return samples[i].Name < samples[j].Name
		}
		return formatLabels(samples[i].Labels) < formatLabels(samples[j].Labels)
	})
	samplesByName := make(map[string][]PrometheusSample)
	for _, sample := range samples {
		descName := prometheusDescName(sample.Name, descs)
		samplesByName[descName] = append(samplesByName[descName], sample)
	}

	sort.Slice(descs, func(i, j int) bool {
		return descs[i].Name < descs[j].Name
	})
	for _, desc := range descs {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n", desc.Name, helpEscaper.Replace(desc.Help)); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", desc.Name, desc.Type); err != nil {
			return err
		}
		if desc.Unit != "" {
			if _, err := fmt.Fprintf(w, "# UNIT %s %s\n", desc.Name, desc.Unit); err != nil {
				return err
			}
		}
		if err := writePrometheusSamples(w, labels, options.dropLabels, samplesByName[desc.Name]); err != nil {
			return err
		}
		delete(samplesByName, desc.Name)
	}

	for _, sample := range samples {
		if _, undescribed := samplesByName[sample.Name]; !undescribed {
			continue
		}
		if err := writePrometheusSamples(w, labels, options.dropLabels, []PrometheusSample{sample}); err != nil {
			return err
		}
	}
	return nil
}

func prometheusDescName(sampleName string, descs []PrometheusDesc) string {
	for _, desc := range descs {
		if desc.Type == PrometheusCounter {
			if sampleName == desc.Name+"_total" || sampleName == desc.Name+"_created" {
				return desc.Name
			}
		}
		if desc.Type == PrometheusHistogram || desc.Type == PrometheusSummary {
			if sampleName == desc.Name ||
				sampleName == desc.Name+"_sum" ||
				sampleName == desc.Name+"_count" ||
				(desc.Type == PrometheusHistogram && sampleName == desc.Name+"_bucket") {
				return desc.Name
			}
		}
	}
	return sampleName
}

func samePrometheusDesc(a, b PrometheusDesc) bool {
	return a.Help == b.Help && a.Type == b.Type && a.Unit == b.Unit && slicesEqual(a.Labels, b.Labels)
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func writePrometheusSamples(w io.Writer, labels map[string]string, dropLabels map[string]struct{}, samples []PrometheusSample) error {
	for _, sample := range samples {
		merged := maps.Clone(labels)
		for k, v := range sample.Labels {
			merged[k] = v
		}
		for name := range dropLabels {
			delete(merged, name)
		}
		if _, err := fmt.Fprintf(w, "%s%s %.4g\n", sample.Name, formatLabels(merged), sample.Value); err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) PrometheusHandler(opts ...PrometheusOutputOption) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := r.Prometheus(w, opts...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func stateSamples(s Snapshot) []PrometheusSample {
	return stateSamplesWithProvenance(s, "", "")
}

func stateSamplesWithProvenance(s Snapshot, inheritedScrapedFrom, inheritedScrapePath string) []PrometheusSample {
	scrapedFrom := inheritedScrapedFrom
	if s.ScrapedFrom != "" {
		scrapedFrom = s.ScrapedFrom
	}
	scrapePath := inheritedScrapePath
	if s.ScrapePath != "" {
		scrapePath = s.ScrapePath
	}
	labels := map[string]string{
		"state":      s.Name,
		"importance": s.Importance.String(),
	}
	// Scraped states carry a provenance tag in Snapshot. Surfacing it here
	// disambiguates two states that happen to share the same name — for
	// example, a local synthetic "health" and a "health" mirrored in from a
	// remote target via state_aggregation. Without this, both would emit
	// identical Prometheus rows (a protocol violation).
	if scrapedFrom != "" {
		labels["scraped_from"] = scrapedFrom
	}
	if scrapePath != "" {
		labels["scrape_path"] = scrapePath
	}
	samples := []PrometheusSample{
		{Name: "state_level", Labels: labels, Value: prometheusStatusValue(s.Status)},
		{Name: "state_time_in_state_seconds", Labels: labels, Value: float64(s.ChangedSecsAgo)},
	}
	for _, child := range s.Checks {
		samples = append(samples, stateSamplesWithProvenance(child, scrapedFrom, scrapePath)...)
	}
	return samples
}

func prometheusStatusValue(status Status) float64 {
	return float64(status) + 1
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, labelValueEscaper.Replace(labels[k])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
