package statekit

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// metricList is the per-collector metric list. Its YAML form is a map keyed by
// metric name, with the help text and Prometheus type rendered as a head
// comment ("<type> - <help>"). JSON encoding is unchanged.
type metricList []metricSnapshot

// collectorSnapshots is a slice of collector snapshots. Its YAML form
// merges all collectors' metrics into one map. JSON encoding is unchanged.
type collectorSnapshots []collectorSnapshot

func (this metricList) MarshalYAML() (interface{}, error) {
	node := &yaml.Node{Kind: yaml.MappingNode}
	for _, metric := range this {
		key := &yaml.Node{Kind: yaml.ScalarNode, Value: metric.Name}
		if comment := metricYAMLComment(metric); comment != "" {
			key.HeadComment = comment
		}
		value, err := encodeMetricYAMLValue(metric)
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, key, value)
	}
	return node, nil
}

func (this *metricList) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("statekit: metrics: expected mapping, got %s", yamlKindName(node.Kind))
	}
	out := make(metricList, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		snap, err := decodeMetricYAMLEntry(node.Content[i], node.Content[i+1])
		if err != nil {
			return err
		}
		out = append(out, snap)
	}
	*this = out
	return nil
}

func (this collectorSnapshot) MarshalYAML() (interface{}, error) {
	return this.Metrics.MarshalYAML()
}

func (this *collectorSnapshot) UnmarshalYAML(node *yaml.Node) error {
	return this.Metrics.UnmarshalYAML(node)
}

func (this collectorSnapshots) MarshalYAML() (interface{}, error) {
	merged := metricList{}
	for _, collector := range this {
		merged = append(merged, collector.Metrics...)
	}
	return merged.MarshalYAML()
}

func (this *collectorSnapshots) UnmarshalYAML(node *yaml.Node) error {
	var merged metricList
	if err := merged.UnmarshalYAML(node); err != nil {
		return err
	}
	if len(merged) == 0 {
		*this = nil
		return nil
	}
	*this = collectorSnapshots{{Metrics: merged}}
	return nil
}

func metricYAMLComment(metric metricSnapshot) string {
	help := strings.TrimSpace(metric.Help)
	typ := strings.TrimSpace(string(metric.Type))
	switch {
	case typ != "" && help != "":
		return typ + " - " + help
	case typ != "":
		return typ
	case help != "":
		return help
	default:
		return ""
	}
}

func encodeMetricYAMLValue(metric metricSnapshot) (*yaml.Node, error) {
	if metric.Type == PrometheusHistogram {
		return encodeHistogramYAMLValue(metric), nil
	}
	if len(metric.Samples) == 0 {
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "~"}, nil
	}
	if len(metric.Samples) == 1 && len(metric.Samples[0].Labels) == 0 {
		return floatScalarNode(metric.Samples[0].Value), nil
	}
	node := &yaml.Node{Kind: yaml.MappingNode}
	for _, sample := range metric.Samples {
		key := &yaml.Node{Kind: yaml.ScalarNode, Value: encodeLabelKey(sample.Labels)}
		node.Content = append(node.Content, key, floatScalarNode(sample.Value))
	}
	return node, nil
}

func encodeHistogramYAMLValue(metric metricSnapshot) *yaml.Node {
	buckets := &yaml.Node{Kind: yaml.MappingNode}
	var count, sum float64
	var hasCount, hasSum bool
	for _, sample := range metric.Samples {
		if le, ok := sample.Labels["le"]; ok {
			buckets.Content = append(buckets.Content,
				&yaml.Node{Kind: yaml.ScalarNode, Value: le},
				floatScalarNode(sample.Value),
			)
			continue
		}
		if len(sample.Labels) == 0 {
			if !hasCount {
				count = sample.Value
				hasCount = true
			} else if !hasSum {
				sum = sample.Value
				hasSum = true
			}
		}
	}
	node := &yaml.Node{Kind: yaml.MappingNode}
	if hasCount {
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "count"},
			floatScalarNode(count),
		)
	}
	if hasSum {
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "sum"},
			floatScalarNode(sum),
		)
	}
	if len(buckets.Content) > 0 {
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "buckets"},
			buckets,
		)
	}
	return node
}

func decodeMetricYAMLEntry(key, value *yaml.Node) (metricSnapshot, error) {
	snap := metricSnapshot{Name: key.Value}
	if comment := strings.TrimSpace(key.HeadComment); comment != "" {
		comment = strings.TrimPrefix(comment, "#")
		comment = strings.TrimSpace(comment)
		if dash := strings.Index(comment, " - "); dash >= 0 {
			snap.Type = PrometheusType(strings.TrimSpace(comment[:dash]))
			snap.Help = strings.TrimSpace(comment[dash+3:])
		} else if isKnownPrometheusType(comment) {
			snap.Type = PrometheusType(comment)
		} else {
			snap.Help = comment
		}
	}
	switch value.Kind {
	case yaml.ScalarNode:
		if value.Tag == "!!null" || value.Value == "" || value.Value == "~" {
			return snap, nil
		}
		v, err := strconv.ParseFloat(value.Value, 64)
		if err != nil {
			return snap, fmt.Errorf("statekit: metric %q: %w", snap.Name, err)
		}
		snap.Samples = []metricSample{{Value: v}}
	case yaml.MappingNode:
		if isHistogramYAMLNode(value) {
			snap.Type = PrometheusHistogram
			if err := decodeHistogramYAMLValue(&snap, value); err != nil {
				return snap, err
			}
		} else {
			for i := 0; i+1 < len(value.Content); i += 2 {
				labelKey := value.Content[i].Value
				v, err := strconv.ParseFloat(value.Content[i+1].Value, 64)
				if err != nil {
					return snap, fmt.Errorf("statekit: metric %q sample %q: %w", snap.Name, labelKey, err)
				}
				snap.Samples = append(snap.Samples, metricSample{
					Labels: decodeLabelKey(labelKey),
					Value:  v,
				})
			}
		}
	default:
		return snap, fmt.Errorf("statekit: metric %q: unexpected yaml kind %s", snap.Name, yamlKindName(value.Kind))
	}
	return snap, nil
}

func decodeHistogramYAMLValue(snap *metricSnapshot, node *yaml.Node) error {
	for i := 0; i+1 < len(node.Content); i += 2 {
		k := node.Content[i].Value
		sub := node.Content[i+1]
		switch k {
		case "count", "sum":
			v, err := strconv.ParseFloat(sub.Value, 64)
			if err != nil {
				return fmt.Errorf("statekit: metric %q %s: %w", snap.Name, k, err)
			}
			snap.Samples = append(snap.Samples, metricSample{Value: v})
		case "buckets":
			if sub.Kind != yaml.MappingNode {
				return fmt.Errorf("statekit: metric %q buckets: expected mapping", snap.Name)
			}
			for j := 0; j+1 < len(sub.Content); j += 2 {
				le := sub.Content[j].Value
				v, err := strconv.ParseFloat(sub.Content[j+1].Value, 64)
				if err != nil {
					return fmt.Errorf("statekit: metric %q bucket %q: %w", snap.Name, le, err)
				}
				snap.Samples = append(snap.Samples, metricSample{
					Labels: map[string]string{"le": le},
					Value:  v,
				})
			}
		}
	}
	return nil
}

func isHistogramYAMLNode(node *yaml.Node) bool {
	for i := 0; i+1 < len(node.Content); i += 2 {
		switch node.Content[i].Value {
		case "buckets", "count", "sum":
			return true
		}
	}
	return false
}

func encodeLabelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(labels))
	for _, name := range names {
		parts = append(parts, name+"="+labels[name])
	}
	return strings.Join(parts, ",")
}

func decodeLabelKey(s string) map[string]string {
	if s == "" {
		return nil
	}
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		out[strings.TrimSpace(kv[:eq])] = strings.TrimSpace(kv[eq+1:])
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func floatScalarNode(v float64) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: formatYAMLFloat(v)}
}

func formatYAMLFloat(v float64) string {
	switch {
	case math.IsNaN(v):
		return ".nan"
	case math.IsInf(v, 1):
		return ".inf"
	case math.IsInf(v, -1):
		return "-.inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func isKnownPrometheusType(s string) bool {
	switch PrometheusType(s) {
	case PrometheusCounter, PrometheusGauge, PrometheusHistogram:
		return true
	}
	return false
}

func yamlKindName(k yaml.Kind) string {
	switch k {
	case yaml.DocumentNode:
		return "document"
	case yaml.SequenceNode:
		return "sequence"
	case yaml.MappingNode:
		return "mapping"
	case yaml.ScalarNode:
		return "scalar"
	case yaml.AliasNode:
		return "alias"
	}
	return "unknown"
}
