package storage

import "sort"

// buildTargetSummaries groups a document's flattened states into L1 target
// summaries with flat StateHeaders. It also stamps each entry's TargetKey so
// the L2 details written from the same entries agree with the L1 layer.
//
// Status counts, worst status, and affected states are computed from
// top-level states only (states whose parent is outside the target), matching
// how the UIs roll a target up; the flat header list still carries every
// state including checks.
func buildTargetSummaries(entries []flattenedState) []TargetSummary {
	keyByIdentity := make(map[string]string, len(entries))
	for i := range entries {
		key, _, _ := targetIdentity(entries[i].Detail)
		keyByIdentity[entries[i].Detail.Identity] = key
		entries[i].Detail.TargetKey = key
	}

	type targetBuilder struct {
		summary TargetSummary
		hashes  []map[string]any
	}

	builders := map[string]*targetBuilder{}
	for i := range entries {
		detail := entries[i].Detail
		key, name, scrapePath := targetIdentity(detail)
		if key == "" {
			continue
		}
		builder := builders[key]
		if builder == nil {
			builder = &targetBuilder{
				summary: TargetSummary{
					Key:          key,
					Name:         name,
					ScrapePath:   scrapePath,
					StatusCounts: map[string]int{},
					WorstStatus:  "pass",
				},
			}
			builders[key] = builder
		}
		for k, v := range detail.Labels {
			if builder.summary.Labels == nil {
				builder.summary.Labels = map[string]string{}
			}
			builder.summary.Labels[k] = v
		}
		if builder.summary.ObservedAt.IsZero() || detail.ObservedAt.After(builder.summary.ObservedAt) {
			builder.summary.ObservedAt = detail.ObservedAt
		}
		builder.summary.States = append(builder.summary.States, detail.StateHeader)
		builder.hashes = append(builder.hashes, stateMaterialFields(detail))

		if keyByIdentity[detail.ParentIdentity] == key {
			continue // a check: rolls up through its parent state
		}
		builder.summary.StatusCounts[detail.Status]++
		if statusRank(detail.Status) > statusRank(builder.summary.WorstStatus) {
			builder.summary.WorstStatus = detail.Status
		}
		if detail.Status != "pass" {
			builder.summary.AffectedStates = append(builder.summary.AffectedStates, AffectedState{
				Name:      detail.Name,
				Status:    detail.Status,
				Reason:    detail.Reason,
				ChangedAt: detail.ChangedAt,
			})
		}
	}

	out := make([]TargetSummary, 0, len(builders))
	for _, builder := range builders {
		order := headerSortOrder(builder.summary.States)
		builder.summary.States = reorderHeaders(builder.summary.States, order)
		builder.hashes = reorderHashes(builder.hashes, order)
		sort.Slice(builder.summary.AffectedStates, func(i, j int) bool {
			affected := builder.summary.AffectedStates
			if statusRank(affected[i].Status) != statusRank(affected[j].Status) {
				return statusRank(affected[i].Status) > statusRank(affected[j].Status)
			}
			return affected[i].Name < affected[j].Name
		})
		builder.summary.MaterialHash = targetMaterialHash(builder.summary, builder.hashes)
		out = append(out, builder.summary)
	}
	sort.Slice(out, func(i, j int) bool {
		if statusRank(out[i].WorstStatus) != statusRank(out[j].WorstStatus) {
			return statusRank(out[i].WorstStatus) > statusRank(out[j].WorstStatus)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func headerSortOrder(headers []StateHeader) []int {
	order := make([]int, len(headers))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		left, right := headers[order[i]], headers[order[j]]
		if statusRank(left.Status) != statusRank(right.Status) {
			return statusRank(left.Status) > statusRank(right.Status)
		}
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		return left.Identity < right.Identity
	})
	return order
}

func reorderHeaders(headers []StateHeader, order []int) []StateHeader {
	out := make([]StateHeader, len(headers))
	for i, from := range order {
		out[i] = headers[from]
	}
	return out
}

func reorderHashes(hashes []map[string]any, order []int) []map[string]any {
	out := make([]map[string]any, len(hashes))
	for i, from := range order {
		out[i] = hashes[from]
	}
	return out
}

func targetIdentity(detail StateDetail) (key, name, scrapePath string) {
	name = detail.ScrapedFrom
	if name == "" && detail.Labels != nil {
		name = detail.Labels["target_id"]
	}
	if name == "" {
		return "", "", ""
	}
	scrapePath = firstNonEmpty(detail.ScrapePath, name)
	return name + "\n" + scrapePath, name, scrapePath
}

// stateMaterialFields is one state's contribution to the target material
// hash: everything material to the L2 detail, excluding volatile fields such
// as observed_at and updated_at so an unchanged target hashes identically
// across scrapes.
func stateMaterialFields(detail StateDetail) map[string]any {
	return map[string]any{
		"identity":        detail.Identity,
		"parent_identity": detail.ParentIdentity,
		"name":            detail.Name,
		"status":          detail.Status,
		"reason":          detail.Reason,
		"importance":      detail.Importance,
		"help":            detail.Help,
		"group_name":      detail.GroupName,
		"labels":          detail.Labels,
		"data_hash":       detail.DataHash,
		"changed_at":      detail.ChangedAt,
	}
}

func targetMaterialHash(summary TargetSummary, states []map[string]any) string {
	return hashJSON(map[string]any{
		"key":             summary.Key,
		"name":            summary.Name,
		"scrape_path":     summary.ScrapePath,
		"labels":          summary.Labels,
		"worst_status":    summary.WorstStatus,
		"status_counts":   summary.StatusCounts,
		"affected_states": summary.AffectedStates,
		"states":          states,
	})
}
