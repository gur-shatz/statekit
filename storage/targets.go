package storage

import "sort"

func buildTargetDocuments(entries []flattenedState) []TargetDocument {
	byIdentity := make(map[string]flattenedState, len(entries))
	for _, entry := range entries {
		byIdentity[entry.Node.Identity] = entry
	}

	type targetBuilder struct {
		doc           TargetDocument
		states        map[string]*TargetState
		pendingChecks map[string][]TargetCheck
	}

	builders := map[string]*targetBuilder{}
	for _, entry := range entries {
		key, name, scrapePath := targetIdentity(entry)
		if key == "" {
			continue
		}
		builder := builders[key]
		if builder == nil {
			builder = &targetBuilder{
				doc: TargetDocument{
					Key:          key,
					Name:         name,
					ScrapePath:   scrapePath,
					StatusCounts: map[string]int{},
					WorstStatus:  "pass",
				},
				states:        map[string]*TargetState{},
				pendingChecks: map[string][]TargetCheck{},
			}
			builders[key] = builder
		}
		for k, v := range entry.Node.Labels {
			if builder.doc.Labels == nil {
				builder.doc.Labels = map[string]string{}
			}
			builder.doc.Labels[k] = v
		}
		if builder.doc.ObservedAt.IsZero() || entry.Current.ObservedAt.After(builder.doc.ObservedAt) {
			builder.doc.ObservedAt = entry.Current.ObservedAt
		}

		parent, hasParent := byIdentity[entry.Node.ParentIdentity]
		if hasParent {
			parentKey, _, _ := targetIdentity(parent)
			if parentKey == key {
				check := targetCheckFromEntry(entry)
				if state := builder.states[entry.Node.ParentIdentity]; state != nil {
					state.Checks = append(state.Checks, check)
				} else {
					builder.pendingChecks[entry.Node.ParentIdentity] = append(builder.pendingChecks[entry.Node.ParentIdentity], check)
				}
				continue
			}
		}

		state := targetStateFromEntry(entry)
		if checks := builder.pendingChecks[entry.Node.Identity]; len(checks) > 0 {
			state.Checks = append(state.Checks, checks...)
			delete(builder.pendingChecks, entry.Node.Identity)
		}
		builder.states[entry.Node.Identity] = &state
		builder.doc.StatusCounts[state.Status]++
		if statusRank(state.Status) > statusRank(builder.doc.WorstStatus) {
			builder.doc.WorstStatus = state.Status
		}
		if state.Status != "pass" {
			builder.doc.AffectedStates = append(builder.doc.AffectedStates, AffectedState{
				Name:      state.Name,
				Status:    state.Status,
				Reason:    state.Reason,
				ChangedAt: state.ChangedAt,
			})
		}
	}

	out := make([]TargetDocument, 0, len(builders))
	for _, builder := range builders {
		for _, state := range builder.states {
			sort.Slice(state.Checks, func(i, j int) bool {
				return compareTargetChecks(state.Checks[i], state.Checks[j])
			})
			builder.doc.States = append(builder.doc.States, *state)
		}
		sort.Slice(builder.doc.States, func(i, j int) bool {
			return compareTargetStates(builder.doc.States[i], builder.doc.States[j])
		})
		sort.Slice(builder.doc.AffectedStates, func(i, j int) bool {
			if statusRank(builder.doc.AffectedStates[i].Status) != statusRank(builder.doc.AffectedStates[j].Status) {
				return statusRank(builder.doc.AffectedStates[i].Status) > statusRank(builder.doc.AffectedStates[j].Status)
			}
			return builder.doc.AffectedStates[i].Name < builder.doc.AffectedStates[j].Name
		})
		builder.doc.MaterialHash = targetMaterialHash(builder.doc)
		out = append(out, builder.doc)
	}
	sort.Slice(out, func(i, j int) bool {
		if statusRank(out[i].WorstStatus) != statusRank(out[j].WorstStatus) {
			return statusRank(out[i].WorstStatus) > statusRank(out[j].WorstStatus)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func targetIdentity(entry flattenedState) (key, name, scrapePath string) {
	name = entry.Node.ScrapedFrom
	if name == "" && entry.Node.Labels != nil {
		name = entry.Node.Labels["target_id"]
	}
	if name == "" {
		return "", "", ""
	}
	scrapePath = firstNonEmpty(entry.Node.ScrapePath, name)
	return name + "\n" + scrapePath, name, scrapePath
}

func targetStateFromEntry(entry flattenedState) TargetState {
	return TargetState{
		Identity:   entry.Node.Identity,
		Name:       entry.Node.Name,
		Status:     entry.Current.Status,
		Reason:     entry.Current.Reason,
		Importance: entry.Node.Importance,
		Help:       entry.Node.Help,
		GroupName:  entry.Node.GroupName,
		Labels:     cloneLabels(entry.Node.Labels),
		ChangedAt:  entry.Current.ChangedAt,
		UpdatedAt:  entry.Current.UpdatedAt,
		ObservedAt: entry.Current.ObservedAt,
	}
}

func targetCheckFromEntry(entry flattenedState) TargetCheck {
	return TargetCheck{
		Identity:   entry.Node.Identity,
		Name:       entry.Node.Name,
		Status:     entry.Current.Status,
		Reason:     entry.Current.Reason,
		Importance: entry.Node.Importance,
		Help:       entry.Node.Help,
		Labels:     cloneLabels(entry.Node.Labels),
		ChangedAt:  entry.Current.ChangedAt,
		UpdatedAt:  entry.Current.UpdatedAt,
		ObservedAt: entry.Current.ObservedAt,
	}
}

func compareTargetStates(a, b TargetState) bool {
	if statusRank(a.Status) != statusRank(b.Status) {
		return statusRank(a.Status) > statusRank(b.Status)
	}
	return a.Name < b.Name
}

func compareTargetChecks(a, b TargetCheck) bool {
	if statusRank(a.Status) != statusRank(b.Status) {
		return statusRank(a.Status) > statusRank(b.Status)
	}
	return a.Name < b.Name
}

func targetMaterialHash(target TargetDocument) string {
	states := make([]map[string]any, 0, len(target.States))
	for _, state := range target.States {
		checks := make([]map[string]any, 0, len(state.Checks))
		for _, check := range state.Checks {
			checks = append(checks, map[string]any{
				"identity":   check.Identity,
				"name":       check.Name,
				"status":     check.Status,
				"reason":     check.Reason,
				"importance": check.Importance,
				"help":       check.Help,
				"labels":     check.Labels,
				"changed_at": check.ChangedAt,
			})
		}
		states = append(states, map[string]any{
			"identity":   state.Identity,
			"name":       state.Name,
			"status":     state.Status,
			"reason":     state.Reason,
			"importance": state.Importance,
			"help":       state.Help,
			"group_name": state.GroupName,
			"labels":     state.Labels,
			"changed_at": state.ChangedAt,
			"checks":     checks,
		})
	}
	return hashJSON(map[string]any{
		"key":             target.Key,
		"name":            target.Name,
		"scrape_path":     target.ScrapePath,
		"labels":          target.Labels,
		"worst_status":    target.WorstStatus,
		"status_counts":   target.StatusCounts,
		"affected_states": target.AffectedStates,
		"states":          states,
	})
}
