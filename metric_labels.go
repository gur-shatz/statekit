package statekit

import (
	"strconv"
	"strings"
)

func labelValuesKey(values []string) string {
	var b strings.Builder
	for _, value := range values {
		b.WriteString(strconv.Itoa(len(value)))
		b.WriteByte(':')
		b.WriteString(value)
		b.WriteByte(';')
	}
	return b.String()
}

func labelsMap(names, values []string) map[string]string {
	labels := make(map[string]string, len(names))
	for i, name := range names {
		labels[name] = values[i]
	}
	return labels
}
