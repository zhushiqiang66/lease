package lease

import (
	"hash/fnv"
	"sort"
)

// HRW (Highest Random Weight / Rendezvous Hashing) assigns tasks to instances.
// For a given taskID, the instance with the highest hash(taskID, instanceID) wins.
//
// Properties: deterministic, minimal disruption on membership change,
// statistically even distribution when task count is large enough.

// HRWAssign returns the owner instanceID for taskID among members.
// members must be non-empty.
func HRWAssign(taskID string, members []string) string {
	var (
		best    string
		bestSum uint64
	)
	for _, m := range members {
		h := hashPair(taskID, m)
		if best == "" || h > bestSum {
			bestSum = h
			best = m
		}
	}
	return best
}

// HRWAssignN returns the top-N owner instanceIDs for taskID (sorted highest first).
// members must be non-empty. n is capped at len(members).
func HRWAssignN(taskID string, members []string, n int) []string {
	if n > len(members) {
		n = len(members)
	}
	type scored struct {
		id    string
		score uint64
	}
	items := make([]scored, len(members))
	for i, m := range members {
		items[i] = scored{id: m, score: hashPair(taskID, m)}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].score != items[j].score {
			return items[i].score > items[j].score
		}
		return items[i].id < items[j].id
	})
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = items[i].id
	}
	return out
}

// SortMembers returns a deterministically sorted copy of members.
// Always call this before passing member lists to HRW functions
// to ensure stable output across instances.
func SortMembers(members []string) []string {
	out := make([]string, len(members))
	copy(out, members)
	sort.Strings(out)
	return out
}

func hashPair(a, b string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(a))
	h.Write([]byte{0})
	h.Write([]byte(b))
	return h.Sum64()
}
