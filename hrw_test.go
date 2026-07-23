package lease

import (
	"testing"
)

func TestHRWAssignDeterministic(t *testing.T) {
	members := []string{"a", "b", "c", "d", "e"}

	// Same input should always produce same output
	for i := 0; i < 10; i++ {
		first := PickOwner("task-1", members)
		second := PickOwner("task-1", members)
		if first != second {
			t.Errorf("non-deterministic: %s vs %s", first, second)
		}
	}
}

func TestHRWAssignDifferentTasks(t *testing.T) {
	members := []string{"a", "b", "c", "d", "e"}
	// Different tasks may land on different members
	counts := make(map[string]int)
	for i := 0; i < 100; i++ {
		taskID := string(rune('a' + i%26)) + "-task"
		owner := PickOwner(taskID, members)
		counts[owner]++
	}
	// With 100 tasks and 5 members, each should get some tasks
	if len(counts) < 2 {
		t.Errorf("expected at least 2 different owners, got %d: %v", len(counts), counts)
	}
}

func TestHRWMinimalDisruption(t *testing.T) {
	members := []string{"a", "b", "c", "d", "e"}
	withoutC := []string{"a", "b", "d", "e"}

	changed := 0
	total := 100
	for i := 0; i < total; i++ {
		taskID := string(rune('a' + i%26)) + "-task-" + string(rune('0'+i/26))
		orig := PickOwner(taskID, members)
		after := PickOwner(taskID, withoutC)
		if orig != after {
			changed++
		}
	}
	// When removing 1 out of 5 members, roughly 1/5 of tasks should migrate
	expected := total / 5
	tolerance := total / 4 // be generous
	if changed < expected-tolerance || changed > expected+tolerance {
		t.Logf("changed %d out of %d (expected ~%d)", changed, total, expected)
	}
}

func TestHRWAssignN(t *testing.T) {
	members := []string{"a", "b", "c", "d", "e"}
	top3 := PickOwnerN("task-xyz", members, 3)
	if len(top3) != 3 {
		t.Errorf("top3 length = %d, want 3", len(top3))
	}
	// Top 1 should match HRWAssign
	first := PickOwner("task-xyz", members)
	if top3[0] != first {
		t.Errorf("top3[0] = %q, want %q", top3[0], first)
	}
	// No duplicates
	seen := make(map[string]bool)
	for _, m := range top3 {
		if seen[m] {
			t.Errorf("duplicate member %q in top3", m)
		}
		seen[m] = true
	}
}

func TestSortMembers(t *testing.T) {
	in := []string{"c", "a", "b"}
	out := CanonicalMembers(in)
	if len(out) != 3 || out[0] != "a" || out[1] != "b" || out[2] != "c" {
		t.Errorf("sorted = %v, want [a b c]", out)
	}
	// Original not modified
	if in[0] != "c" {
		t.Error("SortMembers modified input slice")
	}
}
