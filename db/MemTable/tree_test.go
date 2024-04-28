package memtable

import (
	"errors"
	"slices"
	"testing"
)

func TestInsertLeft(t *testing.T) {
	tree := NewRBTree(0)
	tree.Insert("key2", "value")
	tree.Insert("key", "value2")

	if tree.Root.Left == nil {
		t.Errorf("Expected left node to be set, got nil")
	}
}

func TestInsertRight(t *testing.T) {
	tree := NewRBTree(0)
	tree.Insert("key", "value")
	tree.Insert("key2", "value2")

	if tree.Root.Right == nil {
		t.Errorf("Expected right node to be set, got nil")
	}
}

func TestInsertWithRotation(t *testing.T) {
	tree := NewRBTree(0)
	tree.Insert("key", "value")
	tree.Insert("key2", "value2")
	tree.Insert("key3", "value3")
	wantedSize := int64(len("key") + len("value") + len("key2") + len("value2") + len("key3") + len("value3"))

	if tree.Root.Key != "key2" {
		t.Errorf("Expected root key 'key2', got %v", tree.Root.Key)
	}

	if tree.Root.Left.Key != "key" {
		t.Errorf("Expected left key 'key', got %v", tree.Root.Left.Key)
	}

	if tree.Root.Right.Key != "key3" {
		t.Errorf("Expected right key 'key3', got %v", tree.Root.Right.Key)
	}

	if tree.Root.Color != black {
		t.Errorf("Expected root color black, got %v", tree.Root.Color)
	}

	if tree.Root.Left.Color != red {
		t.Errorf("Expected left color red, got %v", tree.Root.Left.Color)
	}

	if tree.Root.Right.Color != red {
		t.Errorf("Expected right color red, got %v", tree.Root.Right.Color)
	}

	if tree.Size != wantedSize {
		t.Errorf("Expected size %d, got %d", wantedSize, tree.Size)
	}

	if tree.Root.Parent != nil {
		t.Errorf("Expected root parent to be nil, got %v", tree.Root.Parent)
	}

	if tree.Root.Left.Parent != tree.Root {
		t.Errorf("Expected left parent to be root, got %v", tree.Root.Left.Parent)
	}

	if tree.Root.Right.Parent != tree.Root {
		t.Errorf("Expected right parent to be root, got %v", tree.Root.Right.Parent)
	}
}

func TestInsert(t *testing.T) {
	tree := NewRBTree(0)
	tree.Insert("5", "e")
	tree.Insert("6", "f")
	tree.Insert("7", "g")
	tree.Insert("3", "c")
	tree.Insert("4", "d")
	tree.Insert("1", "x")
	tree.Insert("2", "b")
	tree.Insert("1", "a") //overwrite

	keys := tree.Keys()
	expected := []string{"1", "2", "3", "4", "5", "6", "7"}
	if !slices.Equal(keys, expected) {
		t.Errorf("expected %v and got %v", expected, keys)
	}

	values := tree.Values()
	expected2 := []string{"a", "b", "c", "d", "e", "f", "g"}
	if !slices.Equal(values, expected2) {
		t.Errorf("expected %v and got %v", expected2, values)
	}

	toGet := []struct {
		Key   string
		Val   string
		Found Found
	}{
		{"1", "a", true},
		{"2", "b", true},
		{"3", "c", true},
		{"4", "d", true},
		{"5", "e", true},
		{"6", "f", true},
		{"7", "g", true},
		{"8", "", false},
	}

	for _, tg := range toGet {
		val, found := tree.Get(tg.Key)
		if val != tg.Val {
			t.Errorf("expected %v and got %v", tg.Val, val)
		}
		if found != tg.Found {
			t.Errorf("expected %v and got %v", tg.Found, found)
		}
	}

}

func TestGetFromTree(t *testing.T) {
	tree := NewRBTree(0)
	tree.Insert("key", "value")
	tree.Insert("key2", "value2")
	tree.Insert("key3", "value")

	value, found := tree.Get("key")
	if !found {
		t.Errorf("Expected to find key, but did not")
	}

	if value != "value" {
		t.Errorf("Expected value 'value', got %v", value)
	}
}

func TestGetMaxCapacityErrorOnMaxCapacity(t *testing.T) {
	tree := NewRBTree(10)
	tree.Insert("key", "value")
	err := tree.Insert("key2", "value2")
	if !errors.Is(err, AtMaxCapErr) {
		t.Errorf("expected max capacity error, got %+v", err)
	}
}
