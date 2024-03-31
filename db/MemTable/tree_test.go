package memtable

import (
	"slices"
	"testing"
)

func TestNewRBTree(t *testing.T) {
	tree := NewRBTree()
	if tree.Root != nil {
		t.Errorf("Expected nil root, got %v", tree.Root)
	}
	if tree.Size != 0 {
		t.Errorf("Expected size 0, got %v", tree.Size)
	}
}

func TestNewWithRoot(t *testing.T) {
	root := &Node{Key: "root", Value: "root", Color: black}
	tree := NewWithRoot(root)
	if tree.Root != root {
		t.Errorf("Expected root %v, got %v", root, tree.Root)
	}
	if tree.Size != 1 {
		t.Errorf("Expected size 1, got %v", tree.Size)
	}
}

func TestInsertNewNode(t *testing.T) {
	tree := NewRBTree()
	tree.Insert("key", "value")
	if tree.Root == nil {
		t.Errorf("Expected root to be set, got nil")
	}
	if tree.Size != 1 {
		t.Errorf("Expected size 1, got %v", tree.Size)
	}
}

func TestInsertWithMany(t *testing.T) {
	tree := NewRBTree()
	tree.Insert("key", "value")
	tree.Insert("key2", "value2")
	tree.Insert("key3", "value3")
	if tree.Size != 3 {
		t.Errorf("Expected size 3, got %v", tree.Size)
	}
}

func TestInsertWithSameKey(t *testing.T) {
	tree := NewRBTree()
	tree.Insert("key", "value")
	tree.Insert("key", "value")

	if tree.Size != 1 {
		t.Errorf("Expected size 1, got %v", tree.Size)
	}
}

func TestInsertLeft(t *testing.T) {
	tree := NewRBTree()
	tree.Insert("key2", "value")
	tree.Insert("key", "value2")

	if tree.Root.Left == nil {
		t.Errorf("Expected left node to be set, got nil")
	}
}

func TestInsertRight(t *testing.T) {
	tree := NewRBTree()
	tree.Insert("key", "value")
	tree.Insert("key2", "value2")

	if tree.Root.Right == nil {
		t.Errorf("Expected right node to be set, got nil")
	}
}

func TestInsertWithRotation(t *testing.T) {
	tree := NewRBTree()
	tree.Insert("key", "value")
	tree.Insert("key2", "value2")
	tree.Insert("key3", "value3")

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

	if tree.Size != 3 {
		t.Errorf("Expected size 3, got %v", tree.Size)
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

func TestIter(t *testing.T) {
	tree := NewRBTree()
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
}

/*
func TestKeepCorrectOrderWithManyInserts(t *testing.T) {
	tree := NewWithRoot(&Node{Key: "key4", Value: "value4", Color: black})

	tree.Insert("key", "value")
	tree.Insert("key2", "value2")
	tree.Insert("key3", "value3")
	tree.Insert("key5", "value5")
	tree.Insert("key6", "value6")
	tree.Insert("key7", "value7")
	tree.Insert("key8", "value8")
	tree.Insert("key9", "value9")
	tree.Insert("key10", "value10")

	if tree.Root.Key != "key4" {
		t.Errorf("Expected root key %v, got %v", "key4", tree.Root.Key)
	}

	if tree.Root.Left.Key != "key2" {
		t.Errorf("Expected left key %v, got %v", "key", tree.Root.Left.Key)
	}

	if tree.Root.Left.Left.Key != "key" {
		t.Errorf("Expected left left key %v, got %v", "key", tree.Root.Left.Left.Key)
	}

	if tree.Root.Left.Right.Key != "key3" {
		t.Errorf("Expected left right key %v, got %v", "key3", tree.Root.Left.Right.Key)
	}

	if tree.Root.Right.Key != "key6" {
		t.Errorf("Expected right key %v, got %v", "key7", tree.Root.Right.Key)
	}

	if tree.Root.Right.Left.Key != "key5" {
		t.Errorf("Expected right left key %v, got %v", "key5", tree.Root.Right.Left.Key)
	}

	if tree.Root.Right.Right.Key != "key7" {
		t.Errorf("Expected right right key %v, got %v", "key7", tree.Root.Right.Right.Key)
	}

	if tree.Size != 10 {
		t.Errorf("Expected size 10, got %v", tree.Size)
	}
}
*/

func TestGetFromTree(t *testing.T) {
	tree := NewRBTree()
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
