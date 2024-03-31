package memtable

import (
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

