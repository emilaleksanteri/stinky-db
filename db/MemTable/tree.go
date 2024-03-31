package memtable

import (
	"strings"
)

type Color bool

const (
	red, black Color = true, false
)

type Node struct {
	Key    string
	Value  string
	Color  Color
	Left   *Node
	Right  *Node
	Parent *Node
}

type RBTree struct {
	Root *Node
	Size int
}

func NewRBTree() *RBTree {
	return &RBTree{}
}

func NewWithRoot(root *Node) *RBTree {
	return &RBTree{Root: root, Size: 1}
}

const (
	KEY_LESS_NODE    = -1
	KEY_GREATER_NODE = 1
	KEY_EQUAL_NODE   = 0
)

func (t *RBTree) Insert(key, value string) {
	if t.Root == nil {
		t.Root = &Node{Key: key, Value: value, Color: black}
		t.Size += 1
		return
	}

	node := t.Root
	var inserted *Node

	running := true
	for running {
		switch strings.Compare(key, node.Key) {
		case KEY_LESS_NODE:
			if node.Left == nil {
				node.Left = &Node{Key: key, Value: value, Color: red, Parent: node}
				t.Size += 1
				inserted = node.Left
				running = false
			} else {
				node = node.Left
			}
		case KEY_GREATER_NODE:
			if node.Right == nil {
				node.Right = &Node{Key: key, Value: value, Color: red, Parent: node}
				t.Size += 1
				inserted = node.Right
				running = false
			} else {
				node = node.Right
			}
		case KEY_EQUAL_NODE:
			node.Value = value
			inserted = node
			running = false
		}
	}

	t.checkRoate(inserted)
}

type Found bool

func (t *RBTree) Get(key string) (string, Found) {
	node := t.Root
	for node != nil {
		switch strings.Compare(key, node.Key) {
		case KEY_LESS_NODE:
			node = node.Left
		case KEY_GREATER_NODE:
			node = node.Right
		case KEY_EQUAL_NODE:
			return node.Value, true
		}
	}
	return "", false
}

func (t *RBTree) checkRoate(node *Node) {
	if node.Parent == nil {
		node.Color = black
		return
	}

	if node.Parent.Color == black {
		return
	}

	uncle := node.uncle()
	if uncle != nil && uncle.Color == red {
		node.Parent.Color = black
		uncle.Color = black
		node.Parent.Parent.Color = red
		t.checkRoate(node.Parent.Parent)
		return
	}

	grandparent := node.Parent.Parent
	if node == node.Parent.Right && node.Parent == grandparent.Left {
		t.rotateLeft(node.Parent)
		node = node.Left
	} else if node == node.Parent.Left && node.Parent == grandparent.Right {
		t.rotateRight(node.Parent)
		node = node.Right
	}

	node.Parent.Color = black
	grandparent = node.Parent.Parent
	grandparent.Color = red
	if node == node.Parent.Left && node.Parent == grandparent.Left {
		t.rotateRight(grandparent)
	} else if node == node.Parent.Right && node.Parent == grandparent.Right {
		t.rotateLeft(grandparent)
	}
}

func (t *RBTree) replaceNode(old *Node, new *Node) {
	if old.Parent == nil {
		t.Root = new
	} else {
		if old == old.Parent.Left {
			old.Parent.Left = new
		} else {
			old.Parent.Right = new
		}
	}

	if new != nil {
		new.Parent = old.Parent
	}
}

func (t *RBTree) rotateLeft(node *Node) {
	right := node.Right
	t.replaceNode(node, right)
	node.Right = right.Left
	if right.Left != nil {
		right.Left.Parent = node
	}
	right.Left = node
	node.Parent = right
}

func (t *RBTree) rotateRight(node *Node) {
	left := node.Left
	t.replaceNode(node, left)
	node.Left = left.Right
	if left.Right != nil {
		left.Right.Parent = node
	}
	left.Right = node
	node.Parent = left
}

func (node *Node) uncle() *Node {
	if node == nil || node.Parent == nil || node.Parent.Parent == nil {
		return nil
	}

	return node.Parent.sibling()
}

func (node *Node) sibling() *Node {
	if node == nil || node.Parent == nil {
		return nil
	}

	if node == node.Parent.Left {
		return node.Parent.Right
	}

	return node.Parent.Left
}

func (t *RBTree) Keys() []string {
	keys := t.iterateForKeys(t.Root, make([]string, 0, t.Size))
	return keys
}

func (t *RBTree) Values() []string {
	values := t.iterateForVals(t.Root, make([]string, 0, t.Size))
	return values
}

func (t *RBTree) iterateForKeys(start *Node, keys []string) []string {
	if start == nil {
		return keys
	}

	keys = t.iterateForKeys(start.Left, keys)
	keys = append(keys, start.Key)
	keys = t.iterateForKeys(start.Right, keys)

	return keys
}

func (t *RBTree) iterateForVals(start *Node, values []string) []string {
	if start == nil {
		return values
	}

	values = t.iterateForVals(start.Left, values)
	values = append(values, start.Value)
	values = t.iterateForVals(start.Right, values)

	return values
}
