package terminal

import (
	"fmt"

	"github.com/gdamore/tcell"
	"github.com/johnshiver/plankton/scheduler"
	"github.com/johnshiver/plankton/task"
	"github.com/rivo/tview"
)

type node struct {
	text     string
	expand   bool
	selected func()
	children []*node
}

func createAllNodes(ts *scheduler.TaskScheduler) *node {
	var addNode func(tr task.TaskRunner) *node
	addNode = func(tr task.TaskRunner) *node {
		cTask := tr.GetTask()
		node_text := fmt.Sprintf("%s -> %s", cTask.Name, cTask.State)
		newNode := &node{text: node_text, expand: true, children: []*node{}}
		for _, child := range tr.GetTask().Children {
			cNode := addNode(child)
			newNode.children = append(newNode.children, cNode)
		}
		return newNode
	}
	return addNode(ts.RootRunner)

}

func newTreeView(doneFunc func()) *tview.Flex {
	ts := GetCurrentTaskScheduler()
	tree := tview.NewTreeView()
	rootNode := createAllNodes(ts)
	rootNode.selected = doneFunc
	rootNode.expand = false

	tree.SetBorder(true).
		SetTitle(ts.Name)

	tree.SetAlign(false).
		SetTopLevel(0).
		SetGraphics(true).
		SetPrefixes(nil)

	var add func(target *node) *tview.TreeNode
	add = func(target *node) *tview.TreeNode {
		node := tview.NewTreeNode(target.text).
			SetSelectable(target.expand || target.selected != nil).
			SetExpanded(target == rootNode).
			SetReference(target)
		if target.expand {
			node.SetColor(tcell.ColorGreen)
		} else if target.selected != nil {
			node.SetColor(tcell.ColorRed)
		}
		for _, child := range target.children {
			node.AddChild(add(child))
		}
		return node
	}

	root := add(rootNode)
	tree.SetRoot(root).
		SetCurrentNode(root).
		SetSelectedFunc(func(n *tview.TreeNode) {
			original := n.GetReference().(*node)
			if original.expand {
				n.SetExpanded(!n.IsExpanded())
			} else if original.selected != nil {
				original.selected()
			}
		})
	tree.GetRoot().ExpandAll()
	return tview.NewFlex().
		AddItem(tree, 0, 1, true)
}
