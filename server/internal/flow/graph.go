package flow

import (
	"fmt"
	"strings"
)

// GraphGenerator generates visual representations of flows.
type GraphGenerator struct{}

// NewGraphGenerator creates a new graph generator.
func NewGraphGenerator() *GraphGenerator {
	return &GraphGenerator{}
}

// DOT generates Graphviz DOT format.
func (g *GraphGenerator) DOT(ir *IR) string {
	var b strings.Builder

	b.WriteString("digraph flow {\n")
	b.WriteString("  rankdir=TB;\n")
	b.WriteString("  node [shape=box, style=filled, fillcolor=lightblue];\n\n")

	// Services as boxes
	b.WriteString("  // Services\n")
	for _, svc := range ir.Services {
		b.WriteString(fmt.Sprintf("  svc_%s [label=\"%s\\n(%s)\", shape=ellipse, fillcolor=lightyellow];\n", svc.Name, svc.Name, svc.Type))
	}
	b.WriteString("\n")

	// Nodes
	b.WriteString("  // Workflow nodes\n")
	for _, node := range ir.Nodes {
		style := nodeStyle(node.Type)
		label := nodeLabel(node)
		b.WriteString(fmt.Sprintf("  %s [label=\"%s\"%s];\n", node.ID, label, style))
	}
	b.WriteString("\n")

	// Edges
	b.WriteString("  // Edges\n")
	for _, edge := range ir.Edges {
		if edge.Condition != "" {
			b.WriteString(fmt.Sprintf("  %s -> %s [label=\"%s\"];\n", edge.From, edge.To, edge.Condition))
		} else {
			b.WriteString(fmt.Sprintf("  %s -> %s;\n", edge.From, edge.To))
		}
	}

	b.WriteString("}\n")
	return b.String()
}

// Mermaid generates Mermaid flowchart format.
func (g *GraphGenerator) Mermaid(ir *IR) string {
	var b strings.Builder

	b.WriteString("flowchart TD\n")

	// Nodes
	for _, node := range ir.Nodes {
		label := nodeLabel(node)
		switch node.Type {
		case "start":
			b.WriteString(fmt.Sprintf("  %s([%s])\n", node.ID, label))
		case "end":
			b.WriteString(fmt.Sprintf("  %s([%s])\n", node.ID, label))
		case "if":
			b.WriteString(fmt.Sprintf("  %s{%s}\n", node.ID, label))
		case "parallel":
			b.WriteString(fmt.Sprintf("  %s[%s]\n", node.ID, label))
		case "join":
			b.WriteString(fmt.Sprintf("  %s[%s]\n", node.ID, label))
		default:
			b.WriteString(fmt.Sprintf("  %s[%s]\n", node.ID, label))
		}
	}

	b.WriteString("\n")

	// Edges
	for _, edge := range ir.Edges {
		if edge.Condition != "" {
			b.WriteString(fmt.Sprintf("  %s -->|%s| %s\n", edge.From, edge.Condition, edge.To))
		} else {
			b.WriteString(fmt.Sprintf("  %s --> %s\n", edge.From, edge.To))
		}
	}

	return b.String()
}

func nodeStyle(nodeType string) string {
	switch nodeType {
	case "start", "end":
		return ", shape=ellipse, fillcolor=lightgreen"
	case "if":
		return ", shape=diamond, fillcolor=lightyellow"
	case "parallel", "join":
		return ", shape=hexagon, fillcolor=lightpink"
	case "emit":
		return ", shape=note, fillcolor=lightcyan"
	default:
		return ""
	}
}

func nodeLabel(node IRNode) string {
	if node.Name != "" {
		return node.Name
	}
	return node.Type
}
