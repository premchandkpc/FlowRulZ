package flow

import (
	"fmt"
	"regexp"
	"strings"
)

var validIdentRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// CodeGenTarget is the target language for code generation.
type CodeGenTarget string

const (
	TargetGo     CodeGenTarget = "go"
	TargetRust   CodeGenTarget = "rust"
	TargetJava   CodeGenTarget = "java"
	TargetPython CodeGenTarget = "python"
)

// CodeGenerator generates code from IR.
type CodeGenerator struct {
	target CodeGenTarget
}

// NewCodeGenerator creates a new code generator.
func NewCodeGenerator(target CodeGenTarget) *CodeGenerator {
	return &CodeGenerator{target: target}
}

// Generate generates code from IR.
func (g *CodeGenerator) Generate(ir *IR) (string, error) {
	if !validIdentRegex.MatchString(ir.Name) {
		return "", fmt.Errorf("codegen: invalid flow name %q (must be valid identifier)", ir.Name)
	}
	for _, v := range ir.Variables {
		if !validIdentRegex.MatchString(v.Name) {
			return "", fmt.Errorf("codegen: invalid variable name %q (must be valid identifier)", v.Name)
		}
	}
	for _, svc := range ir.Services {
		if !validIdentRegex.MatchString(svc.Name) {
			return "", fmt.Errorf("codegen: invalid service name %q (must be valid identifier)", svc.Name)
		}
	}

	switch g.target {
	case TargetGo:
		return g.generateGo(ir)
	case TargetRust:
		return g.generateRust(ir)
	case TargetJava:
		return g.generateJava(ir)
	case TargetPython:
		return g.generatePython(ir)
	default:
		return "", fmt.Errorf("unsupported target: %s", g.target)
	}
}

func (g *CodeGenerator) generateGo(ir *IR) (string, error) {
	var b strings.Builder

	b.WriteString("package main\n\n")
	b.WriteString("import (\n")
	b.WriteString("\t\"context\"\n")
	b.WriteString(")\n\n")

	// Generate struct
	b.WriteString(fmt.Sprintf("// %s defines the flow.\n", ir.Name))
	b.WriteString(fmt.Sprintf("type %s struct {\n", ir.Name))
	for _, v := range ir.Variables {
		b.WriteString(fmt.Sprintf("\t%s %s\n", strings.Title(v.Name), goType(v.Type)))
	}
	b.WriteString("}\n\n")

	// Generate service interfaces
	for _, svc := range ir.Services {
		b.WriteString(fmt.Sprintf("// %sService is the %s service interface.\n", strings.Title(svc.Name), svc.Name))
		b.WriteString(fmt.Sprintf("type %sService interface {\n", strings.Title(svc.Name)))
		b.WriteString(fmt.Sprintf("\t// Call executes a method on the service.\n"))
		b.WriteString(fmt.Sprintf("\tCall(ctx context.Context, method string, req interface{}) (interface{}, error)\n"))
		b.WriteString("}\n\n")
	}

	// Generate Execute function
	b.WriteString(fmt.Sprintf("// Execute runs the %s flow.\n", ir.Name))
	b.WriteString(fmt.Sprintf("func Execute(ctx context.Context, input *%s) (*%sOutput, error) {\n", ir.Name, ir.Name))
	b.WriteString("\tvar err error\n")
	b.WriteString("\t_ = err\n\n")

	// Generate workflow steps
	for _, node := range ir.Nodes {
		if node.Type == "start" || node.Type == "end" || node.Type == "merge" {
			continue
		}
		if node.Target != "" {
			parts := strings.SplitN(node.Target, ".", 2)
			if len(parts) == 2 {
				b.WriteString(fmt.Sprintf("\t// Step: %s\n", node.Name))
				b.WriteString(fmt.Sprintf("\t// Call %s.%s\n", parts[0], parts[1]))
			}
		}
		if node.Condition != "" {
			b.WriteString(fmt.Sprintf("\tif %s {\n", node.Condition))
			b.WriteString(fmt.Sprintf("\t\t// then branch\n"))
			b.WriteString("\t}\n")
		}
	}

	b.WriteString(fmt.Sprintf("\n\treturn &%sOutput{}, nil\n", ir.Name))
	b.WriteString("}\n")

	return b.String(), nil
}

func (g *CodeGenerator) generateRust(ir *IR) (string, error) {
	var b strings.Builder

	b.WriteString("use std::future::Future;\n\n")

	// Generate struct
	b.WriteString(fmt.Sprintf("/// %s defines the flow.\n", ir.Name))
	b.WriteString(fmt.Sprintf("#[derive(Debug, Clone)]\npub struct %s {\n", ir.Name))
	for _, v := range ir.Variables {
		b.WriteString(fmt.Sprintf("\tpub %s: %s,\n", v.Name, rustType(v.Type)))
	}
	b.WriteString("}\n\n")

	// Generate service traits
	for _, svc := range ir.Services {
		b.WriteString(fmt.Sprintf("/// %sService is the %s service trait.\n", strings.Title(svc.Name), svc.Name))
		b.WriteString(fmt.Sprintf("pub trait %sService {\n", strings.Title(svc.Name)))
		b.WriteString(fmt.Sprintf("\tfn call(&self, method: &str, req: serde_json::Value) -> impl Future<Output = Result<serde_json::Value, Box<dyn std::error::Error>>>;\n"))
		b.WriteString("}\n\n")
	}

	// Generate execute function
	b.WriteString(fmt.Sprintf("/// Execute runs the %s flow.\n", ir.Name))
	b.WriteString(fmt.Sprintf("pub async fn execute(input: %s) -> Result<(), Box<dyn std::error::Error>> {\n", ir.Name))
	for _, node := range ir.Nodes {
		if node.Type == "start" || node.Type == "end" || node.Type == "merge" {
			continue
		}
		if node.Target != "" {
			b.WriteString(fmt.Sprintf("\t// Step: %s\n", node.Name))
		}
	}
	b.WriteString("\tOk(())\n")
	b.WriteString("}\n")

	return b.String(), nil
}

func (g *CodeGenerator) generateJava(ir *IR) (string, error) {
	var b strings.Builder

	b.WriteString("import java.util.concurrent.CompletableFuture;\n\n")

	// Generate class
	b.WriteString(fmt.Sprintf("/** %s defines the flow. */\n", ir.Name))
	b.WriteString(fmt.Sprintf("public class %s {\n", ir.Name))

	// Generate fields
	for _, v := range ir.Variables {
		b.WriteString(fmt.Sprintf("\tprivate %s %s;\n", javaType(v.Type), v.Name))
	}

	b.WriteString("\n")

	// Generate constructor
	b.WriteString(fmt.Sprintf("\tpublic %s() {\n", ir.Name))
	b.WriteString("\t}\n\n")

	// Generate service interfaces
	for _, svc := range ir.Services {
		b.WriteString(fmt.Sprintf("\t/** %sService is the %s service interface. */\n", strings.Title(svc.Name), svc.Name))
		b.WriteString(fmt.Sprintf("\tpublic interface %sService {\n", strings.Title(svc.Name)))
		b.WriteString(fmt.Sprintf("\t\tCompletableFuture<Object> call(String method, Object req);\n"))
		b.WriteString("\t}\n\n")
	}

	b.WriteString("}\n")

	return b.String(), nil
}

func (g *CodeGenerator) generatePython(ir *IR) (string, error) {
	var b strings.Builder

	b.WriteString("from typing import Any, Dict\n")
	b.WriteString("import asyncio\n\n")

	// Generate class
	b.WriteString(fmt.Sprintf("class %s:\n", ir.Name))
	b.WriteString(fmt.Sprintf("\t\"\"\"%s defines the flow.\"\"\"\n\n", ir.Name))

	// Generate __init__
	b.WriteString("\tdef __init__(self")
	for _, v := range ir.Variables {
		b.WriteString(fmt.Sprintf(", %s: %s", v.Name, pythonType(v.Type)))
	}
	b.WriteString("):\n")
	for _, v := range ir.Variables {
		b.WriteString(fmt.Sprintf("\t\tself.%s = %s\n", v.Name, v.Name))
	}
	b.WriteString("\n")

	// Generate service protocols
	for _, svc := range ir.Services {
		b.WriteString(fmt.Sprintf("\tclass %sProtocol:\n", strings.Title(svc.Name)))
		b.WriteString(fmt.Sprintf("\t\t\"\"\"Protocol for %s service.\"\"\"\n\n", svc.Name))
		b.WriteString(fmt.Sprintf("\t\tasync def call(self, method: str, req: Dict[str, Any]) -> Any:\n"))
		b.WriteString("\t\t\traise NotImplementedError\n\n")
	}

	// Generate execute method
	b.WriteString("\tasync def execute(self")
	for _, v := range ir.Variables {
		b.WriteString(fmt.Sprintf(", %s: %s", v.Name, pythonType(v.Type)))
	}
	b.WriteString(") -> Dict[str, Any]:\n")
	b.WriteString(fmt.Sprintf("\t\t\"\"\"Execute the %s flow.\"\"\"\n", ir.Name))
	for _, node := range ir.Nodes {
		if node.Type == "start" || node.Type == "end" || node.Type == "merge" {
			continue
		}
		if node.Target != "" {
			b.WriteString(fmt.Sprintf("\t\t# Step: %s\n", node.Name))
		}
	}
	b.WriteString("\t\treturn {}\n")

	return b.String(), nil
}

func goType(typ string) string {
	switch typ {
	case "string":
		return "string"
	case "int", "int64":
		return "int"
	case "float", "float64":
		return "float64"
	case "bool":
		return "bool"
	default:
		return "interface{}"
	}
}

func rustType(typ string) string {
	switch typ {
	case "string":
		return "String"
	case "int", "int64":
		return "i64"
	case "float", "float64":
		return "f64"
	case "bool":
		return "bool"
	default:
		return "serde_json::Value"
	}
}

func javaType(typ string) string {
	switch typ {
	case "string":
		return "String"
	case "int", "int64":
		return "long"
	case "float", "float64":
		return "double"
	case "bool":
		return "boolean"
	default:
		return "Object"
	}
}

func pythonType(typ string) string {
	switch typ {
	case "string":
		return "str"
	case "int", "int64":
		return "int"
	case "float", "float64":
		return "float"
	case "bool":
		return "bool"
	default:
		return "Any"
	}
}
