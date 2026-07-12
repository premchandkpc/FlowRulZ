package flow

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// CLI is the command-line interface for .flow files.
type CLI struct {
	parser    *Parser
	formatter *Formatter
	analyzer  *Analyzer
	compiler  *Compiler
	codegen   *CodeGenerator
	graph     *GraphGenerator
}

// NewCLI creates a new CLI.
func NewCLI() *CLI {
	return &CLI{
		parser:    NewParser(),
		formatter: NewFormatter(),
		analyzer:  NewAnalyzer(),
		compiler:  NewCompiler(),
		graph:     NewGraphGenerator(),
	}
}

// Run executes a CLI command.
func (c *CLI) Run(args []string) error {
	if len(args) < 2 {
		return c.printUsage()
	}

	cmd := args[1]
	files := args[2:]

	switch cmd {
	case "fmt":
		return c.fmt(files)
	case "validate":
		return c.validate(files)
	case "graph":
		return c.graphCmd(files)
	case "codegen":
		return c.codegenCmd(files)
	case "info":
		return c.info(files)
	case "help":
		return c.printUsage()
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func (c *CLI) printUsage() error {
	fmt.Println(`Usage: flow <command> [files...]

Commands:
  fmt        Format .flow files (canonical style)
  validate   Validate .flow files
  graph      Generate graph (DOT or Mermaid)
  codegen    Generate code (Go, Rust, Java, Python)
  info       Show flow information
  help       Show this help

Examples:
  flow fmt *.flow
  flow validate signup.flow
  flow graph -format dot signup.flow
  flow graph -format mermaid signup.flow
  flow codegen -target go signup.flow
  flow info signup.flow`)
	return nil
}

func (c *CLI) fmt(files []string) error {
	if len(files) == 0 {
		// Read from stdin
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
		return c.formatData(data)
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}

		flow, err := c.parser.Parse(data)
		if err != nil {
			return fmt.Errorf("parse %s: %w", file, err)
		}

		output := c.formatter.Format(flow)

		if err := os.WriteFile(file, []byte(output), 0644); err != nil { //nolint:gosec // CLI formatter writes user-generated code, not sensitive data
			return fmt.Errorf("write %s: %w", file, err)
		}

		fmt.Printf("Formatted %s\n", file)
	}

	return nil
}

func (c *CLI) formatData(data []byte) error {
	flow, err := c.parser.Parse(data)
	if err != nil {
		return err
	}

	fmt.Print(c.formatter.Format(flow))
	return nil
}

func (c *CLI) validate(files []string) error {
	hasErrors := false

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			fmt.Printf("%s: error: %v\n", file, err)
			hasErrors = true
			continue
		}

		flow, err := c.parser.Parse(data)
		if err != nil {
			fmt.Printf("%s: parse error: %v\n", file, err)
			hasErrors = true
			continue
		}

		errs := c.analyzer.Analyze(flow)
		if len(errs) > 0 {
			for _, e := range errs {
				fmt.Printf("%s: %v\n", file, e)
			}
			hasErrors = true
		} else {
			fmt.Printf("%s: valid\n", file)
		}
	}

	if hasErrors {
		return fmt.Errorf("validation failed")
	}
	return nil
}

func (c *CLI) graphCmd(files []string) error {
	format := "dot"
	for i, arg := range files {
		if arg == "-format" && i+1 < len(files) {
			format = files[i+1]
			files = append(files[:i], files[i+2:]...)
			break
		}
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}

		flow, err := c.parser.Parse(data)
		if err != nil {
			return fmt.Errorf("parse %s: %w", file, err)
		}

		ir, err := c.compiler.Compile(flow)
		if err != nil {
			return fmt.Errorf("compile %s: %w", file, err)
		}

		var output string
		switch format {
		case "dot":
			output = c.graph.DOT(ir)
		case "mermaid":
			output = c.graph.Mermaid(ir)
		default:
			return fmt.Errorf("unsupported format: %s", format)
		}

		fmt.Print(output)
	}

	return nil
}

func (c *CLI) codegenCmd(files []string) error {
	target := TargetGo
	for i, arg := range files {
		if arg == "-target" && i+1 < len(files) {
			target = CodeGenTarget(files[i+1])
			files = append(files[:i], files[i+2:]...)
			break
		}
	}

	c.codegen = NewCodeGenerator(target)

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}

		flow, err := c.parser.Parse(data)
		if err != nil {
			return fmt.Errorf("parse %s: %w", file, err)
		}

		ir, err := c.compiler.Compile(flow)
		if err != nil {
			return fmt.Errorf("compile %s: %w", file, err)
		}

		output, err := c.codegen.Generate(ir)
		if err != nil {
			return fmt.Errorf("generate %s: %w", file, err)
		}

		fmt.Print(output)
	}

	return nil
}

func (c *CLI) info(files []string) error {
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}

		flow, err := c.parser.Parse(data)
		if err != nil {
			return fmt.Errorf("parse %s: %w", file, err)
		}

		ir, err := c.compiler.Compile(flow)
		if err != nil {
			return fmt.Errorf("compile %s: %w", file, err)
		}

		fmt.Printf("Flow: %s\n", ir.Name)
		fmt.Printf("Services: %d\n", len(ir.Services))
		fmt.Printf("Events: %d\n", len(ir.Events))
		fmt.Printf("Variables: %d\n", len(ir.Variables))
		fmt.Printf("Constants: %d\n", len(ir.Constants))
		fmt.Printf("Nodes: %d\n", len(ir.Nodes))
		fmt.Printf("Edges: %d\n", len(ir.Edges))

		if ir.Retry != nil {
			fmt.Printf("Retry: %d attempts, %s backoff\n", ir.Retry.Attempts, ir.Retry.Backoff)
		}
		if ir.Breaker != nil {
			fmt.Printf("Breaker: %d%% failure rate\n", ir.Breaker.FailureRate)
		}

		fmt.Println()
	}

	return nil
}

// ParseArgs parses CLI arguments into flags and files.
func ParseArgs(args []string) (flags map[string]string, files []string) {
	flags = make(map[string]string)
	for i := 1; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-") {
			key := args[i]
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				flags[key] = args[i+1]
				i++
			} else {
				flags[key] = "true"
			}
		} else {
			files = append(files, args[i])
		}
	}
	return
}
