# FlowRulZ Agent Configuration

## AI Agent Setup (opencode)

The repository uses **opencode** as the AI coding assistant. It reads `CLAUDE.md` (project context + conventions) and `AGENTS.md` (this file, agent config) on each conversation start.

### Available Skills

| Skill | Trigger | Purpose |
|-------|---------|---------|
| `caveman` | `/caveman`, "caveman mode" | Ultra-compressed communication (lite/full/ultra/wenyan variants) |
| `caveman-commit` | `/commit`, staging changes | Ultra-compressed Conventional Commits (subject <=50 chars) |
| `caveman-compress` | `/caveman:compress <file>` | Compress memory files into caveman format |
| `caveman-help` | `/caveman-help` | Quick-ref for all caveman modes/commands |
| `caveman-review` | `/review`, PR review | Ultra-compressed code review comments |
| `customize-opencode` | Editing opencode config | Editing `opencode.json`, `.opencode/`, skills, MCP, permissions |
| `find-skills` | "find a skill for X" | Discover and install new agent skills |
| `frontend-design` | Building new UI | Aesthetic direction, typography, intentional visual design |
| `skill-creator` | Create/edit skills | Create, modify, benchmark, and optimize agent skills |

### Agent Tools

The AI agent has access to:

- **`bash`** — Shell execution (git, npm, docker, build commands)
- **`read`** — Read files/directories
- **`write`** — Write files
- **`edit`** — Exact string replacement in files
- **`glob`** — File pattern matching
- **`grep`** — Content search with regex
- **`question`** — Ask user for preferences/decisions
- **`task`** — Delegate complex multistep work to sub-agents
- **`todowrite`** — Structured task list management
- **`webfetch`** — Fetch URL content
- **`websearch`** — Real-time web search

### Sub-Agent Types

| Type | Use Case | Tools |
|------|----------|-------|
| `explore` | Fast codebase exploration, file/pattern searches | read, glob, grep, bash |
| `general` | Complex multi-step research and execution tasks | All tools |

### Conventions

1. Agent reads `docs/` dir on each conversation start
2. After any code change, relevant `.md` files in `docs/` must be updated
3. Never let docs go stale
4. Agent runs `make test` / `go vet` to verify changes before signaling completion
5. Only commit when explicitly asked by the user
