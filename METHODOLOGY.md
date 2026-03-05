ALWAYS be concise.

# Development Methodology

This project follows **conversational literate programming** principles. Development
happens through explanatory dialogue, not code-first implementation.

## Evidence-Based Reasoning

Agents and developers MUST:

- **Never speculate without evidence.** Don't guess why something failed.
  Investigate. Read logs. Check actual state.
- **Clearly distinguish hypotheses from facts.** "The CI failed because X"
  requires evidence. "The CI failed, possibly because X — let me verify"
  is honest.
- **Support claims with system interactions.** Before asserting what's wrong,
  run commands, read files, check status. Let the system tell you.
- **Run before you read (when diagnosing).** Execution is evidence. Code review
  is opinion. When investigating a problem, run tests, check imports, verify
  behavior first. Only read the code if running it found nothing. Errors found
  by execution are facts; errors found by reading are hypotheses. This applies
  to diagnosis — when *modifying* code, read first (see Working Method below).
- **Verify automation output.** Self-reported success is a claim. After any
  automated process, check the artifacts independently. "All tests pass" means
  nothing until you see the test output.

## Documentation-First

- All significant changes begin with discussion of intent and reasoning
- Complex work is explained through conversation before code
- Every task should have clear justification — the "why" matters as much
  as the "what"
- Conversations and commit messages serve as living documentation

### Literate Code Principle

An LLM (or any developer) should be able to derive all the state needed to
safely modify a file from three sources:

1. **Directory structure** — the layout of the program tells you what exists
   and how it's organized
2. **Imports within the file** — these declare the file's dependencies
   explicitly
3. **Comments within the file** — these link to prose design docs that explain
   the "why" and the broader context

Comments can reference design documents by path (e.g.,
`# See docs/PLANNING/template-design.md`). Design docs MUST link back to
all source files they reference, using explicit paths. A reference can include
`*` if it covers every file in a directory.

If a file is changed, any documentation that references it is potentially stale
and must be checked. Documentation and code are a bidirectional graph — changes
propagate in both directions.

## Working Method

1. **Explore and understand** — Read existing code/state before changing it.
   Use your own tools to do your own work.
2. **Discuss architecture** — Workshop complex decisions through dialogue
3. **Document intent** — Make the reasoning visible (in tasks, commits, PRs)
4. **Explain changes** — Each modification includes context
5. **Maintain narrative** — Keep threads coherent across sessions

## Technical Discipline

- **Timeouts mean something didn't happen**, not that you need to wait longer.
  A timeout is a symptom, not a tuning parameter. Investigate the root cause.
- **Single responsibility** — Each component has one clear purpose
- **Fail fast** — Let errors propagate, don't hide failures, no fallbacks.
  Review processes are interactive, not loop-based. A review produces a list
  of issues; those issues are fed back one at a time for resolution. If a fix
  fails, the task fails — no retry counter, no `max_loops`, no silent fallback.
  Arbitrary loop limits are fallbacks disguised as configuration. They swallow
  failures instead of surfacing them. Long-running event loops that poll for
  new work are not retry loops — the test is whether the loop re-attempts a
  specific failed operation or advances to whatever work is available next.
  But a blanket `except Exception` that silently continues on unknown errors
  IS a retry loop. If the error is unexpected, retrying it is speculation.
- **DRY/KISS** — Minimize duplication and complexity
- **Fix the root cause, not the symptom.** Multiple layers of verification is
  a fallback chain — the same antipattern as try/except with silent degradation.
  Find the single point of failure and fix that. One targeted fix beats three
  safety nets.
- **Own your consequences.** If you move code, verify nothing still imports the
  old location. If you delete a caller, delete the callee. Grep for orphans
  before declaring done. The person making a change is responsible for everything
  that change breaks. You are responsible for the ongoing cost of every line you
  add, not just the immediate correctness.

## Code Quality Standards

All code MUST:

- **Be linted** — Language-appropriate linter, no warnings in CI
- **Be well-typed** — At minimum, use type hints/annotations. Strongly typed
  where it makes sense (critical paths, shared interfaces, data contracts)
- **Be documented** — Public APIs, non-obvious logic, configuration options
- **Follow language best practices** — Idiomatic code for the language in use

**pre-commit is required** in every repo that deploys a production service.
This catches issues before they enter the commit history:
- Linting
- Type checking
- Formatting
- Secret detection
- Commit message format

**Dev/prod dependency splits are an antipattern.** Code must be packaged such
that it can be meaningfully integration tested *before* a container is built.
The thing you test should be the thing you ship. Don't have different
dependency graphs for "dev" vs "prod" — that's a source of surprises.

Testing philosophy and strategy are defined in [TESTING.md](TESTING.md).

## Python Tooling

- **[uv](https://github.com/astral-sh/uv)** for Python package/project management
- Follow uv best practices (lockfiles, pyproject.toml, reproducible installs)
- **Python preferred over bash for scripting** — more testable, better error
  handling, easier to maintain. Use bash only when shell features are required
  (pipelines, process control, existing CLI tool orchestration)

## Infrastructure Automation

- **Use [Ansible](https://docs.ansible.com/) for deterministic infrastructure
  operations** — installing packages, cloning repos, deploying desks, verifying
  services. These are not AI tasks; they are configuration management.
- **All Ansible files MUST pass
  [ansible-lint](https://ansible.readthedocs.io/projects/lint/)** at the
  `production` profile before commit. Run `ansible-lint <playbook>` to verify.
- Follow Ansible best practices:
  - Use fully qualified collection names (`ansible.builtin.*`)
  - Use `changed_when` and `failed_when` on command/shell tasks
  - Prefer proper modules over `command`/`shell` where one exists
  - Use inventory variables, not hardcoded values in playbooks
  - Gather facts only when needed (`gather_facts: false` by default)
  - No retry loops — each task succeeds or fails deterministically
