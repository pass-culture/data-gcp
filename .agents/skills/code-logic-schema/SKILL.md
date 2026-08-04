---
name: code-logic-schema
description: 'PR Visual Reviewer & Documentation Synchronizer. Use when reviewing a pull request, generating architecture diagrams from a git diff, creating Mermaid schemas for new features, or updating README.md documentation to reflect code changes. Triggers: "review my PR", "generate a schema", "document this change", "create a diagram for this diff".'
argument-hint: 'Optional: provide a branch name, PR number, or paste the git diff directly'
---

# PR Visual Reviewer & Documentation Synchronizer

## When to Use
- Reviewing a pull request and need a visual impact summary
- Generating Mermaid architecture diagrams from a git diff
- Updating `README.md` documentation to reflect code changes
- Documenting new features or modules with architectural blueprints

## Procedure

When executing this skill, follow these precise phases:

### 1. Architectural & Git Context Analysis
* **Determine Scope:** Classify the PR changes into **Scenario A (Completely New Code)** or **Scenario B (Modifications to Existing Logic)**.
* **Identify Touchpoints:** Pinpoint exactly where the modified or new code interacts with the *rest of the repository* (e.g., existing middleware, base classes, database schemas, or external APIs).
* **Filter Noise:** Intentionally exclude non-functional changes (formatting, variable renaming, test additions) to focus entirely on structural or control-flow evolution.

### 2. Schema Type Selection Matrix
Select the most high-signal Mermaid diagram type based on the architectural impact:
* **Control Flow / Branching Logic:** `flowchart TD` (Default choice).
* **Asynchronous / Distributed / Multi-component Systems:** `sequenceDiagram`.
* **Data Models / Schema Definitions:** `erDiagram`.
* **Stateful Systems / Event-Driven Transitions:** `stateDiagram-v2`.

---

### 3. Execution Scenarios

#### Scenario A: Completely New Code (New Files, Modules, or Features)
* **PR Description Objective:** Map the internals of the new feature, but crucially wrap the diagram in boundaries that show how it attaches to the existing system.
* **Visual Standard:**
    * Place existing repository systems in a background subgraph titled `Existing Repository Context`.
    * Highlight the **Primary Entry Point** in Blue — see Appendix §2 for exact style.
    * Use shape standards from Appendix §1 to classify each node.
* **README.md Objective:** Provide a clean, permanent architectural blueprint of the new system. Do not include git-specific concepts (like branches or PR titles).

#### Scenario B: Modifications to Existing Code (Logic Diffs)
* **PR Description Objective:** Generate a "Logic Diff" using a clear, visual before-and-after comparison.
* **Visual Standard:** Use explicit side-by-side or stacked subgraphs (`subgraph Before` and `subgraph After`). Apply the Git-diff color coding from Appendix §2 (Green = added, Red = removed, Yellow = modified).
* **README.md Objective:** Output ONLY the absolute current truth (the `After` state). Strip all diff color-coding and the `Before` subgraph. Use standard neutral documentation styling.

---

### 4. Output Constraints & Formatting
* **Clarity over Complexity:** If a PR modifies 20 files, do not make a 20-node diagram. Group related module changes into unified abstract blocks.
* **Always produce two sections:** the PR Description block and the README.md block.

### 5. Mermaid Compatibility Rules
Always produce Mermaid that renders correctly in GitHub and VS Code. Violations cause silent rendering failures:
* **Use `<br/>` for line breaks inside node labels.** Never use `\n` — it is not reliably supported across Mermaid versions and renderers.
* **No non-ASCII characters in node labels or subgraph names.** Replace:
    * `·` → `-` or `,`
    * `≥` → `>=`
    * `→` → `->`
    * `×` → `x`
* **No `()` inside cylinder node labels** (`[("...")]`). Parentheses inside cylinder nodes cause parse failures in some renderers — omit them or use a regular rectangle node instead.
* **No special characters unquoted in node labels.** Always wrap labels in `"..."` when they contain `:`, `/`, or `#`.
* **Keep node IDs simple:** alphanumeric only (e.g. `S1A`, `GCS`). No spaces or hyphens in IDs.
* **Subgraph names with spaces must be quoted:** `subgraph Step1["Step 1 - Embed Images"]` not `subgraph Step 1`.

---

## Output Templates

### 📝 PR Description Output

**Brief:** [1-2 sentences summarizing the change and its operational impact].

**Impact Architecture Schema:**

```mermaid
[Insert Mermaid diagram here following the rules above]
```

---

### 📄 README.md Output

**Section:** [Suggested heading to insert or replace in the README].

```mermaid
[Insert clean, neutral Mermaid diagram showing the current state only]
```

---

## 🎨 Appendix: Visual Semantics & Formatting Conventions

To maintain a standardized, instantly readable layout for human engineers, strictly map abstract syntax trees, logic paths, and architectural boundaries using the following shape and color matrix.

### 1. Shape Standards (Structure & Intent)
* **Process / Execution Step `[Text]`**: Standard rectangles for linear code execution, data transformation, or sequential method calls.
* **Conditional / Decision `{Text}`**: Diamonds for `if/else` statements, ternary operators, or switch-case routing. Ensure exit paths are clearly labeled (e.g., `-->|True|`, `-->|False|`).
* **Storage / Persistence `[(Text)]`**: Cylinders for anything touching database operations, cache writes, or file-system modifications.
* **I/O Boundaries `[/Text/]`**: Parallelograms for network operations, third-party webhook receptions, or public events.
* **Execution Context `subgraph Name`**: Use subgraphs to group related files or explicitly partition system boundaries (e.g., separating "Before" and "After" architectures).

### 2. Color Coding Matrix (Git-Diff & Functional Flow)

Apply the exact hex-codes below using Mermaid class definitions (`classDef`) or explicit inline styling (`style`).

#### Context & Setup Highlighting
* **Entry Point Trigger:** Highlight the absolute beginning node of new code execution paths in **Blue**.
  `style NodeName fill:#cce5ff,stroke:#004085,color:#004085,stroke-width:2px;`

#### Scenario B (PR Logic Diffs) Only
When illustrating modifications inside code comparisons, force nodes to match Git semantic tracking:
* **Added Code Paths (Green):** Use for brand-new features, conditions, or files.
  `classDef added fill:#d4edda,stroke:#28a745,color:#155724,stroke-width:2px;`
* **Removed Code Paths (Red):** Use for deprecated functions, blocks, or deleted options.
  `classDef removed fill:#f8d7da,stroke:#dc3545,color:#721c24,stroke-width:2px;`
* **Refactored / Modified Paths (Yellow):** Use for existing code structural elements that were modified or updated internally.
  `classDef modified fill:#fff3cd,stroke:#ffc107,color:#856404,stroke-width:2px;`
