---
name: pr-review
description: 'Review a pull request and rate it /10 on bugs, security, improvements, technical quality, and consistency with adjacent code.'
---

# PR Review Skill

Review the PR diff and provide a concise score out of 10 with feedback on each axis below.

## Evaluation Criteria

### 1. Bugs
- Logic errors, off-by-one, race conditions, unhandled edge cases.
- Missing null/empty checks, incorrect types.

### 2. Security
- Vulnerable or outdated packages (check CVEs when relevant).
- Secrets in code, SQL injection, unsafe deserialization, overly permissive permissions.

### 3. Improvements
- Could a better library simplify this?
- Unnecessary complexity—can the code be simplified without losing clarity?
- Over-engineering vs. pragmatic solutions.

### 4. Technical Quality
- Code is clear and readable.
- Well separated into focused functions/modules.
- Apply AHA programming: only extract/reuse when something is used **more than 3 times**; premature abstraction is worse than duplication.

### 5. Consistency
- The code follows the same patterns, naming conventions, and style as the surrounding/adjacent code in the repo.

## Output Format

```
## PR Review — <PR title or short summary>

| Criteria         | Score /10 | Comments |
|------------------|-----------|----------|
| Bugs             |           |          |
| Security         |           |          |
| Improvements     |           |          |
| Technical Quality|           |          |
| Consistency      |           |          |
| **Overall**      |   **/10** |          |

### Key Findings
- …

### Suggestions
- …
```

Keep feedback actionable and concise. Praise what's done well.
