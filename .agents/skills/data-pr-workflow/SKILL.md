---
name: data-pr-workflow
description: Create pull requests that follow data team standards with proper naming conventions, commit validation, and squash-and-merge workflow. Use this skill when the user wants to create a PR, mentions pull requests, needs help with commit naming, wants to clean up commits before review, or talks about submitting work for review. Also trigger when they mention tickets like DE-XXX, HF-XXX, or BSR, or when they're working with dbt, bq_jobs, ml_jobs, or analytics code and need to get their changes reviewed.
---

# Data Team PR Workflow

This skill helps create pull requests that follow the data team's standards, including proper naming conventions, commit cleanup, and review request formatting.

## When to Use This Skill

Use this skill when the user:
- Wants to create a pull request
- Needs to validate or clean up commits before creating a PR
- Asks for help with PR naming or formatting
- Mentions submitting code for review
- Has a branch ready and wants to create a proper PR

## Workflow Overview

The process follows these steps:
1. Validate current branch and commits against naming standards
2. Help clean up commit history if needed (squash/rebase)
3. Generate a compliant PR title
4. Create the PR with proper description
5. Generate the #data-review Slack message

## Step 1: Validate Current State

Start by checking where the user is in their workflow:

```bash
# Check current branch and recent commits
git branch --show-current
git log --oneline -10
git status
```

Look for:
- Branch name should be: `TICKET-ID/simple-description-in-eng`
- Commit messages should follow the format
- Number of commits (ideally < 10)

## Step 2: Commit Naming Standards

Commits must follow this format:
```
(TICKET-ID) type(scope): description
```

**Ticket ID** (optional in commit, required in PR):
- `DE-XXX` - Data tasks
- `HF-XXX` - HotFix (urgent)
- `BSR` - Boy Scout Rule (code improvements)
- Can be omitted in commits if not applicable

**Type** (required):
- `feat` - New feature (code planned in the ticket)
- `fix` - Bug fix
- `test` - Test additions or modifications
- `docs` - Documentation updates
- `refactor` - Code restructuring without behavior change
- `perf` - Performance improvements
- `build`, `lint`, `ci`, `core`, `dbt` - Other types

**Scope** (required):
The area of the codebase affected:
- `bq_jobs` - Import sources (applicative, firebase, contentful) or export (clickhouse, pcapi)
- `dbt` - dbt models and transformations (int_firebase, analytics, export_backend, etc.)
- `ml_jobs` - ML job types (retrieval, ranking, compliance, recommendation)
- Generic: `core`, `analytics`, `engineering`, `science`, `ci`

**Description** (required):
A brief description in English. Can include table names in parentheses if relevant.

**Examples:**
- `(DE-3456) feat(bq_jobs): add new import job for Firebase events`
- `(DE-7890) fix(dbt): correct transformation logic in export_backend model`
- `feat(analytics): add global_booking mart model`
- `review: include review comments`

## Step 3: Commit Cleanup

If the branch has too many commits (>10) or commits with unclear names (like "wip", "fix", "temp"), help clean them up.
Give the rebase strategy specifying the command to the user but do not execute it.

**Identify commits to clean:**
```bash
# Show commits since branching from master
git log master..HEAD --oneline
```

Look for patterns like:
- Multiple "fix" commits that could be squashed
- "wip" or temporary commits
- Commits that should be combined

**Squash strategy:**

For simple cases (combining last N commits):
```bash
# Interactive rebase to squash last N commits
git rebase -i HEAD~N
```

Explain to the user and do not execute :
1. In the editor that opens, change `pick` to `squash` (or `s`) for commits to combine
2. Keep `pick` for the first commit you want to keep
3. Save and close
4. Edit the combined commit message in the next editor
5. Force push: `git push -f`

For more complex cleanup (squashing specific commits):
```bash
# Find the commit where the branch started
git merge-base master HEAD
# Rebase interactively from there
git rebase -i $(git merge-base master HEAD)
```

**Suggest a target structure** based on the changes. For example:
- Commit 1: `feat(scope): add main functionality`
- Commit 2: `test(scope): add tests and documentation`
- Commit 3: `review: incorporate review feedback` (if there was a prior review)

## Step 4: Generate PR Title

The PR title must follow this regex pattern:
```
^(\((DA-[0-9]+|DE-[0-9]+|HF-[0-9]+|BSR|)\) )?(build|lint|ci|docs|feat|fix|perf|refactor|test|core|dbt|)\(\w+\): \w+
```

**Format:**
```
(TICKET-ID) type(scope): brief description (table_name if applicable)
```

**To generate the title:**
1. Identify the ticket ID from the branch name or commits
2. Determine the primary type of change (feat, fix, refactor, etc.)
3. Identify the scope (the area of code most affected)
4. Write a concise description that captures the "why" of the change
5. If relevant, include the main table or model name in parentheses

**Examples:**
- `(DE-3456) feat(bq_jobs): add Firebase events import`
- `(DA-7890) fix(dbt): correct export_backend transformation logic`
- `(DE-2244) feat(dbt): add partitioning macros (global_booking)`
- `(BSR) refactor(core): simplify config loading`

**Validate** the title matches the regex before proceeding.

## Step 5: Create PR Description

The PR description should be concise but informative. Remember: the code is public, so avoid sensitive details.

**Template:**
```markdown
## Summary
[2-3 sentences explaining what this PR does and why]

## Changes
- [Key change 1]
- [Key change 2]
- [Key change 3]

## Related
- Ticket: [link or reference to DE-XXX in Notion]
- Documentation: [link if applicable]

## Testing
[Brief note on how this was tested]
```

**Keep it brief.** If more context is needed, reference the Notion ticket.

## Step 6: Definition of Ready (DOR) Checklist

Before creating the PR, verify:
- [ ] All tests are passing (CI is green)
- [ ] Code has been self-reviewed
- [ ] Documentation is written in Notion
- [ ] Ticket is moved to "In Review" status
- [ ] Commits are cleaned up (< 10 commits, clear names)

Ask the user to confirm these points if not already clear.

## Step 7: Create the PR

Use `gh pr create` with the generated title and description:

```bash
gh pr create --title "GENERATED_TITLE" --body "GENERATED_DESCRIPTION"
```

If the work is not ready for review yet, create as draft:
```bash
gh pr create --title "GENERATED_TITLE" --body "GENERATED_DESCRIPTION" --draft
```

After creation, get the PR number for the review message.

## Step 8: Generate #data-review Message

Create the Slack message for the #data-review channel:

**Format:**
```
[REVIEW] (TICKET-ID) Brief subject @reviewer-name | Complexity: X | Time: ~Xmn
```

**Components:**
- **Brief subject**: Short description of what needs review
- **@reviewer-name**: Tag specific person if known, otherwise skip
- **Complexity**: [0, 3, 5] - 0=trivial, 3=moderate, 5=complex
- **Time**: Estimated review time (<5mn, ~10mn, >20mn)

**Example:**
```
[REVIEW] (DE-3456) New Firebase import job @data-engineer | Complexity: 3 | Time: ~15mn
```

Ask the user about complexity and time if not obvious from the changes.

## Step 9: Final Reminders

After the PR is created, remind the user:

**Before merging** (when approved):
- [ ] PR has been reviewed by a colleague
- [ ] CI is green (all tests passing)
- [ ] Use **SQUASH AND MERGE** (not regular merge)
- [ ] Final commit message should be: `(TICKET-ID) type(scope): description (#PR-NUMBER)`

**Squash merge** ensures the entire PR becomes a single commit on master with a clean history.

## Common Issues and Solutions

**Issue: Branch name doesn't match format**
- Current: `feature/new-stuff`
- Expected: `DE-123/add-firebase-import`
- Solution: Rename branch: `git branch -m DE-123/add-firebase-import`

**Issue: No ticket ID**
- If genuinely no ticket, use `(BSR)` for improvements or create a minimal ticket
- For hotfixes, use `(HF)` prefix

**Issue: Too many small commits**
- Use interactive rebase to squash: `git rebase -i HEAD~N`
- Aim for 2-5 meaningful commits that tell a story

**Issue: Commit messages unclear**
- Rewrite them during squash, or use `git commit --amend` for the last one
- Each commit should explain what and why, not just "fix" or "update"

**Issue: PR title too long**
- Keep under 70 characters
- Move details to the description
- Use abbreviations if necessary

## Tips for Success

**Branch naming**: Always include the ticket ID and a brief English description.
```
DE-123/add-booking-model  ✓
feature/booking            ✗
```

**Commit messages**: Think of them as documentation for future developers.
```
feat(dbt): add global_booking model with daily partitioning  ✓
added stuff                                                   ✗
```

**PR size**: Smaller PRs get faster, better reviews. If your PR is large:
- Consider splitting into multiple PRs
- At minimum, ensure commits are well-organized to aid review

**Self-review**: Before requesting review, go through your own PR and look for:
- Debug code or comments that should be removed
- Obvious improvements or refactorings
- Missing tests or documentation

## Validation Script

If you need to validate a PR title against the regex programmatically:

```python
import re

pattern = r'^(\((DA-[0-9]+|DE-[0-9]+|HF-[0-9]+|BSR|)\) )?(build|lint|ci|docs|feat|fix|perf|refactor|test|core|dbt|)\(\w+\): \w+'

def validate_pr_title(title):
    if re.match(pattern, title):
        return True, "PR title is valid"
    else:
        return False, "PR title does not match required format"

# Test the title
title = "(DE-123) feat(dbt): add new model"
is_valid, message = validate_pr_title(title)
print(f"{title}: {message}")
```

Use this to check titles before creating the PR.
