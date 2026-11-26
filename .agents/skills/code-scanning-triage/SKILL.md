---
name: code-scanning-triage
description: "Triage and fix the open GitHub code scanning alerts (Semgrep OSS and SonarCloud) of one owning team (CI, DS or DE): list them by rule, decide true or false positive with a written reason, fix the code or add an inline suppression, validate, and open one PR per team. Use when the user mentions code scanning, Semgrep, SonarCloud or Sonar alerts, nosemgrep, NOSONAR, a security/code-scanning URL, or wants to clean up SAST findings. For dependency CVEs (Dependabot, uv audit), use vulnerability-fix instead."
---

# Code Scanning Triage

Handles the alerts of the repo's **Security > Code scanning** tab. Two tools feed it:

- **Semgrep OSS**: `.github/workflows/on_schedule_semgrep.yml` runs `--config auto --config p/security-audit` every day on `master`
  and uploads the results. An alert closes when the next daily scan no longer finds it.
- **SonarCloud** (free plan): its own analysis on `master`. An alert closes after the next analysis.
  Dismissals on GitHub and on SonarCloud do not sync.

Order of preference for each alert: **fix the code**, otherwise **inline suppression** with a reason
(reviewed in the PR), and **GitHub dismissal** only for files that have no inline syntax.

`SKILL_DIR` is the folder of this file in the user's checkout (set in step 1).
`references/rules.md` is the playbook per rule: read it before triaging.

## 0. Inputs

Ask the user for what they have not given:

1. **Scope**, one per run, so that each PR goes to one team. Never pick it yourself:
   - `ci`: `.github/` (CI owners);
   - `ds`: `jobs/ml_jobs/`, `orchestration/dags/jobs/ml/` (DS);
   - `de`: everything else (DE).

   Reviewers: the scope's team, see "Map and ownership" in the root `AGENTS.md`.
2. **Ticket**: e.g. `DE-1234`, `DS-123`, or `DE` / `DS` / `BSR` without a number.
3. Optional filter: a rule (`--rule`) or a path (`--path`), to keep the PR small.

## 1. Prepare

Work in a separate git worktree, so the user's checkout is never touched:

```bash
REPO=$(git rev-parse --show-toplevel)
SKILL_DIR="$REPO/.agents/skills/code-scanning-triage"
WT="$REPO/../data-gcp-code-scanning-$SCOPE"
git -C "$REPO" fetch origin master
git -C "$REPO" worktree add "$WT" -b "$TICKET/triage-code-scanning-$SCOPE" origin/master
```

If the worktree or the branch already exists, ask the user whether to reuse or remove it.
Read the skill, `AGENTS.md` files and `references/rules.md` from `$REPO`: the worktree may not have their latest version.

## 2. List the alerts

Run every command below from `$WT` (the script reads the files of the current checkout to spot deleted ones).

```bash
uv run --script "$SKILL_DIR/scripts/alerts.py" --scope "$SCOPE"            # grouped by rule
uv run --script "$SKILL_DIR/scripts/alerts.py" --scope "$SCOPE" --json     # one alert per line, for the details
```

Skip, and list in the report:

- alerts marked `(file deleted)`: they close at the next scan;
- rules marked "fixed in bulk" in `references/rules.md` (uv cooldown, Sonar uv flags...): a dedicated PR handles them.

For the rule documentation (Sonar's includes how to fix it):

```bash
gh api "repos/{owner}/{repo}/code-scanning/alerts/<number>" --jq .rule.help
```

## 3. Triage

For each alert, in the worktree:

1. Read the code around `path:start_line`, and the callers when the flagged value is a parameter.
2. Trace where the flagged value comes from (table "Where does the input come from?" in `references/rules.md`).
3. Reproduce the finding locally (step 6). If the rule finds nothing, the code already changed: the alert closes at
   the next scan, note it and move on.
4. Give a verdict and a one-line reason:
   - **TP**: a real issue → fix.
   - **Mitigated**: the code already guards against it (redacted, `weights_only=True`, parameterized...) → suppress.
   - **FP**: the rule does not apply here (trusted input, non-security hash...) → suppress.
   - **Question**: needs the owner (e.g. who calls this API?) → ask at the checkpoint, do nothing yet.

## 4. Checkpoint

Show the user one table and wait for their go before changing anything:

| # | Rule | File:line | Verdict | Reason | Action |
|---|---|---|---|---|---|

Group the rows by rule. The user can change any verdict.

## 5. Act

**Fix**: the smallest change that removes the issue, no refactoring. Follow `references/rules.md`.

**Inline suppression**, on the alert's `start_line` or on the line just before it, always with the exact rule id
and a reason. Do not use bare `nosemgrep` / `NOSONAR` (they hide every future finding on the line), nor `noqa` (ruff reads it).

```python
conn.execute(f"DESCRIBE {table_name}")  # nosemgrep: <rule id>, <second rule id> -- table name from the job config
import pytz  # NOSONAR(S6890) <reason>
```

```bash
# nosemgrep: <rule id> -- <reason>
json_array="[$(IFS=,; echo "${folders[*]}")]"
```

- Semgrep: the id is the alert's `rule`, copied exactly. A wrong id suppresses nothing.
- Sonar (Python only): the key without the language prefix, `python:S6890` → `NOSONAR(S6890)`.
- If a comment makes the line too long, `ruff format` may move it: check with step 6 after formatting.
  Jobs that enforce `E501`/`W505` on comments (e.g. `graph_recommendation`) reject long ids with a reason: put the
  reason on its own comment line just above `# nosemgrep: <rule id>`.

**GitHub dismissal**, only for Sonar alerts on workflows or Dockerfiles that are FPs (no inline syntax there).
Ask the user first, then:

```bash
gh api -X PATCH "repos/{owner}/{repo}/code-scanning/alerts/<number>" \
  -f state=dismissed -f dismissed_reason="false positive" -f dismissed_comment="<reason, 280 chars max>"
```

`dismissed_reason` is one of `false positive`, `won't fix`, `used in tests`. The alert stays open in SonarCloud.

## 6. Validate

Re-run each Semgrep rule on the changed files, before (it must find the alert) and after (it must find nothing).
Running only the rule is fast and gives an exact before/after; the version is the one of the CI image:

```bash
uvx --from semgrep==1.156.0 semgrep scan --disable-version-check --quiet \
  --config "r/<rule id>" <files>
```

An unknown rule id prints nothing and no error, which is why the "before" run matters.
Once done, check that the changed files bring no other finding, with the rulesets of CI and pre-commit:
`--config auto --config p/security-audit`. Sonar cannot be run locally: the PR's SonarCloud check shows the result.

Then, for every code change:

- the tests of the area (`AGENTS.md` of the job or of `orchestration/`), from the project folder;
- `make ruff_check` from the repo root;
- `"$REPO/.venv/bin/pre-commit" run --files <changed files>`.

Never run a job's `main.py`, deploy or image build scripts for real: they reach GCP or third-party APIs.

## 7. Commit and open the PR

One commit per rule, so that the reviewer can follow: subject `fix(<area>): <fix or suppress> <rule short name> alerts`,
body with one line per alert (`#number path: verdict, reason`).

Ask before pushing. Then use the [`data-pr-workflow`](../data-pr-workflow/SKILL.md) skill, with:

- title: `(<TICKET>) fix(<scope>): triage code scanning alerts (<main rules>)`;
- in the body, the table of step 4 with the final actions, and the alerts skipped (deleted files, bulk rules, questions).

The repo is public: never quote a secret or personal data from the code in the PR, even redacted.

## 8. Hand over and keep the playbook alive

- Give the user the PR link, the table, the open questions and the dismissals done.
- After the merge, the alerts close at the next daily Semgrep scan or Sonar analysis of `master`:
  `uv run --script "$SKILL_DIR/scripts/alerts.py" --scope "$SCOPE"` the next day to check.
- If you met a rule or a pattern missing from `references/rules.md`, add it in the same PR:
  the next run will triage it faster.
- Once the PR is merged or closed: `git -C "$REPO" worktree remove "$WT"`.
