# /// script
# requires-python = ">=3.11"
# ///
"""List the open GitHub code scanning alerts of the repo, grouped by rule.

Standard library only; calls ``git`` and the authenticated ``gh`` CLI. Run it from the
checkout to triage: it flags the alerts whose file no longer exists in that checkout.

Usage::

    uv run --script alerts.py --scope de                 # one owning team, grouped by rule
    uv run --script alerts.py --scope ds --rule shell    # filter on the rule id
    uv run --script alerts.py --path jobs/etl_jobs/external/qualtrics --json

Sample output::

    3 open alerts, 2 rules

    [2] Semgrep OSS | subprocess-shell-true | high
      #2 de jobs/etl_jobs/a.py:30
      #1 de jobs/etl_jobs/b.py:8

    [1] SonarCloud | python:S5332 | medium
      #3 ci .github/old.yml:4  (file deleted)

Rules are sorted by number of alerts, alerts by path and line.
``(file deleted)``: the file is gone from the checkout, the alert closes at the next scan.

With ``--json``, one JSON object per alert and per line, with the fields of ``Alert``.

Scopes follow the directory rules of .github/CODEOWNERS:

    ci  .github/                                     @pass-culture/data-gcp-ci-owners
    ds  jobs/ml_jobs/, orchestration/dags/jobs/ml/   @pass-culture/data-science
    de  everything else                              @pass-culture/data-engineering
"""

import argparse
import json
import re
import subprocess
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, get_args

Scope = Literal["ci", "ds", "de"]
RawAlert = dict[str, Any]  # one alert of the GitHub API, see fetch_open_alerts
RuleKey = tuple[str, str, str | None]  # (tool, rule id, severity)

# Checked in order; a path that matches no prefix belongs to "de".
SCOPE_PREFIXES: dict[Scope, tuple[str, ...]] = {
    "ci": (".github/",),
    "ds": ("jobs/ml_jobs/", "orchestration/dags/jobs/ml/"),
}
MESSAGE_MAX_LENGTH = 300


@dataclass(frozen=True)
class Alert:
    """The fields of one open alert that matter for the triage.

    Attributes:
        number: alert number, as in the URL ``.../code-scanning/<number>``.
        tool: scanner name, e.g. ``Semgrep OSS`` or ``SonarCloud``.
        rule: rule id, e.g. ``python.lang.security.audit.subprocess-shell-true...``.
        severity: security severity (``critical``...``low``) when the rule has one,
            else the rule severity (``error``, ``warning``, ``note``).
        path: file path, relative to the repo root.
        start_line, end_line: flagged lines of the most recent analysis, when known.
        message: first paragraph of the scanner message, without HTML comments.
        scope: owning team of the path, see ``scope_of``.
        file_exists: whether the file still exists in the local checkout.
        url: the alert page on GitHub.
    """

    number: int
    tool: str
    rule: str
    severity: str | None
    path: str
    start_line: int | None
    end_line: int | None
    message: str
    scope: Scope
    file_exists: bool
    url: str


@dataclass(frozen=True)
class Filters:
    """The command-line filters; an alert is kept when it passes all of them.

    Attributes:
        scope: owning team, or ``all``.
        path: keep alerts whose path starts with this prefix.
        rule: keep alerts whose rule id contains this text.
        tool: keep alerts of this exact tool.
    """

    scope: Scope | Literal["all"] = "all"
    path: str | None = None
    rule: str | None = None
    tool: str | None = None

    def matches(self, alert: Alert) -> bool:
        """Tell whether the alert passes every filter that is set.

        >>> alert = Alert(
        ...     number=1, tool="Semgrep OSS", rule="python.subprocess-shell-true",
        ...     severity="high", path="jobs/etl_jobs/a.py", start_line=1, end_line=1,
        ...     message="", scope="de", file_exists=True, url="",
        ... )
        >>> Filters().matches(alert)
        True
        >>> Filters(scope="de", path="jobs/etl_jobs/", rule="shell").matches(alert)
        True
        >>> Filters(scope="ds").matches(alert)
        False
        >>> Filters(tool="SonarCloud").matches(alert)
        False
        """
        return (
            (self.scope == "all" or alert.scope == self.scope)
            and (not self.path or alert.path.startswith(self.path))
            and (not self.rule or self.rule in alert.rule)
            and (not self.tool or alert.tool == self.tool)
        )


def scope_of(path: str) -> Scope:
    """Return the team that owns a path, following .github/CODEOWNERS.

    >>> scope_of(".github/workflows/on_pr.yml")
    'ci'
    >>> scope_of("orchestration/dags/jobs/ml/algo_training.py")
    'ds'
    >>> scope_of("orchestration/dags/jobs/import/import_qualtrics.py")
    'de'
    """
    for scope, prefixes in SCOPE_PREFIXES.items():
        if path.startswith(prefixes):
            return scope
    return "de"


def clean_message(text: str) -> str:
    """Shorten a scanner message to its first paragraph, for a one-line summary.

    Scanners write HTML: HTML comments are removed, the text stops at the first ``<p>``
    and is cut at ``MESSAGE_MAX_LENGTH`` characters.

    >>> clean_message("Found 'subprocess' with 'shell=True'.<!-- id: 42 --><p>Details...</p>")
    "Found 'subprocess' with 'shell=True'."
    >>> len(clean_message("x" * 1000))
    300
    """
    without_comments = re.sub(r"<!--.*?-->", "", text)
    return without_comments.split("<p>")[0].strip()[:MESSAGE_MAX_LENGTH]


def summarize(raw: RawAlert, root: Path) -> Alert:
    """Keep the useful fields of one alert of the GitHub API.

    ``root`` is the checkout where to look for the alert's file.

    >>> raw = {
    ...     "number": 7,
    ...     "html_url": "https://github.com/pass-culture/data-gcp/security/code-scanning/7",
    ...     "tool": {"name": "SonarCloud"},
    ...     "rule": {"id": "python:S5332", "severity": "warning",
    ...              "security_severity_level": "medium"},
    ...     "most_recent_instance": {
    ...         "location": {"path": ".github/old.yml", "start_line": 4, "end_line": 5},
    ...         "message": {"text": "Using http is insecure.<p>See the docs.</p>"},
    ...     },
    ... }
    >>> summarize(raw, Path("/nonexistent"))  # doctest: +NORMALIZE_WHITESPACE
    Alert(number=7, tool='SonarCloud', rule='python:S5332', severity='medium',
          path='.github/old.yml', start_line=4, end_line=5,
          message='Using http is insecure.', scope='ci', file_exists=False,
          url='https://github.com/pass-culture/data-gcp/security/code-scanning/7')
    """
    instance = raw["most_recent_instance"]
    location = instance["location"]
    rule = raw["rule"]
    return Alert(
        number=raw["number"],
        tool=raw["tool"]["name"],
        rule=rule["id"],
        severity=rule.get("security_severity_level") or rule.get("severity"),
        path=location["path"],
        start_line=location.get("start_line"),
        end_line=location.get("end_line"),
        message=clean_message(instance["message"]["text"]),
        scope=scope_of(location["path"]),
        file_exists=(root / location["path"]).exists(),
        url=raw["html_url"],
    )


def group_by_rule(alerts: list[Alert]) -> list[tuple[RuleKey, list[Alert]]]:
    """Group the alerts by (tool, rule, severity), the largest groups first.

    Groups of the same size keep the order in which their rule first appears.
    Inside a group, alerts are sorted by path then line (no line counts as 0).

    >>> from dataclasses import replace
    >>> base = Alert(
    ...     number=1, tool="Semgrep OSS", rule="shell", severity="high", path="b.py",
    ...     start_line=8, end_line=8, message="", scope="de", file_exists=True, url="",
    ... )
    >>> alerts = [
    ...     replace(base, number=1, rule="eval"),
    ...     replace(base, number=2),
    ...     replace(base, number=3, path="a.py", start_line=None),
    ... ]
    >>> [(key, [a.number for a in group]) for key, group in group_by_rule(alerts)]
    [(('Semgrep OSS', 'shell', 'high'), [3, 2]), (('Semgrep OSS', 'eval', 'high'), [1])]
    """
    by_rule: defaultdict[RuleKey, list[Alert]] = defaultdict(list)
    for alert in alerts:
        by_rule[(alert.tool, alert.rule, alert.severity)].append(alert)
    return [
        (key, sorted(group, key=lambda a: (a.path, a.start_line or 0)))
        for key, group in sorted(by_rule.items(), key=lambda item: -len(item[1]))
    ]


def format_report(alerts: list[Alert]) -> str:
    """Format the alerts as the text report: a count, then one block per rule.

    >>> from dataclasses import replace
    >>> base = Alert(
    ...     number=1, tool="Semgrep OSS", rule="subprocess-shell-true", severity="high",
    ...     path="jobs/etl_jobs/b.py", start_line=8, end_line=8, message="", scope="de",
    ...     file_exists=True, url="",
    ... )
    >>> alerts = [
    ...     base,
    ...     replace(base, number=2, path="jobs/etl_jobs/a.py", start_line=30),
    ...     replace(base, number=3, tool="SonarCloud", rule="python:S5332",
    ...             severity="medium", path=".github/old.yml", start_line=4, scope="ci",
    ...             file_exists=False),
    ... ]
    >>> print(format_report(alerts))
    3 open alerts, 2 rules
    <BLANKLINE>
    [2] Semgrep OSS | subprocess-shell-true | high
      #2 de jobs/etl_jobs/a.py:30
      #1 de jobs/etl_jobs/b.py:8
    <BLANKLINE>
    [1] SonarCloud | python:S5332 | medium
      #3 ci .github/old.yml:4  (file deleted)
    """
    groups = group_by_rule(alerts)
    lines = [f"{len(alerts)} open alerts, {len(groups)} rules"]
    for (tool, rule, severity), group in groups:
        lines += ["", f"[{len(group)}] {tool} | {rule} | {severity}"]
        for alert in group:
            gone = "" if alert.file_exists else "  (file deleted)"
            lines.append(
                f"  #{alert.number} {alert.scope} {alert.path}:{alert.start_line}{gone}"
            )
    return "\n".join(lines)


def repo_root() -> Path:
    """Return the root of the git checkout of the current folder."""
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(out.stdout.strip())


def fetch_open_alerts() -> list[RawAlert]:
    """Fetch every open code scanning alert of the current repo, through ``gh api``.

    ``gh`` fills ``{owner}/{repo}`` from the git remote, follows the pages
    (``--paginate``) and returns them as one JSON list of pages (``--slurp``).
    Payload: https://docs.github.com/en/rest/code-scanning/code-scanning#list-code-scanning-alerts-for-a-repository
    """
    out = subprocess.run(
        [
            "gh",
            "api",
            "--paginate",
            "--slurp",
            "repos/{owner}/{repo}/code-scanning/alerts?state=open&per_page=100",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return [alert for page in json.loads(out.stdout) for alert in page]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--scope", choices=[*get_args(Scope), "all"], default="all")
    parser.add_argument("--path", help="keep alerts whose path starts with this prefix")
    parser.add_argument("--rule", help="keep alerts whose rule id contains this text")
    parser.add_argument("--tool", help="keep alerts of this tool, e.g. 'Semgrep OSS'")
    parser.add_argument(
        "--json", action="store_true", help="print one JSON object per alert"
    )
    args = parser.parse_args()
    filters = Filters(scope=args.scope, path=args.path, rule=args.rule, tool=args.tool)

    root = repo_root()
    alerts = [summarize(raw, root) for raw in fetch_open_alerts()]
    alerts = [alert for alert in alerts if filters.matches(alert)]

    if args.json:
        for alert in alerts:
            print(json.dumps(asdict(alert)))
        return
    print(format_report(alerts))


if __name__ == "__main__":
    main()
