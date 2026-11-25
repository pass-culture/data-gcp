"""List the open GitHub code scanning alerts of the repo, grouped by rule.

Standard library only; calls the `gh` CLI (authenticated). Run from anywhere in the checkout:

    python3 alerts.py --scope de                 # one owning team, grouped by rule
    python3 alerts.py --scope ds --rule shell    # filter on the rule id
    python3 alerts.py --path jobs/etl_jobs/external/qualtrics --json

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
from pathlib import Path

SCOPES = {
    "ci": (".github/",),
    "ds": ("jobs/ml_jobs/", "orchestration/dags/jobs/ml/"),
}


def scope_of(path: str) -> str:
    for scope, prefixes in SCOPES.items():
        if path.startswith(prefixes):
            return scope
    return "de"


def repo_root() -> Path:
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(out.stdout.strip())


def fetch_open_alerts() -> list[dict]:
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


def summarize(alert: dict, root: Path) -> dict:
    instance = alert["most_recent_instance"]
    location = instance["location"]
    rule = alert["rule"]
    return {
        "number": alert["number"],
        "tool": alert["tool"]["name"],
        "rule": rule["id"],
        "severity": rule.get("security_severity_level") or rule.get("severity"),
        "path": location["path"],
        "start_line": location.get("start_line"),
        "end_line": location.get("end_line"),
        "message": re.sub(r"<!--.*?-->", "", instance["message"]["text"])
        .split("<p>")[0]
        .strip()[:300],
        "scope": scope_of(location["path"]),
        "file_exists": (root / location["path"]).exists(),
        "url": alert["html_url"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scope", choices=["ci", "ds", "de", "all"], default="all")
    parser.add_argument("--path", help="keep alerts whose path starts with this prefix")
    parser.add_argument("--rule", help="keep alerts whose rule id contains this text")
    parser.add_argument("--tool", help="keep alerts of this tool, e.g. 'Semgrep OSS'")
    parser.add_argument(
        "--json", action="store_true", help="print one JSON object per alert"
    )
    args = parser.parse_args()

    root = repo_root()
    alerts = [summarize(a, root) for a in fetch_open_alerts()]
    alerts = [
        a
        for a in alerts
        if (args.scope == "all" or a["scope"] == args.scope)
        and (not args.path or a["path"].startswith(args.path))
        and (not args.rule or args.rule in a["rule"])
        and (not args.tool or a["tool"] == args.tool)
    ]

    if args.json:
        for alert in alerts:
            print(json.dumps(alert))
        return

    by_rule = defaultdict(list)
    for alert in alerts:
        by_rule[(alert["tool"], alert["rule"], alert["severity"])].append(alert)
    print(f"{len(alerts)} open alerts, {len(by_rule)} rules")
    for (tool, rule, severity), group in sorted(
        by_rule.items(), key=lambda item: -len(item[1])
    ):
        print(f"\n[{len(group)}] {tool} | {rule} | {severity}")
        for alert in sorted(group, key=lambda a: (a["path"], a["start_line"] or 0)):
            gone = "" if alert["file_exists"] else "  (file deleted)"
            print(
                f"  #{alert['number']} {alert['scope']} {alert['path']}:{alert['start_line']}{gone}"
            )


if __name__ == "__main__":
    main()
