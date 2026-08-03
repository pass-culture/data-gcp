"""Pure block/property rendering for the dashboard-docs export.

These helpers (rich_text / prop_to_text / detect_dashboard / skip_reason / ...) are
schema-agnostic and free of network or credentials, so they can be unit-tested in
isolation. The HTTP calls live in `core.notion_api`.
"""

import json
import re

# Metabase dashboard/question deep links, e.g. https://metabase.../dashboard/881
DASH_RE = re.compile(r"https?://[^\s)]+/(?:dashboard|question)/(\d+)")
# Docs we never want to index (WIP / duplicable templates).
TEMPLATE_RE = re.compile(r"template|à dupliquer|WIP|brouillon", re.I)


def rich_text(arr) -> str:
    return "".join(t.get("plain_text", "") for t in (arr or []))


def prop_to_text(p: dict) -> str:
    """Render any Notion property generically to a flat string (schema-agnostic)."""
    t = p.get("type")
    v = p.get(t)
    if t in ("title", "rich_text"):
        return rich_text(v)
    if t == "select":
        return (v or {}).get("name", "") if v else ""
    if t == "status":
        return (v or {}).get("name", "") if v else ""
    if t == "multi_select":
        return ", ".join(o.get("name", "") for o in (v or []))
    if t == "url":
        return v or ""
    if t == "number":
        return "" if v is None else str(v)
    if t == "checkbox":
        return str(v)
    if t == "date":
        return (v or {}).get("start", "") if v else ""
    if t == "people":
        return ", ".join(o.get("name", o.get("id", "")) for o in (v or []))
    if t == "relation":
        return ", ".join(o.get("id", "") for o in (v or []))
    if t in ("formula", "rollup"):
        return json.dumps(v, ensure_ascii=False)
    return json.dumps(v, ensure_ascii=False) if v is not None else ""


def title_of(props: dict) -> str:
    for _name, p in props.items():
        if p.get("type") == "title":
            return rich_text(p.get("title"))
    return ""


def block_line(b: dict) -> str:
    """Render a single block's own line to markdown (children handled by the caller)."""
    t = b.get("type")
    data = b.get(t, {})
    txt = rich_text(data.get("rich_text")) if isinstance(data, dict) else ""
    if t == "paragraph":
        return txt
    if t == "heading_1":
        return f"# {txt}"
    if t == "heading_2":
        return f"## {txt}"
    if t == "heading_3":
        return f"### {txt}"
    if t == "bulleted_list_item":
        return f"- {txt}"
    if t == "numbered_list_item":
        return f"1. {txt}"
    if t == "to_do":
        chk = "x" if data.get("checked") else " "
        return f"- [{chk}] {txt}"
    if t in ("toggle", "quote", "callout"):
        return f"> {txt}"
    if t == "code":
        return f"```{data.get('language', '')}\n{txt}\n```"
    if t == "divider":
        return "---"
    if t == "child_page":
        return f"- (sub-page) {data.get('title', '')}"
    if t in ("bookmark", "embed"):
        return data.get("url", "")
    if t == "image":
        url = (data.get("external") or data.get("file") or {}).get("url", "")
        return f"![image]({url})"
    return txt


def detect_dashboard(props_text: dict, page_url: str, body: str):
    """Return (matched_url, dashboard_id) of the first Metabase deep link, else (None, None)."""
    hay = " ".join(list(props_text.values()) + [page_url or "", body or ""])
    m = DASH_RE.search(hay)
    return (m.group(0), int(m.group(1))) if m else (None, None)


def skip_reason(title: str, dash_id, body: str):
    """Keep only real, populated docs: titled, not a template, with a link or body content."""
    if not title or title.strip() in ("", "(untitled)"):
        return "untitled"
    if TEMPLATE_RE.search(title):
        return "template"
    if dash_id is None and len(body.strip()) < 30:
        return "no content"
    return None
