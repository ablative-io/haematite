#!/usr/bin/env python3
"""Assemble a stacked-dev input JSON from the design docs.

Usage:
    ./scripts/render-input.py CORE-001
    ./scripts/render-input.py CORE-001 BRANCH-001 API-001
    ./scripts/render-input.py --all

Writes assembled input alongside the brief: docs/design/<cluster>/briefs/<id>-input.json
"""

import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DESIGN_DIR = os.path.join(REPO_ROOT, "docs", "design")
DECISIONS_PATH = os.path.join(DESIGN_DIR, "decisions.json")

CLUSTER_MAP = {
    "CORE": "core",
    "BRANCH": "branching",
    "DIST": "distribution",
    "PERSIST": "persistence",
    "API": "api",
    "WASM": "wasm",
}


def resolve_cluster(brief_id):
    prefix = brief_id.split("-")[0]
    cluster = CLUSTER_MAP.get(prefix)
    if not cluster:
        for entry in os.listdir(DESIGN_DIR):
            briefs_dir = os.path.join(DESIGN_DIR, entry, "briefs")
            if os.path.isfile(os.path.join(briefs_dir, f"{brief_id}.json")):
                return entry
        print(f"error: cannot find cluster for brief {brief_id}", file=sys.stderr)
        sys.exit(1)
    return cluster


def load_json(path):
    with open(path) as f:
        return json.load(f)


def assemble(brief_id):
    cluster = resolve_cluster(brief_id)
    cluster_dir = os.path.join(DESIGN_DIR, cluster)

    brief = load_json(os.path.join(cluster_dir, "briefs", f"{brief_id}.json"))
    design = load_json(os.path.join(cluster_dir, "design.json"))
    checklist_raw = load_json(os.path.join(cluster_dir, "checklist.json"))
    stories_raw = load_json(os.path.join(cluster_dir, "stories.json"))
    decisions = load_json(DECISIONS_PATH)

    all_checklist = []
    for section in checklist_raw.get("sections", []):
        for item in section.get("items", []):
            all_checklist.append({"id": item["id"], "text": item["text"]})

    all_stories = []
    for persona in stories_raw.get("personas", []):
        for story in persona.get("stories", []):
            all_stories.append({"id": story["id"], "text": story["text"]})

    brief_adr_ids = set(brief.get("design_anchor", []))
    adrs = []
    for d in decisions.get("decisions", []):
        if d["id"] in brief_adr_ids:
            adrs.append({
                "id": d["id"],
                "title": d["title"],
                "decision": d["decision"],
                "quote": d.get("quote", ""),
                "decided_by": d.get("decided_by", "Tom"),
            })

    brief_checklist_ids = set(brief.get("checklist", []))
    brief_story_ids = set(brief.get("stories", []))

    return {
        "repo_root": REPO_ROOT,
        "brief_id": brief_id,
        "reviewers": ["Danger Mouse"],
        "base_ref": "main",
        "placement": "local",
        "isolation": "worktree",
        "brief_document": brief,
        "resolved_context": {
            "adrs": adrs,
            "checklist": [c for c in all_checklist if c["id"] in brief_checklist_ids],
            "stories": [s for s in all_stories if s["id"] in brief_story_ids],
            "constraints": [{"id": c["id"], "text": c["text"]} for c in design.get("constraints", [])],
            "intention": design.get("intention", ""),
            "design_path": f"docs/design/{cluster}",
            "provenance": {
                "requested_by": "Tom",
                "quote": design.get("intention", "")[:200],
            },
        },
        "verify_fix_cap": 3,
        "review_cap": 1,
        "round_backoff_ms": 2000,
        "review_deadline_ms": 300000,
    }


def find_all_briefs():
    briefs = []
    for cluster in os.listdir(DESIGN_DIR):
        briefs_dir = os.path.join(DESIGN_DIR, cluster, "briefs")
        if not os.path.isdir(briefs_dir):
            continue
        for f in sorted(os.listdir(briefs_dir)):
            if f.endswith(".json") and not f.endswith("-input.json"):
                briefs.append(f.replace(".json", ""))
    return briefs


def main():
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <brief-id> [brief-id ...] | --all", file=sys.stderr)
        sys.exit(1)

    if sys.argv[1] == "--all":
        brief_ids = find_all_briefs()
    else:
        brief_ids = sys.argv[1:]

    for brief_id in brief_ids:
        try:
            cluster = resolve_cluster(brief_id)
            input_obj = assemble(brief_id)
        except FileNotFoundError as e:
            print(f"[{brief_id}] missing file: {e}", file=sys.stderr)
            continue
        except (KeyError, json.JSONDecodeError) as e:
            print(f"[{brief_id}] bad data: {e}", file=sys.stderr)
            continue

        out_path = os.path.join(DESIGN_DIR, cluster, "briefs", f"{brief_id}-input.json")
        with open(out_path, "w") as f:
            json.dump(input_obj, f, indent=2)
            f.write("\n")

        print(f"{out_path}")


if __name__ == "__main__":
    main()
