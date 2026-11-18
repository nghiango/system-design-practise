#!/usr/bin/env python3
"""Choose which single topic is online by writing topics/active.yaml in the GitOps repo."""
import argparse
import re
import sys
import tempfile
from pathlib import Path
from typing import Optional

from gitops_repo import clone, commit_and_push, validate_topic

ACTIVE_FILE = "topics/active.yaml"
HEADER = '# Exactly one topic, or "none". Changed by the Jenkins topic-switch job (or by hand).\n'
NONE = "none"


class SwitchError(Exception):
    pass


def read_active(gitops: Path) -> str:
    path = gitops / ACTIVE_FILE
    if not path.is_file():
        return NONE
    match = re.search(r"^active:\s*\"?([a-z0-9-]+)\"?\s*$", path.read_text(), re.MULTILINE)
    return match.group(1) if match else NONE


def _write_active(gitops: Path, value: str) -> None:
    path = gitops / ACTIVE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{HEADER}active: {value}\n")


def _published(topic_dir: Path) -> bool:
    return (topic_dir / "chart" / "Chart.yaml").is_file() and (topic_dir / "values-local.yaml").is_file()


def _revision(topic_dir: Path) -> str:
    path = topic_dir / "values-local.yaml"
    match = re.search(r'^revision:\s*"?([0-9a-f]{40})"?', path.read_text(), re.MULTILINE) if path.is_file() else None
    return match.group(1) if match else "-"


def activate(gitops: Path, topic: str) -> Optional[str]:
    validate_topic(topic)
    if topic == NONE:
        raise SwitchError('"none" is not a topic; use deactivate')
    topic_dir = gitops / "topics" / topic
    if not _published(topic_dir):
        raise SwitchError(f"topic {topic} has never been published; run system-design/{topic} first")
    previous = read_active(gitops)
    if previous == topic:
        return None
    _write_active(gitops, topic)
    return f"Activate topic {topic} (was {previous})"


def deactivate(gitops: Path) -> Optional[str]:
    previous = read_active(gitops)
    if previous == NONE:
        return None
    _write_active(gitops, NONE)
    return f"Deactivate topic {previous}"


def status(gitops: Path) -> str:
    lines = [f"Active: {read_active(gitops)}"]
    for topic_dir in sorted((gitops / "topics").glob("*/topic.yaml")):
        base = topic_dir.parent
        published = "yes" if _published(base) else "no"
        lines.append(f"{base.name}  published={published}  revision={_revision(base)}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", required=True, choices=["activate", "deactivate", "status"])
    parser.add_argument("--topic", default="")
    parser.add_argument("--gitops-url", required=True)
    parser.add_argument("--gitops-branch", default="main")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="sd-switch-") as tmp:
        repo = clone(args.gitops_url, args.gitops_branch, Path(tmp) / "repo")
        if args.action == "status":
            print(status(repo))
            return 0
        try:
            message = activate(repo, args.topic) if args.action == "activate" else deactivate(repo)
        except (SwitchError, ValueError) as error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 1
        if message is None:
            print(f"Nothing to do; active topic is already {read_active(repo)}")
            return 0
        commit_and_push(repo, [ACTIVE_FILE], message, args.gitops_branch)
        print(message)
        print(status(repo))
    return 0


if __name__ == "__main__":
    sys.exit(main())
