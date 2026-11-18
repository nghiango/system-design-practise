"""Render the topic chart with `helm template` and split it into documents.

Standard library only: the Jenkins agent has no PyYAML, so documents are
inspected with line regexes rather than parsed.
"""
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parents[3]
CHART = ROOT / "deploy" / "topic-chart"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "sample-values.yaml"


@dataclass
class Doc:
    kind: str
    name: str
    namespace: Optional[str]
    wave: Optional[int]
    text: str


def _field(pattern: str, text: str) -> Optional[str]:
    match = re.search(pattern, text, re.MULTILINE)
    return match.group(1).strip().strip('"') if match else None


def _parse(text: str) -> Doc:
    wave = _field(r'^\s+argocd\.argoproj\.io/sync-wave:\s*"?(-?\d+)"?', text)
    return Doc(
        kind=_field(r"^kind:\s*(\S+)", text) or "",
        name=_field(r"^metadata:\n(?:\s+.*\n)*?  name:\s*(\S+)", text) or "",
        namespace=_field(r"^metadata:\n(?:\s+.*\n)*?  namespace:\s*(\S+)", text),
        wave=int(wave) if wave is not None else None,
        text=text,
    )


def _helm(values_file: Path, *set_args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["helm", "template", "topic", str(CHART), "-f", str(values_file), *set_args],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def render(values_file: Path = FIXTURE, *set_args: str) -> List[Doc]:
    result = _helm(values_file, *set_args)
    if result.returncode:
        raise AssertionError(result.stderr)
    parts = re.split(r"^---\s*$", result.stdout, flags=re.MULTILINE)
    return [_parse(p) for p in parts if re.search(r"^kind:", p, re.MULTILINE)]


def render_error(values_file: Path = FIXTURE, *set_args: str) -> str:
    result = _helm(values_file, *set_args)
    if result.returncode == 0:
        raise AssertionError("expected helm template to fail")
    return result.stderr


def helm_lint(chart: Path, values_file: Path) -> int:
    return subprocess.run(["helm", "lint", str(chart), "-f", str(values_file)],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode


def kinds(docs: List[Doc]) -> List[str]:
    return [d.kind for d in docs]


def one(docs: List[Doc], kind: str) -> Doc:
    found = [d for d in docs if d.kind == kind]
    if len(found) != 1:
        raise AssertionError(f"expected one {kind}, found {len(found)}")
    return found[0]


def by_name(docs: List[Doc], name: str, kind: Optional[str] = None) -> Doc:
    found = [d for d in docs if d.name == name and (kind is None or d.kind == kind)]
    if len(found) != 1:
        raise AssertionError(f"expected one resource named {name}, found {len(found)}")
    return found[0]
