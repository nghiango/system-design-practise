"""Shared fixtures for CI script tests: temp git remotes, never the network."""
import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
ROOT = SCRIPTS.parents[1]
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

SHA = "0123456789abcdef0123456789abcdef01234567"
IDENTITY = ["-c", "user.name=Test", "-c", "user.email=test@localhost"]


def git(cwd: Path, *args: str) -> str:
    result = subprocess.run(["git", *IDENTITY, *args], cwd=cwd, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(result.stdout)
    return result.stdout.strip()


def make_remote(tmp: Path) -> Path:
    """Bare repo with one commit `init` on main."""
    remote = tmp / "remote.git"
    seed = tmp / "seed"
    git(tmp, "init", "--bare", "-b", "main", str(remote))
    git(tmp, "init", "-b", "main", str(seed))
    (seed / "README.md").write_text("gitops\n")
    git(seed, "add", "README.md")
    git(seed, "commit", "-m", "init")
    git(seed, "push", str(remote), "main")
    return remote


def clone_of(remote: Path, dest: Path) -> Path:
    git(remote.parent, "clone", "--branch", "main", str(remote), str(dest))
    return dest


def log_subjects(remote: Path) -> list:
    return git(remote, "log", "--format=%s", "main").splitlines()


def files_outside(base: Path, prefix: str) -> list:
    return sorted(str(p.relative_to(base)) for p in base.rglob("*")
                  if p.is_file() and ".git" not in p.parts
                  and not str(p.relative_to(base)).startswith(prefix))
