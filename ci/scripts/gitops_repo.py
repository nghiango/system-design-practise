"""Git helpers shared by the topic publisher and the topic switch (stdlib only)."""
import os
import re
import subprocess
from pathlib import Path
from typing import List

TOPIC_RE = re.compile(r"^[a-z][a-z0-9-]{1,30}$")
BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")
JENKINS_IDENTITY = ["-c", "user.name=Jenkins local CI", "-c", "user.email=jenkins@localhost"]
KNOWN_HOSTS = Path(__file__).resolve().parents[1] / "jenkins" / "github_known_hosts"


def validate_topic(name: str) -> str:
    if not isinstance(name, str) or not TOPIC_RE.fullmatch(name):
        raise ValueError(f"invalid topic {name!r}")
    return name


def git_env() -> dict:
    """Pin GitHub host keys for SSH remotes unless the caller already configured SSH."""
    env = os.environ.copy()
    env.setdefault("GIT_TERMINAL_PROMPT", "0")
    if "GIT_SSH_COMMAND" not in env and KNOWN_HOSTS.is_file():
        env["GIT_SSH_COMMAND"] = (
            f"ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile={KNOWN_HOSTS}")
    return env


def run_git(cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(["git", *JENKINS_IDENTITY, *args], cwd=cwd, env=git_env(), text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and result.returncode:
        raise RuntimeError(f"git {' '.join(args)} failed:\n{result.stdout}")
    return result


def validate_branch(branch: str) -> str:
    if not isinstance(branch, str) or not BRANCH_RE.fullmatch(branch) or ".." in branch:
        raise ValueError(f"invalid branch {branch!r}")
    return branch


def clone(url: str, branch: str, dest: Path) -> Path:
    heads = run_git(dest.parent, "ls-remote", "--heads", url, validate_branch(branch)).stdout.strip()
    if heads:
        run_git(dest.parent, "clone", "--single-branch", "--branch", branch, url, str(dest))
    else:
        run_git(dest.parent, "clone", url, str(dest))
        run_git(dest, "checkout", "--orphan", branch)
    return dest


def commit_and_push(repo: Path, paths: List[str], message: str, branch: str, retries: int = 3) -> bool:
    """Commit `paths` and push; on a rejected push, rebase onto the remote and retry."""
    validate_branch(branch)
    present = [p for p in paths if (repo / p).exists() or run_git(repo, "ls-files", "--", p).stdout.strip()]
    if not present:
        return False
    run_git(repo, "add", "-A", "--", *present)
    if run_git(repo, "diff", "--cached", "--quiet", check=False).returncode == 0:
        return False
    run_git(repo, "commit", "-m", message)
    for attempt in range(retries + 1):
        push = run_git(repo, "push", "origin", f"HEAD:{branch}", check=False)
        if push.returncode == 0:
            return True
        if attempt == retries:
            raise RuntimeError(f"push failed after {retries} retries:\n{push.stdout}")
        # Rebase only when the remote branch exists; an orphan first push has no merge base.
        if run_git(repo, "ls-remote", "--heads", "origin", branch).stdout.strip():
            run_git(repo, "pull", "--rebase", "origin", branch, check=False)
    return False
