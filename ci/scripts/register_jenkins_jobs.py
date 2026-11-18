#!/usr/bin/env python3
"""Create/refresh the Jenkins folder `system-design/` with one job per topic plus `topic-switch`.

Idempotent; run again after adding a topic. Credentials: JENKINS_USER/JENKINS_TOKEN,
or (like hyper-scale-commerce/local-ci/scripts/configure-jenkins.py) the admin password
read from the local Jenkins container. Never prints secrets.
"""
import base64
import http.cookiejar
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import List, Tuple

from gitops_repo import validate_topic

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / "ci" / "jenkins" / "register-jobs.groovy"
DEFAULT_SOURCE_URL = "https://github.com/nghiango/system-design-practise.git"
SAFE_URL = re.compile(r"^[A-Za-z0-9@:/._~+-]+$")


def discover_topics(root: Path) -> List[str]:
    return sorted(validate_topic(p.parent.name) for p in (root / "topics").glob("*/topic.yaml"))


def render_script(template: str, topics: List[str], source_url: str) -> str:
    if not SAFE_URL.fullmatch(source_url):
        raise ValueError(f"unsafe source URL {source_url!r}")
    groovy_list = "[" + ", ".join(f"'{validate_topic(t)}'" for t in topics) + "]"
    return template.replace("__TOPICS__", groovy_list).replace("__SOURCE_URL__", source_url)


def credentials() -> Tuple[str, str]:
    user, token = os.environ.get("JENKINS_USER"), os.environ.get("JENKINS_TOKEN")
    if user and token:
        return user, token
    container = os.environ.get("JENKINS_CONTAINER", "hyperscale-local-ci-jenkins-1")
    result = subprocess.run(["docker", "exec", container, "cat", "/var/jenkins_home/secrets/initialAdminPassword"],
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    if result.returncode or not result.stdout.strip():
        raise SystemExit("Set JENKINS_USER and JENKINS_TOKEN, or run the local Jenkins container "
                         f"({container}) so its admin password can be read.")
    return "admin", result.stdout.strip()


def post_script(base_url: str, script: str) -> str:
    user, token = credentials()
    headers = {"Authorization": "Basic " + base64.b64encode(f"{user}:{token}".encode()).decode()}
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    with opener.open(urllib.request.Request(f"{base_url}/crumbIssuer/api/json", headers=headers), timeout=30) as r:
        crumb = json.load(r)
    headers[crumb["crumbRequestField"]] = crumb["crumb"]
    body = urllib.parse.urlencode({"script": script}).encode()
    with opener.open(urllib.request.Request(f"{base_url}/scriptText", data=body, headers=headers), timeout=60) as r:
        return r.read().decode()


def main() -> int:
    base_url = os.environ.get("JENKINS_URL", "http://localhost:8090").rstrip("/")
    source_url = os.environ.get("SD_SOURCE_URL", DEFAULT_SOURCE_URL)
    script = render_script(TEMPLATE.read_text(), discover_topics(ROOT), source_url)
    output = post_script(base_url, script).strip()
    try:
        print(json.dumps(json.loads(output.splitlines()[-1]), indent=2))
    except (ValueError, IndexError):
        print(output, file=sys.stderr)
        raise SystemExit("Jenkins did not return the expected JSON; see output above.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
