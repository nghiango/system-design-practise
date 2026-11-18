import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TOPIC_SH = ROOT / "scripts" / "topic.sh"

# Fake kubectl: records argv, answers `get ns` / `get pod` like a cluster would.
FAKE_KUBECTL = """#!/usr/bin/env bash
echo "$*" >> "$FAKE_LOG"
case "$*" in
  *"get ns topic-"*) [[ "$FAKE_NS_EXISTS" == "1" ]] && exit 0 || exit 1 ;;
  *"get pod -l app.kubernetes.io/component=postgres-ha,role=primary -o name"*) echo "pod/postgres-ha-1" ;;
esac
exit 0
"""


class TopicShTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.log = tmp / "kubectl.log"
        self.log.write_text("")
        self.kubectl = tmp / "kubectl"
        self.kubectl.write_text(FAKE_KUBECTL)
        self.kubectl.chmod(self.kubectl.stat().st_mode | stat.S_IEXEC)

    def tearDown(self):
        self._tmp.cleanup()

    def run_topic(self, *args, ns_exists=False, stdin=""):
        env = dict(os.environ, KUBECTL=str(self.kubectl), FAKE_LOG=str(self.log),
                   FAKE_NS_EXISTS="1" if ns_exists else "0")
        return subprocess.run(["bash", str(TOPIC_SH), *args], input=stdin, env=env, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def logged(self) -> str:
        return self.log.read_text()

    def test_port_forward_command(self):
        r = self.run_topic("port-forward", "url-shortener", "link-service", "8081")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("-n topic-url-shortener port-forward --address 127.0.0.1 svc/link-service 8081:8080",
                      self.logged())

    def test_logs_command(self):
        self.run_topic("logs", "url-shortener", "link-service")
        self.assertIn("-n topic-url-shortener logs -l app.kubernetes.io/name=link-service --tail=200 -f",
                      self.logged())

    def test_status_command(self):
        self.run_topic("status")
        self.assertIn("-n argocd get applications -l system-design/topic", self.logged())
        self.assertIn("get ns -l system-design/topic", self.logged())

    def test_reset_refuses_active_topic(self):
        r = self.run_topic("reset", "url-shortener", ns_exists=True, stdin="url-shortener\n")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("is online; deactivate it first", r.stderr)
        self.assertNotIn("DROP DATABASE", self.logged())

    def test_reset_requires_typed_confirmation(self):
        r = self.run_topic("reset", "url-shortener", stdin="yes\n")
        self.assertNotEqual(r.returncode, 0)
        self.assertNotIn("DROP DATABASE", self.logged())

    def test_reset_drops_db_kafka_topics_and_redis_keys(self):
        r = self.run_topic("reset", "url-shortener", stdin="url-shortener\n")
        self.assertEqual(r.returncode, 0, r.stderr)
        log = self.logged()
        self.assertIn('-n hyperscale exec pod/postgres-ha-1 -c postgres-patroni', log)
        self.assertIn('DROP DATABASE IF EXISTS "url_shortener"', log)
        self.assertIn("--delete --if-exists --topic url-shortener\\..*", log)
        redis = [line for line in log.splitlines() if "exec redis-0" in line]
        self.assertEqual(len(redis), 1, log)
        self.assertTrue(redis[0].endswith(' _ url-shortener:*'), redis[0])

    def test_rejects_bad_slug(self):
        for bad in ("../x", "URL", "x"):
            self.assertEqual(self.run_topic("port-forward", bad, "svc", "8081").returncode, 2, bad)
        self.assertEqual(self.logged(), "")

    def test_unknown_command_shows_usage(self):
        r = self.run_topic("nope")
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage", r.stderr.lower())


if __name__ == "__main__":
    unittest.main()
