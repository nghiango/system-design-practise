import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import helpers  # noqa: F401  (adds ci/scripts to sys.path)

from register_jenkins_jobs import credentials, discover_topics, render_script


class RegisterJenkinsJobsTest(unittest.TestCase):
    def test_discover_topics_sorted_and_validated(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("b-x", "a-x"):
                (root / "topics" / name).mkdir(parents=True)
                (root / "topics" / name / "topic.yaml").write_text(f"name: {name}\n")
            (root / "topics" / "not-a-topic").mkdir()
            self.assertEqual(discover_topics(root), ["a-x", "b-x"])
            (root / "topics" / "BAD").mkdir()
            (root / "topics" / "BAD" / "topic.yaml").write_text("name: BAD\n")
            with self.assertRaises(ValueError):
                discover_topics(root)

    def test_discover_topics_in_this_repo(self):
        self.assertIn("url-shortener", discover_topics(helpers.ROOT))

    def test_render_script_injects_topics_and_url(self):
        s = render_script("t=__TOPICS__ u=__SOURCE_URL__", ["a-x", "b-x"], "https://x/y.git")
        self.assertEqual(s, "t=['a-x', 'b-x'] u=https://x/y.git")

    def test_render_script_rejects_unsafe_url(self):
        with self.assertRaises(ValueError):
            render_script("__SOURCE_URL__", ["a-x"], "https://x/y.git'; evil()")

    def test_credentials_from_environment(self):
        with mock.patch.dict(os.environ, {"JENKINS_USER": "u", "JENKINS_TOKEN": "t"}):
            self.assertEqual(credentials(), ("u", "t"))

    def test_credentials_missing_everywhere_fails_clearly(self):
        env = {k: v for k, v in os.environ.items() if k not in ("JENKINS_USER", "JENKINS_TOKEN")}
        env["JENKINS_CONTAINER"] = "sd-test-no-such-container"
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(SystemExit, "JENKINS_USER and JENKINS_TOKEN"):
                credentials()


if __name__ == "__main__":
    unittest.main()
