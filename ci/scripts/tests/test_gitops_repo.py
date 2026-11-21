import tempfile
import unittest
from pathlib import Path

from helpers import clone_of, git, log_subjects, make_remote

from gitops_repo import commit_and_push, validate_branch, validate_topic


class GitopsRepoTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.remote = make_remote(self.tmp)

    def test_validate_branch(self):
        self.assertEqual(validate_branch("main"), "main")
        self.assertEqual(validate_branch("release/2026-11"), "release/2026-11")
        for bad in ("--upload-pack=/bin/sh", "-b", "..", "a..b", "sp ace"):
            with self.assertRaises(ValueError):
                validate_branch(bad)

    def tearDown(self):
        self._tmp.cleanup()

    def test_commit_and_push_returns_false_when_clean(self):
        clone = clone_of(self.remote, self.tmp / "a")
        self.assertFalse(commit_and_push(clone, ["topics"], "nothing", "main"))
        self.assertEqual(log_subjects(self.remote), ["init"])

    def test_commit_and_push_pushes_new_commit(self):
        clone = clone_of(self.remote, self.tmp / "a")
        (clone / "topics").mkdir()
        (clone / "topics" / "a.txt").write_text("a\n")
        self.assertTrue(commit_and_push(clone, ["topics"], "A", "main"))
        self.assertEqual(log_subjects(self.remote), ["A", "init"])

    def test_push_retries_after_non_fast_forward(self):
        clone_a = clone_of(self.remote, self.tmp / "a")
        clone_b = clone_of(self.remote, self.tmp / "b")
        (clone_b / "b.txt").write_text("b\n")
        git(clone_b, "add", "b.txt")
        git(clone_b, "commit", "-m", "B")
        git(clone_b, "push", "origin", "main")
        (clone_a / "topics").mkdir()
        (clone_a / "topics" / "a.txt").write_text("a\n")
        self.assertTrue(commit_and_push(clone_a, ["topics/a.txt"], "A", "main"))
        self.assertEqual(log_subjects(self.remote)[:2], ["A", "B"])

    def test_push_gives_up_after_retries(self):
        clone = clone_of(self.remote, self.tmp / "a")
        git(clone, "remote", "set-url", "origin", str(self.tmp / "missing.git"))
        (clone / "x.txt").write_text("x\n")
        with self.assertRaises(RuntimeError):
            commit_and_push(clone, ["x.txt"], "X", "main", retries=1)

    def test_validate_topic(self):
        self.assertEqual(validate_topic("url-shortener"), "url-shortener")
        for bad in ("../charts", "URL_Shortener", "x", "-a", "a" * 32):
            with self.assertRaises(ValueError):
                validate_topic(bad)


if __name__ == "__main__":
    unittest.main()
