import tempfile
import unittest
from pathlib import Path

from helpers import ROOT, SHA

from publish_topic import publish
from topic_switch import SwitchError, activate, deactivate, read_active, status

HEADER = '# Exactly one topic, or "none". Changed by the Jenkins topic-switch job (or by hand).\n'


def publish_fixture(gitops: Path, topic: str) -> None:
    """A published topic: chart/Chart.yaml + values-local.yaml (+ topic.yaml)."""
    if topic == "url-shortener":
        publish(gitops, ROOT, topic, "reg/sd", SHA)
        return
    base = gitops / "topics" / topic
    (base / "chart").mkdir(parents=True)
    (base / "chart" / "Chart.yaml").write_text("name: topic-chart\n")
    (base / "topic.yaml").write_text(f"name: {topic}\n")
    (base / "values-local.yaml").write_text(f'revision: "{SHA}"\n')


class TopicSwitchTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.gitops = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def active_file(self) -> Path:
        return self.gitops / "topics" / "active.yaml"

    def test_missing_active_file_means_none(self):
        self.assertEqual(read_active(self.gitops), "none")

    def test_malformed_active_file_fails_loudly(self):
        self.active_file().parent.mkdir(parents=True, exist_ok=True)
        self.active_file().write_text("active: foo # extra junk\n")
        with self.assertRaises(SwitchError):
            read_active(self.gitops)

    def test_activate_from_none(self):
        publish_fixture(self.gitops, "url-shortener")
        self.assertEqual(activate(self.gitops, "url-shortener"), "Activate topic url-shortener (was none)")
        self.assertEqual(read_active(self.gitops), "url-shortener")
        self.assertEqual(self.active_file().read_text(), HEADER + "active: url-shortener\n")

    def test_activate_switches_previous_off(self):
        publish_fixture(self.gitops, "a-topic")
        publish_fixture(self.gitops, "b-topic")
        activate(self.gitops, "a-topic")
        self.assertEqual(activate(self.gitops, "b-topic"), "Activate topic b-topic (was a-topic)")
        self.assertEqual(read_active(self.gitops), "b-topic")

    def test_activate_same_topic_is_noop(self):
        publish_fixture(self.gitops, "url-shortener")
        activate(self.gitops, "url-shortener")
        before = self.active_file().read_text()
        self.assertIsNone(activate(self.gitops, "url-shortener"))
        self.assertEqual(self.active_file().read_text(), before)

    def test_activate_unpublished_topic_fails(self):
        (self.gitops / "topics" / "new-topic").mkdir(parents=True)
        (self.gitops / "topics" / "new-topic" / "topic.yaml").write_text("name: new-topic\n")
        with self.assertRaisesRegex(SwitchError, "run system-design/new-topic first"):
            activate(self.gitops, "new-topic")
        self.assertEqual(read_active(self.gitops), "none")
        self.assertFalse(self.active_file().exists())

    def test_activate_topic_without_gitops_folder_says_publish_first(self):
        with self.assertRaisesRegex(SwitchError, "run system-design/brand-new first"):
            activate(self.gitops, "brand-new")
        self.assertFalse(self.active_file().exists())

    def test_switch_rejects_unknown_topic(self):
        for bad in ("../charts", "nope", "none"):
            with self.assertRaises((SwitchError, ValueError)):
                activate(self.gitops, bad)

    def test_deactivate(self):
        publish_fixture(self.gitops, "url-shortener")
        activate(self.gitops, "url-shortener")
        self.assertEqual(deactivate(self.gitops), "Deactivate topic url-shortener")
        self.assertEqual(self.active_file().read_text(), HEADER + "active: none\n")
        self.assertIsNone(deactivate(self.gitops))

    def test_status_lists_every_topic(self):
        publish_fixture(self.gitops, "url-shortener")
        (self.gitops / "topics" / "new-topic").mkdir(parents=True)
        (self.gitops / "topics" / "new-topic" / "topic.yaml").write_text("name: new-topic\n")
        activate(self.gitops, "url-shortener")
        text = status(self.gitops)
        self.assertIn("Active: url-shortener", text)
        self.assertIn(f"url-shortener  published=yes  revision={SHA}", text)
        self.assertIn("new-topic  published=no  revision=-", text)


if __name__ == "__main__":
    unittest.main()
