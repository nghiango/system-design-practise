import unittest

from chart_docs import CHART, FIXTURE, ROOT, by_name, helm_lint, kinds, one, render, render_error

SHA = "a" * 40


class TopicChartTest(unittest.TestCase):
    def test_every_resource_labelled_with_topic(self):
        for d in render(FIXTURE):
            self.assertIn("system-design/topic: sample", d.text, f"{d.kind}/{d.name}")

    def test_namespace_is_topic_prefixed_and_first(self):
        ns = one(render(FIXTURE), "Namespace")
        self.assertEqual((ns.name, ns.wave), ("topic-sample", -3))

    def test_invalid_topic_slug_fails(self):
        self.assertIn("invalid topic", render_error(FIXTURE, "--set", "topic=Bad_Name"))

    def test_kafka_topic_without_prefix_fails(self):
        self.assertIn("must start with 'sample.'",
                      render_error(FIXTURE, "--set", "kafka.topics[0].name=other.x"))

    def test_shared_policies_live_in_hyperscale(self):
        p = by_name(render(FIXTURE), "topic-sample-allow-shared-infra")
        self.assertEqual(p.namespace, "hyperscale")
        self.assertIn("kubernetes.io/metadata.name: topic-sample", p.text)
        for port in ("5432", "9092", "6379"):
            self.assertIn(f"port: {port}", p.text)

    def test_hook_order(self):
        docs = render(FIXTURE)
        self.assertEqual([by_name(docs, n, "Job").wave for n in ("wait-for-exclusive", "db-init", "kafka-topics")],
                         [-1, 0, 0])
        self.assertEqual(one(docs, "Deployment").wave, 1)

    def test_wait_job_excludes_own_namespace(self):
        self.assertIn("labelSelector=system-design/topic,system-design/topic!=sample",
                      by_name(render(FIXTURE), "wait-for-exclusive", "Job").text)

    def test_wait_job_reports_blocking_namespaces(self):
        t = by_name(render(FIXTURE), "wait-for-exclusive", "Job").text
        self.assertIn("still online:", t)
        self.assertIn("300", t)

    def test_database_name_underscored(self):
        docs = render(FIXTURE, "--set", "topic=url-shortener", "--set", "kafka.topics=null")
        self.assertIn('CREATE DATABASE "url_shortener"', by_name(docs, "db-init").text)

    def test_no_services_renders_no_deployments(self):
        self.assertEqual(kinds(render(FIXTURE, "--set", "services=null")).count("Deployment"), 0)

    def test_service_image_and_env(self):
        t = one(render(FIXTURE, "--set", "imagePrefix=reg/sd", "--set", "imageTag=" + SHA), "Deployment").text
        self.assertIn(f"image: \"reg/sd/sample-api:{SHA}\"", t)
        for env in ("SD_REDIS_KEY_PREFIX", "SD_KAFKA_GROUP_PREFIX", "SPRING_DATASOURCE_URL"):
            self.assertIn(env, t)

    def test_url_shortener_values_lint(self):
        self.assertEqual(helm_lint(CHART, ROOT / "topics/url-shortener/deploy/values.yaml"), 0)


if __name__ == "__main__":
    unittest.main()
