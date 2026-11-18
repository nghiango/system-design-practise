# Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared foundation that lets any topic be built, published and switched online/offline from Jenkins, with at most one topic online. It is proven end-to-end with the `url-shortener` topic's plumbing (namespace, network access, secrets, database, Kafka topics), without any services yet.

**Architecture:** A Gradle monorepo auto-discovers `topics/*/services/*`. A single generic Helm chart (`deploy/topic-chart`) plus each topic's `deploy/values.yaml` describes a topic. Jenkins jobs, one per topic, publish chart + values into `hyper-scale-commerce-gitops/topics/<topic>/`. A `topic-switch` job writes `topics/active.yaml`. An Argo ApplicationSet turns that one value into zero or one `topic-<topic>` Application. A one-time platform change makes Prometheus/Alloy watch `topic-*` namespaces.

**Tech Stack:** Gradle 9.6.1 (Kotlin DSL), Java 21, Spring Boot 4.0.0, Spotless 7.0.2, Helm (3.17 on the agent, 4.x locally), Argo CD 2.14 ApplicationSet, Jenkins declarative pipelines + Groovy script console, Python 3 stdlib only, bash.

**Spec:** `docs/superpowers/specs/2026-11-18-system-design-practise-design.md`

**Out of scope (Plan 2, URL Shortener services):** Java services, Dockerfiles, k6 load scenarios, `scripts/topic.sh load`, the topic README content beyond its skeleton. Plan 2 is written once this plan is verified.

## Global Constraints

- Topic slug regex: `^[a-z][a-z0-9-]{1,30}$`; every artefact follows the naming table in spec 5.1.
- Namespace `topic-<topic>`; label `system-design/topic: <topic>` on **every** rendered resource.
- Postgres database `<topic with - replaced by _>`; Kafka topics must start with `<topic>.`; Redis prefix `<topic>:`.
- Shared infra lives in namespace `hyperscale`: `postgres-ha-primary:5432` (user `hyperscale`, admin DB `hyperscale`, password at Vault `hyperscale/postgres` property `password`), `kafka:9092` (image `confluentinc/cp-kafka:7.7.0`, replication factor 3), `redis:6379` (password at Vault `hyperscale/redis` property `password`), `otel-collector:9411` / `:4317`, `ClusterSecretStore vault-backend`.
- Pod labels in `hyperscale` used by policies: `app.kubernetes.io/component` = `postgres-ha` | `kafka` | `redis` | `monitoring`; Prometheus pod `local-component: prometheus`.
- Image name: `<IMAGE_PREFIX>/<topic>-<service>:<40-char sha>`, `IMAGE_PREFIX` default `hyperscale-k8s-worker:30500/system-design`.
- Jenkins never writes `topics/active.yaml` from a topic job, and never touches `charts/` or `environments/` in the GitOps repo.
- Python scripts: stdlib only (the Jenkins agent has no PyYAML), runnable on Python 3.9.
- Charts must pass `helm lint` on Helm 3.17 and 4.x.
- Gradle wrapper `9.6.1`; Java toolchain 21 via `org.gradle.toolchains.foojay-resolver-convention` `1.0.0`.
- No secrets in Git. Jenkins admin credentials only via `JENKINS_USER` / `JENKINS_TOKEN` environment variables.
- Never push to any remote, run Jenkins jobs or apply to the cluster without asking the user first (Task 9).

## Review Focus

1. A→B switch while A's namespace is stuck `Terminating` → B's `wait-for-exclusive` job fails after `300` s and names the blocking namespace (Task 2: `test_wait_job_reports_blocking_namespaces`).
2. `activate` on a topic that was never published → fails with `run system-design/<topic> first` and makes no commit (Task 4: `test_activate_unpublished_topic_fails`).
3. Topic job and switch job push at the same time → the second push rebases and succeeds (Task 3: `test_push_retries_after_non_fast_forward`).
4. Topic argument `../charts`, `URL_Shortener` or an unknown topic → rejected before any file is written (Task 3: `test_publish_rejects_bad_topic_names`, Task 4: `test_switch_rejects_unknown_topic`).
5. `reset` run against the topic that is currently online → refused (Task 7: `test_reset_refuses_active_topic`).

---

## File Structure

`system-design-practise/`
```
.gitignore  AGENTS.md  README.md  PROGRESS.md
gradlew  gradlew.bat  gradle/wrapper/*            # copied from hyper-scale-commerce (9.6.1)
gradle/libs.versions.toml
settings.gradle.kts                               # discovers topics/* and topics/*/services/*
build.gradle.kts                                  # per-topic aggregate tasks
build-logic/                                      # included build, plugin id `sd.spring-service`
  settings.gradle.kts  build.gradle.kts
  src/main/kotlin/sd.spring-service.gradle.kts
  src/test/java/com/systemdesign/buildlogic/SpringServiceConventionTest.java
deploy/topic-chart/                               # the one generic Helm chart
  Chart.yaml  values.yaml
  templates/_helpers.tpl namespace.yaml networkpolicies-topic.yaml networkpolicies-shared.yaml
           external-secrets.yaml exclusivity.yaml db-init.yaml kafka-topics.yaml services.yaml
  tests/test_topic_chart.py  tests/fixtures/sample-values.yaml  tests/chart_docs.py
ci/
  Jenkinsfile  Jenkinsfile.switch
  jenkins/register-jobs.groovy
  scripts/gitops_repo.py publish_topic.py topic_switch.py register_jenkins_jobs.py build_images.sh
  scripts/tests/test_gitops_repo.py test_publish_topic.py test_topic_switch.py test_register_jenkins_jobs.py
scripts/topic.sh  scripts/tests/test_topic_sh.py
docs/topic-template.md
topics/url-shortener/
  topic.yaml  README.md  deploy/values.yaml
```

`hyper-scale-commerce-gitops/` (new files only, plus README section)
```
argocd/topics-applicationset.yaml
topics/README.md  topics/active.yaml
```

`hyper-scale-commerce/` (one-time platform change)
```
local-ci/platform/files/prometheus.yaml        # + job `topics`
local-ci/platform/logging/config.alloy         # + topic pod discovery
local-ci/platform/templates/monitoring.yaml    # + ClusterRole/Binding local-monitoring-topics
local-ci/platform/templates/logging.yaml       # + ClusterRole/Binding local-log-reader-topics
local-ci/platform/files/topics-dashboard.json  # generic dashboard with `topic` variable
```

---

### Task 1: Gradle monorepo, build convention, tracker and docs skeleton

**Files:**
- Create: `.gitignore`, `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar`, `gradle/wrapper/gradle-wrapper.properties` (copy from `../hyper-scale-commerce`), `gradle/libs.versions.toml`, `settings.gradle.kts`, `build.gradle.kts`, `build-logic/settings.gradle.kts`, `build-logic/build.gradle.kts`, `build-logic/src/main/kotlin/sd.spring-service.gradle.kts`
- Create: `topics/url-shortener/topic.yaml`, `topics/url-shortener/README.md`, `docs/topic-template.md`, `PROGRESS.md`, `README.md`, `AGENTS.md`
- Test: `build-logic/src/test/java/com/systemdesign/buildlogic/SpringServiceConventionTest.java`

**Interfaces:**
- Produces: plugin id `sd.spring-service`; tasks `integrationTest` (source set `integrationTest`, `useJUnitPlatform`, `shouldRunAfter(test)`), `spotlessCheck`, `bootJar`; per-topic aggregate tasks on project `:<topic>`: `topicTest`, `topicIntegrationTest`, `topicAssemble` (each depends on that task in every `:<topic>:<service>` subproject; zero subprojects is valid). Subprojects are named `:<topic>:<service>`, with projectDir `topics/<topic>/services/<service>`, included only when `build.gradle.kts` exists there. Project `:<topic>` is included for every `topics/<topic>/topic.yaml`.
- `topics/<topic>/topic.yaml` format (flat `key: value`, read by later tasks):
  ```yaml
  name: url-shortener
  title: URL Shortener
  focus: Capacity estimation, database sharding, caching, ID generation
  level: Foundational
  ```

- [ ] **Step 1: Write the failing TestKit test** `SpringServiceConventionTest`

```java
@Test void appliesJava21SpringBootSpotlessAndIntegrationTest(@TempDir Path dir) throws IOException {
  Files.writeString(dir.resolve("settings.gradle.kts"), "rootProject.name = \"sample\"");
  Files.writeString(dir.resolve("build.gradle.kts"), """
      plugins { id("sd.spring-service") }
      tasks.register("printRelease") { doLast { println("release=" + tasks.compileJava.get().options.release.get()) } }
      """);
  var result = GradleRunner.create().withProjectDir(dir.toFile()).withPluginClasspath()
      .withArguments("tasks", "--all", "printRelease").build();
  assertThat(result.getOutput()).contains("integrationTest", "spotlessCheck", "bootJar", "release=21");
}
```

- [ ] **Step 2: Run it and confirm failure**

Run: `./gradlew -p build-logic test`
Expected: FAIL with `Plugin [id: 'sd.spring-service'] was not found`.

- [ ] **Step 3: Implement `build-logic` and the convention plugin**

`build-logic/build.gradle.kts` uses `kotlin-dsl` and `java-gradle-plugin`, with `implementation` of `org.springframework.boot:spring-boot-gradle-plugin:4.0.0`, `io.spring.gradle:dependency-management-plugin:1.1.7`, `com.diffplug.spotless:spotless-plugin-gradle:7.0.2`, and test deps `gradleTestKit()`, JUnit 5, AssertJ. The plugin mirrors `hyper-scale-commerce/app/build.gradle.kts`: Java toolchain + `options.release` 21, UTF-8, Spotless `googleJavaFormat("1.24.0")`, `integrationTest` source set/configurations/task, `tasks.check { dependsOn("integrationTest") }`, deps `spring-boot-starter-actuator`, `micrometer-registry-prometheus`, `micrometer-tracing-bridge-brave`, `zipkin-reporter-brave`, `zipkin-sender-urlconnection`, `net.logstash.logback:logstash-logback-encoder:8.0`; test deps `spring-boot-starter-test`; integration deps `spring-boot-testcontainers`, `org.testcontainers:junit-jupiter:1.21.3`. Versions go in `gradle/libs.versions.toml`.

- [ ] **Step 4: Run the test and confirm it passes**

Run: `./gradlew -p build-logic test`
Expected: `BUILD SUCCESSFUL`, 1 test passed.

- [ ] **Step 5: Root build with topic discovery**

`settings.gradle.kts`: `pluginManagement { includeBuild("build-logic") }`, the foojay plugin, `rootProject.name = "system-design-practise"`, plus the discovery loop over `topics/*` described under Interfaces. `build.gradle.kts` registers `topicTest`, `topicIntegrationTest` and `topicAssemble` on each `:<topic>` project.

Run: `./gradlew :url-shortener:topicTest :url-shortener:topicAssemble`
Expected: `BUILD SUCCESSFUL` (no services yet).

- [ ] **Step 6: Docs skeleton and tracker**

- `docs/topic-template.md`: the 10 sections of spec 5.5, each with one line saying what goes there.
- `topics/url-shortener/README.md`: copy of the template with the title and `Status: foundation only`.
- `PROGRESS.md`: the table from spec 5.6 with rows Foundation, URL Shortener, Flash Sale, Notification System, Ticket Booking, Banking Money Transfer, Real-Time Auction. Under it, the Foundation checklist = Tasks 1–9 of this plan, a `Next step:` line, and a `Log` section (`YYYY-MM-DD — what changed`).
- `README.md`: purpose, the naming table (spec 5.1), how to build/test, links to PROGRESS and the spec.
- `AGENTS.md`: build/test/verify commands from this plan and the rule "update PROGRESS.md in the same commit as the work".

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "Add Gradle monorepo, sd.spring-service convention and progress tracker"
```

---

### Task 2: Generic topic Helm chart

**Files:**
- Create: `deploy/topic-chart/Chart.yaml` (`name: topic-chart`, `version: 0.1.0`), `values.yaml`, `templates/*` (see File Structure)
- Create: `topics/url-shortener/deploy/values.yaml`
- Test: `deploy/topic-chart/tests/test_topic_chart.py`, `tests/chart_docs.py`, `tests/fixtures/sample-values.yaml`

**Interfaces:**
- Consumes: none.
- Produces: values contract (Tasks 3 and 9 rely on it):
  ```yaml
  topic: ""                     # required slug
  sharedNamespace: hyperscale
  revision: ""                  # set by values-local.yaml
  imagePrefix: ""               # set by values-local.yaml
  imageTag: ""                  # set by values-local.yaml
  imagePullPolicy: Always
  postgres: {host: postgres-ha-primary.hyperscale.svc, port: 5432, adminUser: hyperscale, adminDatabase: hyperscale, clientImage: "postgres:17-alpine"}
  database: {enabled: true}     # name is derived: topic with - → _
  kafka: {bootstrap: kafka.hyperscale.svc:9092, image: "confluentinc/cp-kafka:7.7.0", replicationFactor: 3, topics: []}  # [{name, partitions}]
  redis: {enabled: true}
  tracing: {endpoint: http://otel-collector.hyperscale.svc:9411/api/v2/spans}
  exclusivity: {image: "curlimages/curl:8.10.1", timeoutSeconds: 300}
  services: []                  # [{name, port: 8080, replicas: 1, resources: {...}, env: {}}]
  ```
- Produces: env vars injected into every service container: `SD_TOPIC`, `SPRING_DATASOURCE_URL=jdbc:postgresql://<host>:<port>/<db>`, `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD` (from Secret `postgres-credentials`/`password`), `SPRING_DATA_REDIS_HOST=redis.hyperscale.svc`, `SPRING_DATA_REDIS_PASSWORD` (Secret `redis-secret`/`redis-password`), `SD_REDIS_KEY_PREFIX=<topic>:`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `SD_KAFKA_GROUP_PREFIX=<topic>.`, `MANAGEMENT_ZIPKIN_TRACING_ENDPOINT`.
- Produces: ordering via `argocd.argoproj.io/sync-wave`: Namespace `-3`; SA/ClusterRole/ClusterRoleBinding/NetworkPolicies/ExternalSecrets `-2`; Job `wait-for-exclusive` (`hook: Sync`, `hook-delete-policy: BeforeHookCreation`) `-1`; Jobs `db-init` and `kafka-topics` (same hook annotations) `0`; Deployments/Services `1`. Hooks are Sync-phase, not PreSync, because PreSync runs before the chart's Namespace exists.
- `tests/chart_docs.py` produces `render(values_file: Path, *set_args: str) -> list[Doc]` and `render_error(values_file, *set_args) -> str`. `Doc` has `kind`, `name`, `namespace`, `wave: int | None`, `text`. It parses `helm template` output by splitting on `^---$` and using line regexes, because PyYAML is not available.

- [ ] **Step 1: Write the failing tests** (fixture topic `sample`, one Kafka topic `sample.thing.created`, one service `api`)

```python
def test_every_resource_labelled_with_topic(self):
    for d in render(FIXTURE): self.assertIn("system-design/topic: sample", d.text, f"{d.kind}/{d.name}")
def test_namespace_is_topic_prefixed_and_first(self):
    ns = one(render(FIXTURE), "Namespace"); self.assertEqual((ns.name, ns.wave), ("topic-sample", -3))
def test_invalid_topic_slug_fails(self):
    self.assertIn("invalid topic", render_error(FIXTURE, "--set", "topic=Bad_Name"))
def test_kafka_topic_without_prefix_fails(self):
    self.assertIn("must start with 'sample.'", render_error(FIXTURE, "--set", "kafka.topics[0].name=other.x"))
def test_shared_policies_live_in_hyperscale(self):
    p = by_name(render(FIXTURE), "topic-sample-allow-shared-infra")
    self.assertEqual(p.namespace, "hyperscale"); self.assertIn("kubernetes.io/metadata.name: topic-sample", p.text)
    for port in ("5432", "9092", "6379"): self.assertIn(f"port: {port}", p.text)
def test_hook_order(self):
    docs = render(FIXTURE)
    self.assertEqual([by_name(docs, n).wave for n in ("wait-for-exclusive", "db-init", "kafka-topics")], [-1, 0, 0])
    self.assertEqual(one(docs, "Deployment").wave, 1)
def test_wait_job_excludes_own_namespace(self):
    self.assertIn("labelSelector=system-design/topic,system-design/topic!=sample", by_name(render(FIXTURE), "wait-for-exclusive").text)
def test_wait_job_reports_blocking_namespaces(self):
    t = by_name(render(FIXTURE), "wait-for-exclusive").text
    self.assertIn("still online:", t); self.assertIn("300", t)
def test_database_name_underscored(self):
    self.assertIn('CREATE DATABASE "url_shortener"', by_name(render(FIXTURE, "--set", "topic=url-shortener", "--set", "kafka.topics=null"), "db-init").text)
def test_no_services_renders_no_deployments(self):
    self.assertEqual(kinds(render(FIXTURE, "--set", "services=null")).count("Deployment"), 0)
def test_service_image_and_env(self):
    t = one(render(FIXTURE, "--set", "imagePrefix=reg/sd", "--set", "imageTag=" + "a"*40), "Deployment").text
    self.assertIn("image: reg/sd/sample-api:" + "a"*40, t)
    for env in ("SD_REDIS_KEY_PREFIX", "SD_KAFKA_GROUP_PREFIX", "SPRING_DATASOURCE_URL"): self.assertIn(env, t)
def test_url_shortener_values_lint(self):
    self.assertEqual(helm_lint(ROOT / "deploy/topic-chart", ROOT / "topics/url-shortener/deploy/values.yaml"), 0)
```

- [ ] **Step 2: Run and confirm failure**

Run: `python3 -m unittest discover -s deploy/topic-chart/tests -v`
Expected: FAIL (`Error: ... Chart.yaml file is missing` or similar).

- [ ] **Step 3: Implement the chart**

- `_helpers.tpl` defines `topic.validate` (uses `fail` with `invalid topic "<x>"`, and `kafka topic "<n>" must start with '<topic>.'`), `topic.namespace`, `topic.labels` (`system-design/topic`, `app.kubernetes.io/part-of: system-design`, `app.kubernetes.io/managed-by: argocd`), `topic.dbName`.
- `networkpolicies-topic.yaml` (in `topic-<topic>`): `default-deny`; `allow-dns` to kube-dns 53; `allow-egress-shared-infra` to namespace `hyperscale` pods by component (`postgres-ha` 5432, `kafka` 9092, `redis` 6379, `monitoring` 9411/4317); `allow-kube-api-egress` 443/6443 for pods labelled `system-design/job: wait-for-exclusive`; `allow-intra-topic`; `allow-prometheus-scrape` ingress on 8080 from namespace `hyperscale` pods `local-component: prometheus`.
- `networkpolicies-shared.yaml` (in `hyperscale`): `topic-<topic>-allow-shared-infra` (ingress to postgres-ha/kafka/redis from namespace `topic-<topic>`), `topic-<topic>-allow-tracing` (ingress to `monitoring` 9411/4317), `topic-<topic>-allow-prometheus-egress` (egress from `local-component: prometheus` to namespace `topic-<topic>` port 8080).
- `external-secrets.yaml`: `postgres-credentials` (key `password` ← `hyperscale/postgres`.`password`) and `redis-secret` (key `redis-password` ← `hyperscale/redis`.`password`), store `ClusterSecretStore vault-backend`, `refreshInterval: 1m`.
- `exclusivity.yaml`: SA `wait-for-exclusive`, ClusterRole `topic-<topic>-namespace-reader` (`namespaces`: `list`), and a binding. The Job script loops until `timeoutSeconds`: it curls `https://kubernetes.default.svc/api/v1/namespaces?labelSelector=system-design/topic,system-design/topic!=<topic>` with the SA token/CA, succeeds when the response contains `"items":[]`, and otherwise prints `still online: <names>`, extracted with `grep -o '"name":"topic-[a-z0-9-]*"'`. On timeout it exits 1.
- `db-init.yaml`: `PGPASSWORD` from the secret; `psql -h … -U hyperscale -d hyperscale -tAc "SELECT 1 FROM pg_database WHERE datname='<db>'" | grep -q 1 || psql … -c 'CREATE DATABASE "<db>"'`.
- `kafka-topics.yaml`: `kafka-topics --bootstrap-server … --create --if-not-exists --topic <n> --partitions <p> --replication-factor 3` for each topic. The Job is skipped when the list is empty.
- `services.yaml`: Deployment + ClusterIP Service per entry, port name `http`; readiness `/actuator/health/readiness` and liveness `/actuator/health/liveness`; default requests `cpu: 100m, memory: 384Mi`, limits `memory: 768Mi`.
- `topics/url-shortener/deploy/values.yaml`: `topic: url-shortener`, `kafka.topics: [{name: url-shortener.link.clicked, partitions: 6}]`, `services: []`.

- [ ] **Step 4: Run and confirm pass**

Run: `python3 -m unittest discover -s deploy/topic-chart/tests -v`
Expected: all 12 tests `ok`.

- [ ] **Step 5: Commit**

```bash
git add deploy topics/url-shortener/deploy && git commit -m "Add generic topic Helm chart with exclusivity, DB and Kafka bootstrap"
```

---

### Task 3: GitOps publish script with safe push

**Files:**
- Create: `ci/scripts/gitops_repo.py`, `ci/scripts/publish_topic.py`
- Test: `ci/scripts/tests/test_gitops_repo.py`, `ci/scripts/tests/test_publish_topic.py`

**Interfaces:**
- Consumes: `deploy/topic-chart/` (Task 2), `topics/<topic>/topic.yaml` (Task 1), `topics/<topic>/deploy/values.yaml` (Task 2).
- Produces (`gitops_repo.py`):
  - `TOPIC_RE = re.compile(r"^[a-z][a-z0-9-]{1,30}$")`; `validate_topic(name: str) -> str`, which raises `ValueError(f"invalid topic {name!r}")`.
  - `clone(url: str, branch: str, dest: Path) -> Path`
  - `commit_and_push(repo: Path, paths: list[str], message: str, branch: str, retries: int = 3) -> bool`. It returns `False` when there's nothing to commit. On a rejected push it runs `git pull --rebase origin <branch>` and retries, raising `RuntimeError` after `retries` failures. Author: `-c user.name='Jenkins local CI' -c user.email='jenkins@localhost'`.
- Produces (`publish_topic.py`):
  - `publish(gitops: Path, source_root: Path, topic: str, image_prefix: str, revision: str) -> list[str]`. It returns the changed paths relative to `gitops`. It **replaces** `topics/<topic>/chart/` with `deploy/topic-chart/` plus `chart/values-topic.yaml` (a copy of the topic's `deploy/values.yaml`) plus any `topics/<topic>/deploy/templates/*.yaml` copied into `chart/templates/`. It writes `topics/<topic>/values-local.yaml` and copies `topics/<topic>/topic.yaml`. It writes nothing else.
  - `values-local.yaml` exact content:
    ```yaml
    # Generated by Jenkins (system-design/<topic>). Never put secrets here.
    revision: "<sha>"
    imagePrefix: "<prefix>"
    imageTag: "<sha>"
    ```
  - CLI: `publish_topic.py --topic T --image-prefix P --revision SHA --gitops-url URL --gitops-branch B`. It clones into a temp dir, calls `publish`, runs `helm lint topics/T/chart -f topics/T/chart/values-topic.yaml -f topics/T/values-local.yaml`, then calls `commit_and_push(..., f"Deploy topic {T} {SHA} (linux/arm64)")`. `revision` must match `^[0-9a-f]{40}$`.

- [ ] **Step 1: Write the failing tests** (temp bare remote + two clones; no network)

```python
def test_commit_and_push_returns_false_when_clean(self): ...  # assertFalse(commit_and_push(clone_a, ["topics"], "m", "main"))
def test_push_retries_after_non_fast_forward(self):
    # clone_b pushes first; clone_a's push must rebase then succeed
    self.assertTrue(commit_and_push(clone_a, ["topics/a.txt"], "A", "main"))
    self.assertEqual(git_log_subjects(remote)[:2], ["A", "B"])
def test_publish_writes_only_topic_folder(self):
    changed = publish(gitops, ROOT, "url-shortener", "reg/sd", SHA)
    self.assertTrue(all(p.startswith("topics/url-shortener/") for p in changed))
    self.assertEqual(sorted(untracked_outside(gitops, "topics/url-shortener")), [])
def test_values_local_exact_content(self):
    publish(...); self.assertEqual((gitops/"topics/url-shortener/values-local.yaml").read_text(), EXPECTED_VALUES_LOCAL)
def test_publish_replaces_stale_chart_files(self):
    (gitops/"topics/url-shortener/chart/templates/old.yaml").parent.mkdir(parents=True); ...write...
    publish(...); self.assertFalse((gitops/"topics/url-shortener/chart/templates/old.yaml").exists())
def test_publish_never_touches_active_yaml(self):
    (gitops/"topics/active.yaml").write_text("active: none\n"); publish(...)
    self.assertEqual((gitops/"topics/active.yaml").read_text(), "active: none\n")
def test_publish_rejects_bad_topic_names(self):
    for bad in ("../charts", "URL_Shortener", "x", "missing-topic"):
        with self.assertRaises(ValueError): publish(gitops, ROOT, bad, "reg/sd", SHA)
    self.assertEqual(list(gitops.rglob("*.yaml")), [])
def test_publish_rejects_short_revision(self): ...  # ValueError for "abc123"
```

- [ ] **Step 2: Run and confirm failure**

Run: `python3 -m unittest discover -s ci/scripts/tests -p 'test_gitops_repo.py' -v && python3 -m unittest discover -s ci/scripts/tests -p 'test_publish_topic.py' -v`
Expected: FAIL (`ModuleNotFoundError: gitops_repo`).

- [ ] **Step 3: Implement both modules** per Interfaces. An unknown topic (no `topics/<t>/topic.yaml` in `source_root`) raises `ValueError(f"unknown topic {t!r}")`.

- [ ] **Step 4: Run and confirm pass** — same command; all tests `ok`.

- [ ] **Step 5: Commit**

```bash
git add ci/scripts && git commit -m "Add topic GitOps publisher with rebase-and-retry push"
```

---

### Task 4: Topic switch script

**Files:**
- Create: `ci/scripts/topic_switch.py`
- Test: `ci/scripts/tests/test_topic_switch.py`

**Interfaces:**
- Consumes: `validate_topic`, `clone`, `commit_and_push` (Task 3).
- Produces:
  - `class SwitchError(Exception)`
  - `read_active(gitops: Path) -> str`: reads `topics/active.yaml` (`active: <x>`). A missing file counts as `none`.
  - `activate(gitops: Path, topic: str) -> str | None`: returns the commit message `Activate topic <t> (was <prev>)`, or `None` if `<t>` is already active.
  - `deactivate(gitops: Path) -> str | None`: returns `Deactivate topic <prev>`, or `None` if already `none`.
  - `status(gitops: Path) -> str`: prints `Active: <x>`, then one row per `topics/*/` directory with `topic.yaml`: `<topic>  published=<yes|no>  revision=<sha|->` (revision read from `values-local.yaml`).
  - File content written: `# Exactly one topic, or "none". Changed by the Jenkins topic-switch job (or by hand).\nactive: <x>\n`
  - CLI: `topic_switch.py --action {activate,deactivate,status} [--topic T] --gitops-url URL --gitops-branch B`. For `status` it makes no commit.

- [ ] **Step 1: Write the failing tests**

```python
def test_activate_from_none(self):
    publish_fixture(gitops, "url-shortener")
    self.assertEqual(activate(gitops, "url-shortener"), "Activate topic url-shortener (was none)")
    self.assertEqual(read_active(gitops), "url-shortener")
def test_activate_switches_previous_off(self):
    publish_fixture(gitops, "a-topic"); publish_fixture(gitops, "b-topic"); activate(gitops, "a-topic")
    self.assertEqual(activate(gitops, "b-topic"), "Activate topic b-topic (was a-topic)")
def test_activate_same_topic_is_noop(self): ...  # returns None, file unchanged
def test_activate_unpublished_topic_fails(self):
    (gitops/"topics/new-topic").mkdir(parents=True); (gitops/"topics/new-topic/topic.yaml").write_text("name: new-topic\n")
    with self.assertRaisesRegex(SwitchError, "run system-design/new-topic first"): activate(gitops, "new-topic")
    self.assertEqual(read_active(gitops), "none")
def test_switch_rejects_unknown_topic(self):
    for bad in ("../charts", "nope"):
        with self.assertRaises((SwitchError, ValueError)): activate(gitops, bad)
def test_deactivate(self): ...  # "Deactivate topic url-shortener", then None on second call
def test_status_lists_every_topic(self): ...  # contains "Active: url-shortener" and "url-shortener  published=yes  revision=<sha>"
```

- [ ] **Step 2: Run and confirm failure** — `python3 -m unittest discover -s ci/scripts/tests -p 'test_topic_switch.py' -v` → `ModuleNotFoundError`.
- [ ] **Step 3: Implement** per Interfaces. "Published" means both `chart/Chart.yaml` and `values-local.yaml` exist.
- [ ] **Step 4: Run and confirm pass.**
- [ ] **Step 5: Commit** — `git commit -m "Add topic-switch script enforcing a single active topic"`

---

### Task 5: Jenkins pipelines and job registration

**Files:**
- Create: `ci/Jenkinsfile`, `ci/Jenkinsfile.switch`, `ci/scripts/build_images.sh`, `ci/jenkins/register-jobs.groovy`, `ci/scripts/register_jenkins_jobs.py`
- Test: `ci/scripts/tests/test_register_jenkins_jobs.py`

**Interfaces:**
- Consumes: `publish_topic.py` CLI (Task 3), `topic_switch.py` CLI (Task 4), Gradle tasks `:<topic>:topicTest|topicIntegrationTest|topicAssemble` (Task 1).
- Produces:
  - `ci/Jenkinsfile`: agent `local-arm64`; `TOPIC = env.JOB_BASE_NAME`, checked against `topics/<TOPIC>/topic.yaml`; parameters `REGISTRY` (`hyperscale-k8s-worker:30500`), `IMAGE_PREFIX` (`hyperscale-k8s-worker:30500/system-design`), `GITOPS_URL` (`git@github.com:nghiango/hyper-scale-commerce-gitops.git`), `GITOPS_BRANCH` (`main`), `RUN_INTEGRATION_TESTS` (false), `DEPLOY_BRANCH` (`main`). Stages: Checkout → Unit tests (`./gradlew :$TOPIC:topicTest --no-daemon`, junit `allowEmptyResults: true`) → Integration tests (when param) → Assemble → Build images (`PUSH_IMAGES=false bash ci/scripts/build_images.sh $TOPIC`) → Publish images (deploy branch only) → Publish GitOps (`sshagent(['gitops-write'])` + `python3 ci/scripts/publish_topic.py …`). `disableConcurrentBuilds()`.
  - `ci/Jenkinsfile.switch`: **no `parameters {}` block**, because the job definition owns `ACTION`/`TOPIC` and a Jenkinsfile block would overwrite them. It runs `python3 ci/scripts/topic_switch.py --action $ACTION --topic $TOPIC …` under `sshagent(['gitops-write'])`.
  - `build_images.sh <topic>`: for each `topics/<topic>/services/*/Dockerfile`, runs `docker buildx build --builder hyperscale-local --platform linux/arm64 (--load|--push) --tag $IMAGE_PREFIX/<topic>-<service>:$REVISION topics/<topic>/services/<service>`. With zero services it prints `no services for <topic>` and exits 0. It reuses the existing `hyperscale-local` builder and creates it the same way as `hyper-scale-commerce/local-ci/scripts/publish.sh` if missing.
  - `register_jenkins_jobs.py`: `discover_topics(root: Path) -> list[str]` (sorted, validated) and `render_script(template: str, topics: list[str], source_url: str) -> str` (replaces `__TOPICS__` with a Groovy list literal and `__SOURCE_URL__`). Its `main()` posts to `${JENKINS_URL:-http://localhost:8090}/scriptText` with crumb, using `JENKINS_USER`/`JENKINS_TOKEN` and failing clearly if either is unset. Source URL default: `https://github.com/nghiango/system-design-practise.git`.
  - `register-jobs.groovy` (idempotent):
    - Creates folder `system-design` (`com.cloudbees.hudson.plugins.folder.Folder`).
    - Creates one `WorkflowJob` per topic with `CpsScmFlowDefinition(GitSCM(main) + PathRestriction(includedRegions: "topics/<t>/.*\nbuild-logic/.*\ngradle/.*\nsettings\\.gradle\\.kts\nbuild\\.gradle\\.kts\nci/.*\ndeploy/.*"), "ci/Jenkinsfile")` and `SCMTrigger('H/2 * * * *')`.
    - Creates job `topic-switch` with `ci/Jenkinsfile.switch`, no trigger, and `ParametersDefinitionProperty(ChoiceParameterDefinition('ACTION', ['status','activate','deactivate']), ChoiceParameterDefinition('TOPIC', topics))`.
    - Returns JSON `{"jobs": [...]}`.

- [ ] **Step 1: Write failing tests**

```python
def test_discover_topics_sorted_and_validated(self): ...  # tmp topics/b-x, topics/a-x, topics/BAD (no topic.yaml ignored; bad slug with topic.yaml raises ValueError)
def test_render_script_injects_topics_and_url(self):
    s = render_script("t=__TOPICS__ u=__SOURCE_URL__", ["a-x", "b-x"], "https://x/y.git")
    self.assertEqual(s, "t=['a-x', 'b-x'] u=https://x/y.git")
def test_main_requires_credentials(self): ...  # env without JENKINS_TOKEN → SystemExit with "JENKINS_USER and JENKINS_TOKEN"
```

- [ ] **Step 2: Run and confirm failure** — `python3 -m unittest discover -s ci/scripts/tests -p 'test_register_jenkins_jobs.py' -v`.
- [ ] **Step 3: Implement** all five files per Interfaces.
- [ ] **Step 4: Run and confirm pass**, then run the static checks:

Run: `bash -n ci/scripts/build_images.sh && REVISION=$(printf 'a%.0s' {1..40}) IMAGE_PREFIX=x/y PUSH_IMAGES=false bash ci/scripts/build_images.sh url-shortener`
Expected: prints `no services for url-shortener`, exit 0.

- [ ] **Step 5: Commit** — `git commit -m "Add per-topic and topic-switch Jenkins pipelines with job registration"`

---

### Task 6: GitOps repo — ApplicationSet and switch file

**Repo:** `/Users/ngng/working/projects/personal/hyper-scale-commerce-gitops`

**Files:**
- Create: `argocd/topics-applicationset.yaml`, `topics/active.yaml` (`active: none`, with the header comment from Task 4), `topics/README.md`
- Modify: `README.md` (ownership table: add row `topic-<topic>` (one at a time) | `topics/<topic>/chart` | generated by system-design-practise Jenkins, switched by `topics/active.yaml`; plus a "System design topics" section linking `topics/README.md`)

**Interfaces:**
- Consumes: the GitOps layout written by Task 3/4 (`topics/<t>/chart`, `chart/values-topic.yaml`, `values-local.yaml`).
- Produces: ApplicationSet `system-design-topics` in `argocd`:
  - `goTemplate: true`, `goTemplateOptions: ["missingkey=error"]`.
  - Git files generator on `topics/active.yaml` (repo `git@github.com:nghiango/hyper-scale-commerce-gitops.git`, revision `main`) with selector `matchExpressions: [{key: active, operator: NotIn, values: [none]}]`.
  - Template: name `topic-{{.active}}`, label `system-design/topic`, finalizer `resources-finalizer.argocd.argoproj.io`, path `topics/{{.active}}/chart`, `helm.valueFiles: [values-topic.yaml, ../values-local.yaml]`, destination namespace `topic-{{.active}}`, `syncPolicy.automated: {prune: true, selfHeal: true}`.

- [ ] **Step 1: Write the files.** `topics/README.md` explains the switch (Jenkins `system-design/topic-switch` or editing `active.yaml`), the folder ownership (switch-owned vs Jenkins-owned), what survives going offline, and the warning that deleting the ApplicationSet takes the active topic offline.
- [ ] **Step 2: Validate locally**

Run: `kubectl apply --dry-run=client -f argocd/topics-applicationset.yaml -o name 2>&1 || true` and `python3 -c "print(open('topics/active.yaml').read())"`
Expected: the YAML parses. Client dry-run may fail without a cluster; the real validation is in Task 9, so record which one ran.

- [ ] **Step 3: Commit (do not push)** — `git commit -m "Add system design topics ApplicationSet with single active topic switch"`

---

### Task 7: `scripts/topic.sh` operator helper

**Files:**
- Create: `scripts/topic.sh`
- Test: `scripts/tests/test_topic_sh.py`

**Interfaces:**
- Produces: `scripts/topic.sh <command> [args]` with `KUBECTL` (default `kubectl --context kind-hyperscale-k8s`) overridable for tests:
  - `status` → `kubectl get applications -n argocd -l system-design/topic` and `kubectl get ns -l system-design/topic`.
  - `port-forward <topic> <service> <local-port>` → `kubectl -n topic-<topic> port-forward --address 127.0.0.1 svc/<service> <local-port>:8080`.
  - `logs <topic> [service]` → `kubectl -n topic-<topic> logs -l app.kubernetes.io/name=<service> --tail=200 -f`. Without a service, all pods.
  - `reset <topic>` → refuses if namespace `topic-<topic>` exists (message `topic <t> is online; deactivate it first`). Asks `Type <topic> to delete its database, Kafka topics and Redis keys:`. Then: find the primary pod with `kubectl -n hyperscale get pod -l app.kubernetes.io/component=postgres-ha,role=primary -o name`, then `kubectl -n hyperscale exec <pod> -- psql -U hyperscale -d hyperscale -c 'DROP DATABASE IF EXISTS "<db>"'`; `kubectl -n hyperscale exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --delete --topic '<topic>\..*'`; and Redis `--scan --pattern '<topic>:*'` piped to `DEL` via `redis-cli -a "$REDIS_PASSWORD"`, with the password read from Secret `redis-secret`.
  - Invalid topic slug → exit 2.

- [ ] **Step 1: Write failing tests** (fake `KUBECTL` script that appends its argv to a log file and exits per a scripted map)

```python
def test_port_forward_command(self): ...  # log contains "-n topic-url-shortener port-forward --address 127.0.0.1 svc/link-service 8081:8080"
def test_reset_refuses_active_topic(self):
    r = run_topic("reset", "url-shortener", ns_exists=True, stdin="url-shortener\n")
    self.assertNotEqual(r.returncode, 0); self.assertIn("is online; deactivate it first", r.stderr); self.assertNotIn("DROP DATABASE", log())
def test_reset_requires_typed_confirmation(self): ...  # stdin "yes\n" → nonzero, no DROP
def test_reset_drops_db_with_underscored_name(self): ...  # 'DROP DATABASE IF EXISTS "url_shortener"' in log
def test_rejects_bad_slug(self): ...  # "../x" → returncode 2
```

- [ ] **Step 2: Run and confirm failure** — `python3 -m unittest discover -s scripts/tests -v`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run and confirm pass.**
- [ ] **Step 5: Commit** — `git commit -m "Add topic.sh operator helper with guarded reset"`

---

### Task 8: Platform change — Prometheus/Alloy watch topic namespaces

**Repo:** `/Users/ngng/working/projects/personal/hyper-scale-commerce`. Follow that repo's `AGENTS.md`: scope is exactly this change, approved by the user in the design (spec 5.8).

**Files:**
- Modify: `local-ci/platform/files/prometheus.yaml`, `local-ci/platform/logging/config.alloy`, `local-ci/platform/templates/monitoring.yaml`, `local-ci/platform/templates/logging.yaml`, `local-ci/scripts/validate.sh`
- Test: `local-ci/scripts/test-platform-topics.py`
- Create: `local-ci/platform/files/topics-dashboard.json` (provisioned the same way as `files/dashboard.json`; check `files/dashboards.yaml` first)

**Interfaces:**
- Produces:
  - Prometheus job `topics`:
    - `kubernetes_sd_configs: [{role: endpoints}]` with no namespace restriction, and `metrics_path: /actuator/prometheus`.
    - Relabel `keep` with `__meta_kubernetes_namespace` =~ `topic-.+` and `keep` with `__meta_kubernetes_endpoint_port_name` = `http`.
    - Copy `__meta_kubernetes_pod_label_system_design_topic` → `topic`, `__meta_kubernetes_service_name` → `service`, `__meta_kubernetes_pod_name` → `pod`.
  - Alloy: a new `discovery.kubernetes "topic_pods"` (role pod, no namespace restriction) → `discovery.relabel "topic_pod_logs"` that keeps namespace `topic-.+`, sets `namespace`, `pod`, `container`, `topic`, and `cluster=hyperscale-local` → `loki.source.kubernetes "topic_pod_logs"` forwarding to the existing `loki.write.local`. The existing `pods` pipeline is unchanged.
  - ClusterRole + ClusterRoleBinding `local-monitoring-topics` (pods, services, endpoints: get/list/watch → SA `local-monitoring`) and `local-log-reader-topics` (pods get/list/watch, pods/log get → SA `local-log-reader`). The existing namespaced Roles are kept.
  - Dashboard "System Design Topics":
    - Variable `topic` = `label_values(up{job="topics"}, topic)`.
    - Panels: request rate `sum by (service)(rate(http_server_requests_seconds_count{topic="$topic"}[1m]))`, p95 `histogram_quantile(0.95, sum by (le, service)(rate(http_server_requests_seconds_bucket{topic="$topic"}[1m])))`, 5xx rate, JVM heap, `hikaricp_connections_active`, and Kafka consumer lag `kafka_consumer_fetch_manager_records_lag_max`.

- [ ] **Step 1: Write the failing check** — create `local-ci/scripts/test-platform-topics.py` and call it from the end of `local-ci/scripts/validate.sh`. It runs `helm template platform local-ci/platform` and asserts that the output contains `job_name: topics`, `regex: topic-.+`, `kind: ClusterRole` with name `local-monitoring-topics`, `local-log-reader-topics`, and `discovery.kubernetes "topic_pods"`. It also asserts that the existing `names: [hyperscale]` scrape is still present.
- [ ] **Step 2: Run and confirm failure** — `python3 local-ci/scripts/test-platform-topics.py` → assertion error on `job_name: topics`.
- [ ] **Step 3: Implement** per Interfaces.
- [ ] **Step 4: Run and confirm pass**, and also `helm lint local-ci/platform` and that repo's `bash local-ci/scripts/validate.sh`.
- [ ] **Step 5: Commit (do not push)** — `git commit -m "Let platform Prometheus and Alloy observe system-design topic namespaces"`

---

### Task 9: End-to-end verification on the cluster

**Prerequisites. Ask the user before each item that has side effects.**
1. kind cluster `kind-hyperscale-k8s`, Argo CD and local Jenkins are running (they were down when the spec was written).
2. Push `system-design-practise` to `git@github.com:nghiango/system-design-practise.git`. The repo must be readable by Jenkins over HTTPS (public), or the user provides a read credential.
3. Push the GitOps commit (Task 6) and the platform commit (Task 8). Run the commerce Jenkins job with `RELEASE_SCOPE=platform`.
4. `JENKINS_USER` / `JENKINS_TOKEN` exported by the user.

- [ ] **Step 1: Apply the ApplicationSet** — `kubectl --context kind-hyperscale-k8s apply -f argocd/topics-applicationset.yaml`. Then `kubectl -n argocd get applications -l system-design/topic` → `No resources found` (active is `none`).
- [ ] **Step 2: Register jobs** — `python3 ci/scripts/register_jenkins_jobs.py` → JSON lists `system-design/url-shortener` and `system-design/topic-switch`. If the Folder class is missing, stop and report it rather than flattening names.
- [ ] **Step 3: Run `system-design/url-shortener`** → SUCCESS. The GitOps repo gains commit `Deploy topic url-shortener <sha> (linux/arm64)`, which touches only `topics/url-shortener/`.
- [ ] **Step 4: Run `topic-switch` with `ACTION=status`** → `Active: none`, `url-shortener  published=yes  revision=<sha>`.
- [ ] **Step 5: Activate** — `ACTION=activate TOPIC=url-shortener`. Then verify:
  - `kubectl -n argocd get application topic-url-shortener` → `Synced Healthy`.
  - `kubectl get ns topic-url-shortener --show-labels` shows `system-design/topic=url-shortener`.
  - `kubectl -n hyperscale exec $(kubectl -n hyperscale get pod -l app.kubernetes.io/component=postgres-ha,role=primary -o name) -- psql -U hyperscale -d hyperscale -tAc "SELECT datname FROM pg_database WHERE datname='url_shortener'"` → `url_shortener`.
  - `kubectl -n hyperscale exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --list | grep '^url-shortener\.'` → `url-shortener.link.clicked`.
  - `kubectl -n hyperscale get networkpolicy -l system-design/topic=url-shortener` → 3 policies.
  - `kubectl -n argocd get applications hyperscale-local hyperscale-local-infrastructure hyperscale-local-platform` → all still `Healthy`.
- [ ] **Step 6: Exclusivity check**
  1. `ACTION=deactivate`, and wait until `kubectl get ns topic-url-shortener` → NotFound.
  2. `kubectl create ns topic-dummy && kubectl label ns topic-dummy system-design/topic=dummy`.
  3. `ACTION=activate TOPIC=url-shortener`. `kubectl -n topic-url-shortener logs job/wait-for-exclusive` shows `still online: "name":"topic-dummy"`, and no `db-init` pod exists yet.
  4. `kubectl delete ns topic-dummy`. Within about 30 s the wait job completes and the app goes `Healthy`.
- [ ] **Step 7: Data survives** — `ACTION=deactivate`. Namespace and `hyperscale`-side policies are gone, while DB `url_shortener` and Kafka topic `url-shortener.link.clicked` still exist.
- [ ] **Step 8: Observability smoke check** — in Grafana Explore, Prometheus `up{job="topics"}` returns empty without error (no services yet), and the "System Design Topics" dashboard loads.
- [ ] **Step 9: Record evidence and close the foundation**
  - Update `PROGRESS.md`: Foundation row done, a Log entry with the date and the command outputs above (abbreviated), and `Next step: brainstorm + plan URL Shortener services (Plan 2)`.
  - Commit `Record foundation end-to-end verification`.
