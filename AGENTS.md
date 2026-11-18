# Agent notes

## Commands
- Build convention tests: `./gradlew -p build-logic test`
- All Java builds/tests: `./gradlew build`
- One topic: `./gradlew :<topic>:topicTest`, `:<topic>:topicIntegrationTest`, `:<topic>:topicAssemble`
- Topic chart tests: `python3 -m unittest discover -s deploy/topic-chart/tests -v`
- CI script tests: `python3 -m unittest discover -s ci/scripts/tests -v`
- Operator script tests: `python3 -m unittest discover -s scripts/tests -v`

## Rules
- Update `PROGRESS.md` in the same commit as the work it describes.
- Follow the naming table in `README.md`; every Kubernetes resource carries `system-design/topic: <topic>`.
- Python scripts use the standard library only (the Jenkins agent has no PyYAML) and must run on Python 3.9.
- Topics never call each other or read each other's database, Kafka topics or Redis keys.
- Technology choices in a topic are documented with the reason and rejected alternatives (`topics/<topic>/docs/decisions/`).
- Never put secrets in Git.
