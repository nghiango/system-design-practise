# <Topic title>

Status: <not started | in progress | done> · Focus: <focus> · Level: <level>

## 1. Problem and requirements
Functional requirements, plus non-functional requirements with numbers (latency, throughput, consistency, availability).

## 2. Capacity estimation
QPS, storage, bandwidth and cache size for the "real" scale, and how the local kind scale-down relates to it.

## 3. API
Endpoints or messages, with request/response examples.

## 4. Data model
Tables, keys, Kafka topics (`<topic>.<entity>.<event>`) and Redis keys (`<topic>:...`), with the reason each exists.

## 5. Architecture
A Mermaid diagram of the services and shared infrastructure, plus a step-by-step flow of the main request.

## 6. The hard problems and how they are solved
Each core problem (for example overselling): why the naive version fails, and what this design does instead.

## 7. Technology choices
| Need | Chosen | Why | Rejected alternatives | Decision record |
|---|---|---|---|---|
Each row links to `docs/decisions/NNNN-<slug>.md` (Context / Options / Decision / Consequences).

## 8. Failure scenarios and recovery
What happens when Redis, Kafka, Postgres or a pod fails, and how the system recovers.

## 9. Run and verify
How to deploy (Jenkins `system-design/<topic>` then `topic-switch` → activate), how to call it, the k6 scenarios, and recorded results (numbers, date, revision).

## 10. Experiments to try next
Changes worth trying, and what you expect to observe.
