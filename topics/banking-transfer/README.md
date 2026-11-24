# Banking Money Transfer

Status: services implemented, deploy + load evidence pending · Focus: ACID transactions,
idempotency, double-entry ledger, transactional outbox, reconciliation · Level: Advanced

## 1. Problem and requirements

Move money between accounts. The rules that make it a banking exercise rather than a CRUD one:

- **Never lose or create money** — under concurrency, retries, and crashes. The sum of balances
  is conserved; a balance can never go negative.
- **Every transfer is idempotent** — the client supplies an `idempotencyKey`; a retry after a
  timeout/crash returns the stored result and never double-posts.
- **Every transfer is auditable** — a double-entry ledger records both sides of every completed
  transfer with the resulting balance on each account.
- **Events are never lost** — the Kafka event is committed in the same transaction as the state
  change (transactional outbox); a worker relays it at-least-once and consumers dedupe.
- **Trust is verified, not assumed** — a reconciliation endpoint proves balance == ledger ==
  projection per account, plus a `reconciliation.drift` gauge for alerting.

Functional:

- `POST /api/accounts {owner, initial}` → 201 `{id}`; 400 bad input; 409 duplicate owner.
- `POST /api/transfers {idempotencyKey, from, to, amount}` → 201 `{id,status:"COMPLETED"}`;
  422 `REJECTED` on insufficient funds (durable, with `transfer.rejected` event); 200 with
  `replayed:true` and the stored result on a repeated key; 400 amount ≤ 0 or from == to; 404
  unknown account.
- `GET /api/accounts/{id}` → `{id, owner, balance}`; 404.
- `GET /api/accounts/{id}/statement` → source-ledger entries for the account (DEBIT/CREDIT with
  `balanceAfter`).
- `GET /api/transfers/{id}` and `GET /api/transfers/key/{key}` → the stored transfer; 404.
- `GET ledger-worker:8080/api/reconciliation` → per account
  `{accountId, balance, ledgerSum, projectionSum, drift, projectionDrift}`.
- `GET ledger-worker:8080/api/accounts/{id}/audit` → the Kafka-projected entries — the
  eventually-consistent view (it can lag the source ledger by a relay tick; lag is expected,
  drift is not).

## 2. Capacity estimation

| Dimension | Estimate | Basis |
|---|---|---|
| Transfers | ~500 rps sustained | one txn = 1 insert + 2 row locks + 2 updates + 2 ledger + 1 outbox inserts; ~8 statements on 2 hot rows |
| Hot-account contention | serialises on the row pair | a corporate settlement account hit by 500 rps queues on its FOR UPDATE lock — that's the real ceiling, not CPU |
| Reads | 10–50× writes | statements/balances are plain SELECTs; scale with read replicas if needed |
| Outbox relay | 100 rows/tick/replica | `LIMIT 100 FOR UPDATE SKIP LOCKED` — scales horizontally, O(unpublished) via partial index |
| ledger_audit | 2× completed transfers | append-only; partition by month when it hurts |

The bottleneck is **correctness on contended rows**, identical in spirit to ticket-booking's hot
seat: N threads moving money through the same two accounts must serialise — the exercise is that
they serialise *safely* (no lost update, no deadlock, no double-post).

## 3. API

```
POST transfer-api:8080/api/accounts        {"owner":"alice","initial":100.00}
  → 201 {"id":1} | 400 | 409 owner taken
POST transfer-api:8080/api/transfers       {"idempotencyKey":"k1","from":1,"to":2,"amount":30.00}
  → 201 {"id":1,"status":"COMPLETED","from":1,"to":2,"amount":30.00,"replayed":false}
  → 200 same body with "replayed":true          (repeated idempotencyKey, stored result)
  → 422 {"id":1,"status":"REJECTED",...}        (insufficient funds — durable + outbox event)
  → 400 amount ≤ 0 | from == to | missing key   → 404 unknown account
GET  transfer-api:8080/api/accounts/1            → 200 {"id":1,"owner":"alice","balance":70.00}
GET  transfer-api:8080/api/accounts/1/statement  → 200 [{transferId,entry,amount,balanceAfter,at}]
GET  transfer-api:8080/api/transfers/1           → 200 stored transfer | 404
GET  transfer-api:8080/api/transfers/key/k1      → 200 stored transfer | 404
GET  ledger-worker:8080/api/reconciliation       → 200 [{accountId,balance,ledgerSum,projectionSum,drift,projectionDrift}]
GET  ledger-worker:8080/api/accounts/1/audit     → 200 projected entries (eventually consistent)
Kafka banking-transfer.ledger.entries (6 partitions, key = transferId)
  {"type":"transfer.completed","transferId":1,"from":1,"to":2,"amount":30.00,
   "entries":[{accountId,entry,amount,balanceAfter}, ...]}
  {"type":"transfer.rejected","transferId":2,"reason":"insufficient-funds",...}
```

## 4. Architecture

Two services, **one Postgres database** (`banking_transfer`) — that is the whole transaction
boundary (decision 001):

```
client ──POST /api/transfers──▶ transfer-api ──ONE txn──▶ Postgres
                                   │                     accounts / transfers / ledger / outbox
                                   └── 1. INSERT transfers ON CONFLICT DO NOTHING  (idempotency gate)
                                       2. SELECT ... FOR UPDATE ×2 in ASC id order (deadlock-safe)
                                       3. funds check → REJECTED + outbox, or
                                       4. UPDATE balances + ledger pair + COMPLETED + outbox
                                            │ commits atomically
Postgres outbox ──@Scheduled 1s──▶ ledger-worker (relay)
   SELECT ... FOR UPDATE SKIP LOCKED → kafka.send(payload, key=transferId) → published=true
                                            │
Kafka banking-transfer.ledger.entries ──▶ ledger-worker (consumer, group <prefix>projection)
   payload.entries → INSERT INTO ledger_audit ON CONFLICT DO NOTHING   (dedupe)
                                            │
                            /api/reconciliation → balance vs Σledger vs Σledger_audit
```

Why these primitives:

- **Double-entry ledger** — money never appears/disappears silently: every transfer writes a
  DEBIT and a CREDIT row with the post-entry balance. Sum of signed entries per account must
  equal `accounts.balance`; that equality is what reconciliation verifies, and it is what makes
  "did we lose money?" an *answerable* question.
- **`SELECT ... FOR UPDATE`** — the row lock serialises concurrent transfers on an account;
  the funds check then reads a balance nobody else can change mid-transaction (no lost update).
- **Ascending-id lock order** — a transfer A→B locks `min(A,B)` then `max(A,B)`. Two
  opposite-direction transfers then always wait on the same first lock instead of each holding
  the lock the other wants: the classic A-holds-1-wants-2 / B-holds-2-wants-1 cycle cannot form.
- **Transactional outbox** — `ledger` + `outbox` commit atomically: a published transfer always
  has its event. The relay's at-least-once republish is absorbed by the consumer's
  `ON CONFLICT DO NOTHING` dedupe on `(transfer_id, account_id, entry)`.
- **Reconciliation** — teaches verification vs trust: the projection is a *second* copy of the
  money trail built only from Kafka events; when `balance − Σledger` or
  `Σledger_audit − Σledger` is non-zero something between the API and the consumer lied.

### Alternatives considered (and rejected)

- **2PC / saga across services** — the transfer boundary is one database; splitting it would
  buy nothing and pay for it in partial-commit recovery logic. Rejected.
- **Best-effort publish after commit** — a crash between commit and `kafka.send` loses the
  event forever with no way to detect it. Rejected; the outbox makes the event part of the commit.
- **Event sourcing** (ledger *is* the state, balances derived) — viable, but turns every balance
  read into a fold and every funds check into a consistency question; the mutable `accounts`
  row + ledger pair gives the same auditability with simpler reads. Rejected for the exercise.
- **SELECT-check-then-UPDATE** — reads a stale balance under READ COMMITTED and loses the
  update under concurrency; needs the FOR UPDATE lock anyway to be correct. Rejected.
- **`double`/`float` for money** — binary floats cannot represent 0.10; `numeric(19,2)` /
  `BigDecimal` throughout, plus `CHECK (balance >= 0)` as the last line of defence.

## 5. Data model

Flyway `V1` shipped in **both** services (same migration set; `flyway_schema_history` locking
serialises parallel startup, same pattern as the other topics):

- `accounts(id, owner UNIQUE, balance numeric(19,2) CHECK ≥ 0, created_at)`
- `transfers(id, idempotency_key UNIQUE, from_account, to_account, amount CHECK > 0,
  status INITIATED|COMPLETED|REJECTED, created_at)` — `INITIATED` is never visible post-commit;
  every stored row is terminal.
- `ledger(id, transfer_id → transfers NULL, account_id → accounts, entry DEBIT|CREDIT, amount,
  balance_after, created_at)` — exactly two rows per COMPLETED transfer, **plus one opening
  CREDIT row (`transfer_id NULL`) at account creation**: the seed money is a ledger entry too,
  which is what makes `Σ signed entries == balance` hold and reconciliation start at zero.
  Account creation also emits an `account.opened` outbox event (transfer id `0` in the
  projection) so `ledger_audit` sees the same stream.
- `outbox(id, type, payload, published, created_at)` + partial index on `id WHERE NOT published`.
- `ledger_audit(id, transfer_id, account_id, entry, amount, balance_after, at,
  UNIQUE(transfer_id, account_id, entry))` — the projection; the unique key IS the dedupe.

## 6. Failure and recovery

| Failure | Behaviour |
|---|---|
| Crash mid-transfer | whole txn rolls back; the idempotency-key insert never committed → client retry is a fresh attempt, not a replay of a half-write |
| Retry of committed transfer | `ON CONFLICT` fires → stored row returned as `replayed:true`, zero new writes |
| Crash after commit, before relay | outbox row is `published=false` → next relay tick publishes it; at-least-once |
| Crash after `kafka.send`, before mark-published | row republished → consumer `ON CONFLICT` dedupes it |
| Two opposite-direction transfers | same lock order → one waits, no deadlock |
| Relay replica ×N | `FOR UPDATE SKIP LOCKED` → disjoint batches, no double-claim |
| Malformed Kafka event | dropped, partition not wedged |

## 7. Testing

- `transfer-api` unit (`TransferServiceTest`): replay never locks/writes; insufficient funds →
  REJECTED + outbox, no ledger; ascending lock order; completed transfer writes balances, ledger
  pair, status, outbox; missing account aborts.
- `transfer-api` integration (`TransferApiIntegrationTest`, Postgres+Kafka Testcontainers):
  double-entry written; **same key ×5 concurrent → 1 write, identical stored response**; **50
  concurrent ring transfers → total conserved, no negative balance, every DEBIT has its CREDIT**;
  rejected transfer replays REJECTED; validation matrix; statement view.
- `ledger-worker` unit (`LedgerConsumerTest`, `OutboxRelayTest`): both entries projected via the
  dedupe insert; redelivery is the same insert; rejected/malformed events dropped; relay marks
  published only after a successful send; send failure leaves the row unpublished.
- `ledger-worker` integration (`LedgerWorkerIntegrationTest`): unpublished outbox row → relayed
  → `ledger_audit` populated → duplicate delivery deduped → **reconciliation drift 0** on both
  axes, audit endpoint serves the projected view.
- `load/transfer.js` (k6): seeds an account pool, 500 VUs transfer random pairs with unique
  keys, teardown asserts `GET /api/reconciliation` shows zero drift and the pool total is
  conserved.

## 8. Metrics

`transfers.completed` / `transfers.rejected` / `transfers.replayed` (transfer-api);
`outbox.published` (relay); `reconciliation.drift` gauge = Σ|balance − ledgerSum| across
accounts — non-zero is the page-the-on-call condition this topic exists to teach.

Decisions: [001 — single-DB transaction + double-entry + outbox](docs/decisions/001-double-entry-outbox.md),
[002 — idempotency and at-least-once recovery](docs/decisions/002-idempotency-and-recovery.md).
