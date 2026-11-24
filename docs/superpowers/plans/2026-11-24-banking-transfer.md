# Banking Transfer plan (2026-11-24)

- transfer-api: Flyway V1 (both services): accounts(id bigserial pk, owner text unique, balance numeric(19,2) check >=0, created_at); transfers(id bigserial pk, idempotency_key text unique, from_account, to_account, amount, status INITIATED|COMPLETED|REJECTED, created_at); ledger(id bigserial pk, transfer_id, account_id, entry DEBIT|CREDIT, amount, balance_after, created_at); outbox(id bigserial pk, type, payload text, published boolean default false, created_at).
  Endpoints:
  - POST /api/accounts {owner, initial} -> 201.
  - POST /api/transfers {idempotencyKey, from, to, amount>0} -> ONE txn:
      1. INSERT transfers ON CONFLICT (idempotency_key) DO NOTHING RETURNING; conflict -> read stored row, return same result (200 replay, never double-post).
      2. Lock both account rows in id order (SELECT ... FOR UPDATE — prevents deadlock).
      3. If from.balance < amount -> transfer status REJECTED + outbox; return 422.
      4. UPDATE both balances; INSERT 2 ledger rows (DEBIT/-, CREDIT/+) with balance_after; status COMPLETED; INSERT outbox row.
    Return 201 {id,status}.
  - GET /api/accounts/{id} -> balance; GET /api/transfers/{id or key}; GET /api/accounts/{id}/statement -> ledger rows for that account.
  - Rejected: distributed tx/2PC (single DB suffices for the boundary), best-effort publish (lost events), no double-entry (no audit trail).
- ledger-worker:
  - @Scheduled relay (1s): SELECT outbox WHERE NOT published ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 100 -> publish to banking-transfer.ledger.entries -> mark published. Crash-safe, at-least-once + consumer dedupe by outbox id.
  - @KafkaListener group ${SD_KAFKA_GROUP_PREFIX}projection -> statements projection table (account_id, entry count, last entry) OR just an audit log table; dedupe by event id.
  - Reconciliation endpoint GET /api/reconciliation: for each account SUM ledger vs balance -> {account, balance, ledgerSum, drift} + counter metric reconciliation.drift. Teaches recovery/verification.
- Race/evidence: 2 threads transfer overlapping same account -> balance never negative; same key retried -> identical response, 1 ledger pair; kill worker mid-relay -> entries eventually all published.
- load/transfer.js k6: concurrent transfers on a pool of accounts, then /api/reconciliation must show zero drift.
- Docs README + decisions/001-double-entry-outbox.md, 002-idempotency-recovery.md.
