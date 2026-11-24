// k6 load test for the Banking Money Transfer topic — concurrency + reconciliation evidence.
// Port-forward first (both services):
//   scripts/topic.sh port-forward banking-transfer transfer-api 18080
//   scripts/topic.sh port-forward banking-transfer ledger-worker 18081
// Then:
//   k6 run -e TRANSFER_API=http://127.0.0.1:18080 \
//          -e LEDGER_WORKER=http://127.0.0.1:18081 \
//          topics/banking-transfer/load/transfer.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const TRANSFER_API = __ENV.TRANSFER_API || 'http://127.0.0.1:18080';
const LEDGER_WORKER = __ENV.LEDGER_WORKER || 'http://127.0.0.1:18081';

const POOL = 20; // accounts, each seeded with SEED units
const SEED = 1000;
const VUS = 500;
const completed = new Counter('transfers_completed');
const rejected = new Counter('transfers_rejected');
const replayed = new Counter('transfers_replayed');

export const options = {
  scenarios: {
    transfers: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: 10, // 5000 transfers over the 20-account pool
      maxDuration: '300s',
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'] },
};

const headers = { 'Content-Type': 'application/json' };

export function setup() {
  const ids = [];
  for (let i = 0; i < POOL; i++) {
    const res = http.post(
      `${TRANSFER_API}/api/accounts`,
      JSON.stringify({ owner: `k6-pool-${i}`, initial: SEED }),
      { headers },
    );
    check(res, { 'seed 201': (r) => r.status === 201 });
    ids.push(res.json('id'));
  }
  return { ids, total: POOL * SEED };
}

export default function (data) {
  // Random ordered pair; every request carries a globally unique idempotency key.
  const from = data.ids[Math.floor(Math.random() * data.ids.length)];
  let to = data.ids[Math.floor(Math.random() * data.ids.length)];
  if (to === from) {
    to = data.ids[(data.ids.indexOf(to) + 1) % data.ids.length];
  }
  const key = `k6-${__VU}-${__ITER}-${Date.now()}`;
  const res = http.post(
    `${TRANSFER_API}/api/transfers`,
    JSON.stringify({ idempotencyKey: key, from, to, amount: 1 }),
    { headers },
  );
  if (res.status === 201) completed.add(1);
  else if (res.status === 422) rejected.add(1);
  else if (res.status === 200 && res.json('replayed')) replayed.add(1);
  check(res, { 'terminal status': (r) => [200, 201, 422].includes(r.status) });
  sleep(0.05);
}

export function teardown(data) {
  // Give the outbox relay + consumer time to drain before reconciling.
  let report = null;
  for (let i = 0; i < 60; i++) {
    report = http.get(`${LEDGER_WORKER}/api/reconciliation`).json();
    const caughtUp = report.every(
      (r) => r.drift === 0 && r.projectionDrift === 0,
    );
    if (caughtUp) break;
    sleep(2);
  }
  check(report, {
    'reconciliation: zero drift on every account': (rows) =>
      rows && rows.every((r) => r.drift === 0 && r.projectionDrift === 0),
    'reconciliation: money conserved': (rows) =>
      rows &&
      Math.abs(
        rows
          .filter((r) => data.ids.includes(r.accountId))
          .reduce((s, r) => s + r.balance, 0) - data.total,
      ) < 0.001,
  });
}
