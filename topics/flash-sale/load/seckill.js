// k6 load test for the Flash Sale topic — the oversell evidence run.
// Port-forward first:
//   scripts/topic.sh port-forward flash-sale seckill-api 18080
// Then: k6 run -e SECKILL_API=http://127.0.0.1:18080 topics/flash-sale/load/seckill.js
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const SECKILL_API = __ENV.SECKILL_API || 'http://127.0.0.1:18080';

const STOCK = 100;
const BUYERS = 500;

const accepted = new Counter('sales_accepted');
const soldOut = new Counter('sales_sold_out');

export const options = {
  scenarios: {
    flash_sale: {
      executor: 'per-vu-iterations',
      vus: BUYERS,
      iterations: 1,
      maxDuration: '60s',
    },
  },
  thresholds: {
    // The headline assertion: exactly STOCK sales accepted, never more.
    sales_accepted: [`count==${STOCK}`],
  },
};

// Seed one item with stock=100; every VU then tries to buy exactly one unit.
export function setup() {
  const res = http.post(
    `${SECKILL_API}/api/items`,
    JSON.stringify({ name: 'k6-flash-item', price: 9.99, stock: STOCK }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(res, { 'seed 201': (r) => r.status === 201 });
  return { itemId: res.json('id') };
}

export default function (data) {
  const res = http.post(
    `${SECKILL_API}/api/sales`,
    JSON.stringify({ itemId: data.itemId, orderId: `k6-vu-${__VU}` }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (res.status === 202) {
    accepted.add(1);
  } else if (res.status === 409) {
    soldOut.add(1);
  }
  check(res, { 'accepted or sold out': (r) => r.status === 202 || r.status === 409 });
}

// Cross-check via the API: live stock must read exactly 0 after the rush.
export function teardown(data) {
  const res = http.get(`${SECKILL_API}/api/items`);
  const item = res.json().find((i) => i.id === data.itemId);
  console.log(`item ${data.itemId} final live stock=${item ? item.stock : 'n/a'} (expected 0)`);
  if (!item || item.stock !== 0) {
    throw new Error(`stock drift: expected 0, got ${item ? item.stock : 'item missing'}`);
  }
}
