// k6 load test for the Real-Time Auction topic — bid-ordering evidence.
// Port-forward first:
//   scripts/topic.sh port-forward realtime-auction auction-api 18080
// Then:
//   k6 run -e AUCTION_API=http://127.0.0.1:18080 topics/realtime-auction/load/auction.js
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const AUCTION_API = __ENV.AUCTION_API || 'http://127.0.0.1:18080';
const VUS = 500;

const accepted = new Counter('bids_accepted');
const rejected = new Counter('bids_rejected');
const gone = new Counter('bids_gone');

export const options = {
  scenarios: {
    bids: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: 1, // 500 concurrent bidders, one bid each
      maxDuration: '120s',
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'] },
};

const headers = { 'Content-Type': 'application/json' };

export function setup() {
  const res = http.post(
    `${AUCTION_API}/api/auctions`,
    JSON.stringify({ title: 'k6-auction', startPrice: 0, endsInSec: 600 }),
    { headers },
  );
  check(res, { 'auction 201': (r) => r.status === 201 });
  return { auctionId: res.json('id'), maxAmount: VUS };
}

export default function (data) {
  // Distinct, increasing amounts: VU n bids exactly n, so the global max (VUS)
  // is guaranteed to be accepted and MUST end as highest_bid.
  const res = http.post(
    `${AUCTION_API}/api/auctions/${data.auctionId}/bids`,
    JSON.stringify({ bidder: `k6-vu-${__VU}`, amount: __VU }),
    { headers },
  );
  if (res.status === 200) accepted.add(1);
  else if (res.status === 409) rejected.add(1);
  else if (res.status === 410) gone.add(1);
  check(res, { 'terminal status': (r) => [200, 409].includes(r.status) });
}

export function teardown(data) {
  const state = http.get(`${AUCTION_API}/api/auctions/${data.auctionId}`).json();
  const bids = http.get(`${AUCTION_API}/api/auctions/${data.auctionId}/bids`).json();
  check(state, {
    'highest bid is the max amount submitted': (s) => s.highestBid === data.maxAmount,
    'auction still open': (s) => s.status === 'OPEN',
  });
  check(bids, {
    'accepted count equals bidCount': (b) => b.length === state.bidCount,
    'seq contiguous 1..N': (b) => b.every((e, i) => e.seq === i + 1),
    'accepted amounts strictly increase with seq': (b) =>
      b.every((e, i) => i === 0 || e.amount > b[i - 1].amount),
  });
}
