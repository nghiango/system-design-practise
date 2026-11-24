// k6 load test for the Ticket Booking topic — hold/confirm/sweep evidence run.
// Port-forward first:
//   scripts/topic.sh port-forward ticket-booking booking-api 18080
// Then: k6 run -e BOOKING_API=http://127.0.0.1:18080 topics/ticket-booking/load/book.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BOOKING_API = __ENV.BOOKING_API || 'http://127.0.0.1:18080';

const ROWS = 10;
const COLS = 50; // 500 seats — every VU holds a distinct seat
const BUYERS = 500;
const CONFIRMED = 300; // first 60% of VUs confirm their hold

const held = new Counter('seats_held');
const booked = new Counter('seats_booked');
const conflicts = new Counter('seat_conflicts');

export const options = {
  scenarios: {
    booking: {
      executor: 'per-vu-iterations',
      vus: BUYERS,
      iterations: 1,
      maxDuration: '120s',
    },
  },
  thresholds: {
    // Distinct seats → every hold should win; the headline booked==300 check runs in teardown.
    seats_held: [`count==${BUYERS}`],
  },
};

function seatFor(vu) {
  const idx = vu - 1; // 0..499 → A1..J50, one distinct seat per buyer
  const row = String.fromCharCode(65 + Math.floor(idx / COLS));
  return `${row}${(idx % COLS) + 1}`;
}

export function setup() {
  const res = http.post(
    `${BOOKING_API}/api/events`,
    JSON.stringify({ name: 'k6-booking', price: 39.9, rows: ROWS, cols: COLS }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(res, { 'seed 201': (r) => r.status === 201 });
  return { eventId: res.json('id'), seatCount: res.json('seatCount') };
}

export default function (data) {
  const seat = seatFor(__VU);
  const owner = `k6-vu-${__VU}`;
  const hold = http.post(
    `${BOOKING_API}/api/events/${data.eventId}/hold`,
    JSON.stringify({ seat, owner }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (hold.status === 200) {
    held.add(1);
    if (__VU <= CONFIRMED) {
      const confirm = http.post(
        `${BOOKING_API}/api/events/${data.eventId}/confirm`,
        JSON.stringify({ seat, owner }),
        { headers: { 'Content-Type': 'application/json' } },
      );
      if (confirm.status === 200) {
        booked.add(1);
      }
      check(confirm, { 'confirm ok': (r) => r.status === 200 });
    }
    // The other 40% simply walk away: their holds must expire and be swept.
  } else if (hold.status === 409) {
    conflicts.add(1);
  }
  check(hold, { 'held or conflict': (r) => r.status === 200 || r.status === 409 });
}

// Cross-check via the API: after the sweeper interval, exactly the confirmed
// 300 seats are BOOKED and the 200 abandoned holds are FREE again.
export function teardown(data) {
  sleep(12); // one sweeper interval (10s) plus slack
  const res = http.get(`${BOOKING_API}/api/events/${data.eventId}/seats`);
  const seats = res.json();
  const byStatus = seats.reduce((m, s) => ((m[s.status] = (m[s.status] || 0) + 1), m), {});
  check(byStatus, {
    'exactly 300 seats BOOKED': (s) => (s.BOOKED || 0) === CONFIRMED,
    'abandoned holds swept FREE': (s) =>
      (s.BOOKED || 0) + (s.FREE || 0) === data.seatCount && (s.HELD || 0) === 0,
  });
}
