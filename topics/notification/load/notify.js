// k6 load test for the Notification topic — the end-to-end delivery evidence run.
// Port-forward first:
//   scripts/topic.sh port-forward notification notify-api 18080
//   scripts/topic.sh port-forward notification sender-worker 18081
// Then: k6 run topics/notification/load/notify.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const NOTIFY_API = __ENV.NOTIFY_API || 'http://127.0.0.1:18080';
const SENDER_API = __ENV.SENDER_API || 'http://127.0.0.1:18081';

const COUNT = 100;
const VUS = 20;
// The token bucket paces sends at 10/s/channel, so 100 messages need ~10s to drain.
const POLL_DEADLINE_S = 90;

const sent = new Counter('notifications_sent');
const notTerminal = new Counter('notifications_not_terminal');

export const options = {
  scenarios: {
    submit_and_verify: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: COUNT / VUS,
      maxDuration: '150s',
    },
  },
  thresholds: {
    // The headline assertion: every submitted notification reaches SENT.
    notifications_sent: [`count==${COUNT}`],
    notifications_not_terminal: ['count==0'],
  },
};

// Each iteration submits one notification, then polls the worker's read API
// until the row reaches a terminal status (SENT or DEAD).
export default function () {
  const res = http.post(
    `${NOTIFY_API}/api/notifications`,
    JSON.stringify({
      idempotencyKey: `k6-${__VU}-${__ITER}`,
      channel: 'email',
      target: `vu-${__VU}@example.com`,
      body: 'load test',
    }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (!check(res, { 'queued': (r) => r.status === 201 })) {
    notTerminal.add(1);
    return;
  }
  const id = res.json('id');

  const deadline = Date.now() + POLL_DEADLINE_S * 1000;
  while (Date.now() < deadline) {
    const status = http.get(`${SENDER_API}/api/notifications/${id}`);
    if (status.status === 200) {
      const s = status.json('status');
      if (s === 'SENT') {
        sent.add(1);
        return;
      }
      if (s === 'DEAD') {
        notTerminal.add(1);
        return;
      }
    }
    sleep(0.5);
  }
  notTerminal.add(1);
}
