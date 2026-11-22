// k6 load test for the URL Shortener topic.
// Port-forward the services first:
//   scripts/topic.sh port-forward url-shortener link-api 18080   (terminal 1)
//   scripts/topic.sh port-forward url-shortener redirect 18081   (terminal 2)
// Then: k6 run -e LINK_API=http://127.0.0.1:18080 -e REDIRECT=http://127.0.0.1:18081 topics/url-shortener/load/links.js
import http from 'k6/http';
import { check } from 'k6';

const LINK_API = __ENV.LINK_API || 'http://127.0.0.1:18080';
const REDIRECT = __ENV.REDIRECT || 'http://127.0.0.1:18081';

// ~100:1 read:write ratio at local scale.
export const options = {
  scenarios: {
    create_links: {
      executor: 'constant-vus',
      vus: 1,
      duration: '30s',
      exec: 'createLink',
    },
    redirects: {
      executor: 'constant-vus',
      vus: 20,
      duration: '30s',
      exec: 'redirect',
    },
  },
};

// Seed links so the redirect scenario has real codes to hit.
export function setup() {
  const codes = [];
  for (let i = 0; i < 50; i++) {
    const res = http.post(
      `${LINK_API}/api/links`,
      JSON.stringify({ url: `https://example.com/seed/${i}` }),
      { headers: { 'Content-Type': 'application/json' } },
    );
    if (res.status === 201) {
      codes.push(res.json('code'));
    }
  }
  return { codes };
}

export function createLink() {
  const res = http.post(
    `${LINK_API}/api/links`,
    JSON.stringify({ url: `https://example.com/page/${__VU}-${__ITER}` }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(res, { 'create 201': (r) => r.status === 201 });
}

export function redirect(data) {
  const codes = data.codes.length ? data.codes : ['a'];
  const code = codes[Math.floor(Math.random() * codes.length)];
  const res = http.get(`${REDIRECT}/${code}`, { redirects: 0 });
  check(res, { 'redirect 302': (r) => r.status === 302 });
}
