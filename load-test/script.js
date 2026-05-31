import http from 'k6/http';
import { check } from 'k6';

export let options = {
  vus: 10000,
  duration: '10s',
};

export default function () {
  const userId = `user_${Math.random().toString(36).substr(2, 9)}`;
  const res = http.post(
    'http://localhost:8080/buy',
    JSON.stringify({
      user_id: userId,
      product_id: 'iphone15',
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, {
    'status is 200 or 409': (r) => r.status === 200 || r.status === 409,
  });
}
